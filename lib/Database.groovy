import groovy.sql.Sql

class Database {
    private String uri
    private Sql sql
    private static final def INSERT_SIZE = 10000

    Database(String uri, String user, String password, boolean sql = false) {
        this.uri = uri
        this.connect(user, password)
    }

    private void connect(String user, String password) {
        String url = "jdbc:oracle:thin:${this.uri}"
        String driver = "oracle.jdbc.driver.OracleDriver"
        try {
            this.sql = Sql.newInstance(url, user, password, driver)
        } catch (Exception e) {
            e.printStackTrace()
            throw e
        }
    }

    void close() {
        if (this.sql) {
            this.sql.close()
        }
    }

    String getMaxUPI() { // UPI of the most recent seq in UniParc
        def query = "SELECT MAX(UPI) FROM UNIPARC.PROTEIN"
        return this.sql.rows(query)[0][0]
    }

    void wipeProteinTable() {
        this.sql.execute("DROP TABLE UNIPARC.PROTEIN PURGE")
        this.sql.execute(
            """
            CREATE TABLE UNIPARC.PROTEIN
            (
                ID NUMBER(15) NOT NULL,
                UPI CHAR(13) NOT NULL,
                TIMESTAMP DATE NOT NULL,
                USERSTAMP VARCHAR2(30) NOT NULL,
                CRC64 CHAR(16) NOT NULL,
                LEN NUMBER(6) NOT NULL,
                SEQ_SHORT VARCHAR2(4000),
                SEQ_LONG CLOB,
                MD5 VARCHAR2(32) NOT NULL
            ) NOLOGGING
            """
        )
    }

    void configureProteinTable() {
        this.sql.execute("GRANT SELECT ON UNIPARC.PROTEIN TO PUBLIC")
        this.sql.execute("CREATE UNIQUE INDEX ON UNIPARC.PROTEIN (UPI)")
    }

    List<String> iterProteins(String gt = null, String le = null, Closure rowHandler) {
        String sqlQuery = """
            SELECT ID, UPI, TIMESTAMP, USERSTAMP, CRC64, LEN, SEQ_SHORT, SEQ_LONG, MD5
            FROM UNIPARC.PROTEIN
        """
        def filters = []
        def params = [:]
        if (gt) {
            filters << "UPI > :gt"
            params.gt = gt
        }
        if (le) {
            filters << "UPI <= :le"
            params.le = le
        }
        if (filters) {
            sqlQuery += " WHERE " + filters.join(" AND ")
        }
        this.sql.eachRow(sqlQuery, params, rowHandler)
    }

    void insertProteins(List<String> records) {
        String insertQuery = """
            INSERT /* APPEND */ INTO UNIPARC.PROTEIN
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        this.sql.withBatch(insertQuery) { stmt ->
            records.each { stmt.addBatch([
                it.ID.toInteger(),
                it.UPI,
                it.TIMESTAMP,
                it.USERSTAMP,
                it.CRC64,
                it.LEN.toInteger(),
                it.SEQ_SHORT,
                it.SEQ_LONG,
                it.MD5
            ]) }
        }
        this.sql.commit()
    }

    List<String> getAnalyses() {
        def query = """
            SELECT A.MAX_UPI, A.I6_DIR, A.INTERPRO_VERSION, A.NAME,
                T.MATCH_TABLE, T.SITE_TABLE,
                A.ID, A.VERSION
            FROM ANALYSIS_I6 A
            INNER JOIN ANALYSIS_TABLES T
                ON LOWER(A.NAME) = LOWER(T.NAME)
            WHERE A.ACTIVE = 'Y'
        """
        return this.sql.rows(query)
    }

    List<Map> getPartitions(schema, table) {
        String query ="""
            SELECT P.PARTITION_NAME, P.PARTITION_POSITION, P.HIGH_VALUE,
            K.COLUMN_NAME, K.COLUMN_POSITION
            FROM ALL_TAB_PARTITIONS P
            INNER JOIN ALL_PART_KEY_COLUMNS K
            ON P.TABLE_OWNER = K.OWNER 
            AND P.TABLE_NAME = K.NAME
            WHERE P.TABLE_OWNER = ? 
            AND P.TABLE_NAME = ?
        """
        Map<String, Map> partitions = [:]

        this.sql.eachRow(query, [schema.toUpperCase(), table.toUpperCase()]) { row ->
            String partName = row.PARTITION_NAME
            if (partitions.containsKey(partName)) {
                throw new Exception("Multi-column partitioning keys are not supported")
            }
            partitions[partName] = [
                    name    : partName,
                    position: row.PARTITION_POSITION,
                    value   : row.HIGH_VALUE,
                    column  : row.COLUMN_NAME
            ]
        }

        return partitions.values().sort { it.position }
    }
}
