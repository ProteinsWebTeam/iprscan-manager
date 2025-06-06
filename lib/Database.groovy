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

    void query(String query, List<String> params) {
        this.sql.execute(query, params)
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

    void writeFasta(String upi_from, String upi_to, String fasta) {
        // Build a fasta file of protein seqs. Batch for speed.
        def writer = new File(fasta.toString()).newWriter()
        Integer offset = 0
        Integer batchSize = 1000
        String query = """
        SELECT UPI, SEQ_SHORT, SEQ_LONG
        FROM (
            SELECT UPI, SEQ_SHORT, SEQ_LONG, ROW_NUMBER() OVER (ORDER BY UPI) AS row_num
            FROM UNIPARC.PROTEIN
            WHERE UPI BETWEEN ? AND ?
        ) WHERE row_num BETWEEN ? AND ?
        """

        def batch = this.sql.rows(query, [upi_from, upi_to, offset + 1, offset + batchSize])
        for (row: batch) {
            def upi = row[0]
            def seq = row[1] ?: row[2]
            seq = seq.toString()
            writer.writeLine(">${upi}")
            for (int i = 0; i < seq.length(); i += 60) {
                int end = Math.min(i + 60, seq.length())
                writer.writeLine(seq.substring(i, end))
            }
        }

        writer.close()
    }

    Integer getJobCount(Integer analysis_id, String max_upi) {
        return this.sql.execute(
            "SELECT COUNT (*) FROM IPRSCAN.ANALYSIS_JOBS WHERE ANALYSIS_ID = ? AND UPI_FROM > ?",
            [analysis_id, max_upi]
        )[0]
    }
}
