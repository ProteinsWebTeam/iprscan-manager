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
}
