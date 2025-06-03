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
            System.out.println("FILTERs: ${filters}")
        }
        System.out.println("params: ${params}")
        System.out.println("sqlQuery = ${sqlQuery}")
        this.sql.eachRow(sqlQuery, params, rowHandler)
    }

//    void insertProteins(List<String> records) {
//        String insertQuery = """
//
//    """
//    }
}
