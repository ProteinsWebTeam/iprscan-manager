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
    }
}
