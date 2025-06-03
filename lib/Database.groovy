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
}
