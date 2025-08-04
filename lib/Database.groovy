import oracle.sql.CLOB
import groovy.sql.Sql


class Database {
    private Sql sql
    private static final def INSERT_SIZE = 10000
    private static final def DRIVERS = [
        oracle: [
            driver: "oracle.jdbc.driver.OracleDriver",
            prefix: "jdbc:oracle:thin"
        ],
        postgresql: [
            driver: "org.postgresql.Driver",
            prefix: "jdbc:postgresql"
        ]
    ]

    Database(String uri, String user, String password, String engine) {
        this.connect(uri, user, password, engine.replace("-", ""))
    }

    private void connect(String uri, String user, String password, String engine) {
        String url = "${this.DRIVER[engine].prefix}:${uri}"
        String driver = this.DRIVER[engine].driver
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

    void dropProteinTable() {
        this.sql.execute("DROP TABLE UNIPARC.PROTEIN PURGE")
    }

    void buildProteinTable() {
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
        System.out.println("GT: ${gt} LE: ${le}")
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
        System.out.println("filters: ${filters}")
        System.out.println("params: ${params}")
        if (filters) {
            sqlQuery += " WHERE " + filters.join(" AND ")
        }
        System.out.println("Query: ${sqlQuery}\nParams: ${params}")
        this.sql.eachRow(sqlQuery, params, rowHandler)
    }

    void insertProteins(List<String> records) {
        String insertQuery = """
            INSERT /*+ APPEND */ INTO UNIPARC.PROTEIN
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        System.out.println("INSERT - ${records[0]} to ${records[-1]}")
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
    }

    List<String> getAnalyses() {
        def query = """
            SELECT A.max_upi, A.i6_dir, A.INTERPRO_VERSION, A.name,
                T.match_table, T.site_table,
                A.id, A.version
            FROM iprscan.analysis A
            INNER JOIN iprscan.analysis_tables T
                ON LOWER(A.name) = LOWER(T.name)
            WHERE A.active AND
                A.i6_dir IS NOT NULL
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

    Integer getJobCount(Integer analysis_id, String max_upi) {
        return this.sql.execute(
            "SELECT COUNT (*) FROM IPRSCAN.ANALYSIS_JOBS WHERE ANALYSIS_ID = ? AND UPI_FROM > ?",
            [analysis_id, max_upi]
        )[0]
    }

    Integer writeFasta(String upi_from, String upi_to, String fasta) {
        // Build a fasta file of protein seqs. Batch for speed.
        def writer = new File(fasta.toString()).newWriter()
        Integer seqCount = 0
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

        while (true) {
            def batch = this.sql.rows(query, [upi_from, upi_to, offset + 1, offset + batchSize])
            for (row: batch) {
                def upi = row.UPI
                def seq = row.SEQ_SHORT ?: row.SEQ_LONG

                // Convert Oracle CLOB to String if necessary - needed for very long seqs
                if (seq instanceof CLOB) {
                    seq = seq.getSubString(1, (int) seq.length())
                } else {
                    seq = seq.toString()
                }

                if (seq) {
                    writer.writeLine(">${upi}")
                    for (int i = 0; i < seq.length(); i += 60) {
                        int end = Math.min(i + 60, seq.length())
                        writer.writeLine(seq.substring(i, end))
                    }
                    seqCount += 1
                }
            }
            if (batch.isEmpty()) {
                break
            }

            offset += batchSize
        }

        writer.close()

        return seqCount
    }

    List<String> getIndexStatuses(String owner, String table) {
        def query = """SELECT I.INDEX_NAME, I.STATUS
            FROM ALL_INDEXES I
            INNER JOIN ALL_IND_COLUMNS IC
              ON I.OWNER = IC.INDEX_OWNER
              AND I.INDEX_NAME = IC.INDEX_NAME
              AND I.TABLE_NAME = IC.TABLE_NAME
            WHERE I.TABLE_OWNER = ?
            AND I.TABLE_NAME = ?
            ORDER BY I.INDEX_NAME, IC.COLUMN_POSITION
        """
        return this.sql.rows(query, [owner, table])
    }

    void rebuildIndex(String name) {
        def query = "ALTER INDEX" + name.toString() + "REBUILD"
        this.sql.execute(query)
    }

    void persistDefaultMatches(values, matchTable) {
        String insertQuery = """INSERT INTO ${matchTable} (
            ANALYSIS_ID, ANALYSIS_NAME, RELNO_MAJOR, RELNO_MINOR,
            UPI, METHOD_AC, MODEL_AC, SEQ_START, SEQ_END, FRAGMENTS,
            SEQSCORE, SEQEVALUE, HMM_BOUNDS, HMM_START, HMM_END,
            HMM_LENGTH, ENV_START, ENV_END, SCORE, EVALUE
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        this.sql.withBatch(INSERT_SIZE, insertQuery) { preparedStmt ->
            values.each { row ->
                preparedStmt.addBatch(row)
            }
        }
    }

    void persistDefaultSites(values, siteTable) {
        String insertQuery = """INSERT INTO ${siteTable} (
            ANALYSIS_ID, UPI_RANGE, UPI, MD5, SEQ_LENGTH, ANALYSIS_NAME, METHOD_AC,
            LOC_START, LOC_END, NUM_SITES, RESIDUE, RES_START, RES_END, DESCRIPTION
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        this.sql.withBatch(INSERT_SIZE, insertQuery) { preparedStmt ->
            values.each { row ->
                preparedStmt.addBatch(row)
            }
        }
    }

    void persistMinimalistMatches(values, matchTable) {
        String insertQuery = """INSERT INTO ${matchTable} (
            ANALYSIS_ID, ANALYSIS_NAME, RELNO_MAJOR, RELNO_MINOR,
            UPI, METHOD_AC, MODEL_AC, SEQ_START, SEQ_END, FRAGMENTS
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        this.sql.withBatch(INSERT_SIZE, insertQuery) { preparedStmt ->
            values.each { row ->
                preparedStmt.addBatch(row)
            }
        }
    }

    void persistCddMatches(values, matchTable) {
        String insertQuery = """INSERT INTO ${matchTable} (
            ANALYSIS_ID, ANALYSIS_NAME, RELNO_MAJOR, RELNO_MINOR,
            UPI, METHOD_AC, MODEL_AC, SEQ_START, SEQ_END, FRAGMENTS,
            SEQSCORE, SEQEVALUE
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        this.sql.withBatch(INSERT_SIZE, insertQuery) { preparedStmt ->
            values.each { row ->
                preparedStmt.addBatch(row)
            }
        }
    }

    void persistHamapMatches(values, matchTable) {
        String insertQuery = """INSERT INTO ${matchTable} (
            ANALYSIS_ID, ANALYSIS_NAME, RELNO_MAJOR, RELNO_MINOR,
            UPI, METHOD_AC, MODEL_AC, SEQ_START, SEQ_END, FRAGMENTS,
            SEQSCORE, ALIGNMENT
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        this.sql.withBatch(INSERT_SIZE, insertQuery) { preparedStmt ->
            values.each { row ->
                preparedStmt.addBatch(row)
            }
        }
    }

    void persistMobiDBliteMatches(values, matchTable) {
        String insertQuery = """INSERT INTO ${matchTable} (
            ANALYSIS_ID, ANALYSIS_NAME, RELNO_MAJOR, RELNO_MINOR,
            UPI, METHOD_AC, MODEL_AC, SEQ_START, SEQ_END,
            FRAGMENTS, SEQ_FEATURE
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        this.sql.withBatch(INSERT_SIZE, insertQuery) { preparedStmt ->
            values.each { row ->
                preparedStmt.addBatch(row)
            }
        }
    }

    void persistPantherMatches(values, matchTable) {
        String insertQuery = """INSERT INTO ${matchTable} (
            ANALYSIS_ID, ANALYSIS_NAME, RELNO_MAJOR, RELNO_MINOR,
            UPI, METHOD_AC, MODEL_AC, SEQ_START, SEQ_END, FRAGMENTS,
            SEQSCORE, SEQEVALUE, HMM_BOUNDS, HMM_START, HMM_END,
            HMM_LENGTH, ENV_START, ENV_END, SCORE, EVALUE, AN_NODE_ID
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        this.sql.withBatch(INSERT_SIZE, insertQuery) { preparedStmt ->
            values.each { row ->
                preparedStmt.addBatch(row)
            }
        }
    }

    void persistPirsrMatches(values, matchTable) {
        String insertQuery = """INSERT INTO ${matchTable} (
            ANALYSIS_ID, ANALYSIS_NAME, RELNO_MAJOR, RELNO_MINOR,
            UPI, METHOD_AC, MODEL_AC, SEQ_START, SEQ_END, FRAGMENTS,
            SEQSCORE, SEQEVALUE, HMM_BOUNDS, HMM_START, HMM_END,
            HMM_LENGTH, SCORE, EVALUE, ENV_START, ENV_END
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        this.sql.withBatch(INSERT_SIZE, insertQuery) { preparedStmt ->
            values.each { row ->
                preparedStmt.addBatch(row)
            }
        }
    }

    void persistPrintsMatches(values, matchTable) {
        String insertQuery = """INSERT INTO ${matchTable} (
            ANALYSIS_ID, ANALYSIS_NAME, RELNO_MAJOR, RELNO_MINOR,
            UPI, METHOD_AC, MODEL_AC, SEQ_START, SEQ_END, FRAGMENTS,
            SEQSCORE, SEQEVALUE, MOTIF_NUMBER, PVALUE, GRAPHSCAN
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        this.sql.withBatch(INSERT_SIZE, insertQuery) { preparedStmt ->
            values.each { row ->
                preparedStmt.addBatch(row)
            }
        }
    }

    void persistPrositePatternsMatches(values, matchTable) {
        String insertQuery = """INSERT INTO ${matchTable} (
            ANALYSIS_ID, ANALYSIS_NAME, RELNO_MAJOR, RELNO_MINOR,
            UPI, METHOD_AC, MODEL_AC, SEQ_START, SEQ_END, FRAGMENTS,
            LOCATION_LEVEL, ALIGNMENT
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        this.sql.withBatch(INSERT_SIZE, insertQuery) { preparedStmt ->
            values.each { row ->
                preparedStmt.addBatch(row)
            }
        }
    }

    void persistPrositeProfileMatches(values, matchTable) {
        String insertQuery = """INSERT INTO ${matchTable} (
            ANALYSIS_ID, ANALYSIS_NAME, RELNO_MAJOR, RELNO_MINOR,
            UPI, METHOD_AC, MODEL_AC, SEQ_START, SEQ_END, FRAGMENTS,
            ALIGNMENT
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        this.sql.withBatch(INSERT_SIZE, insertQuery) { preparedStmt ->
            values.each { row ->
                preparedStmt.addBatch(row)
            }
        }
    }

    void persistSignalpMatches(values, matchTable) {
        String insertQuery = """INSERT INTO ${matchTable} (
            ANALYSIS_ID, ANALYSIS_NAME, RELNO_MAJOR, RELNO_MINOR,
            UPI, METHOD_AC, MODEL_AC, SEQ_START, SEQ_END, FRAGMENTS,
            SEQSCORE
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        this.sql.withBatch(INSERT_SIZE, insertQuery) { preparedStmt ->
            values.each { row ->
                preparedStmt.addBatch(row)
            }
        }
    }

    void persistSmartMatches(values, matchTable) {
        String insertQuery = """INSERT INTO ${matchTable} (
            ANALYSIS_ID, ANALYSIS_NAME, RELNO_MAJOR, RELNO_MINOR,
            UPI, METHOD_AC, MODEL_AC, SEQ_START, SEQ_END, FRAGMENTS,
            SEQSCORE, SEQEVALUE, HMM_BOUNDS, HMM_START, HMM_END,
            HMM_LENGTH, SCORE, EVALUE
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        this.sql.withBatch(INSERT_SIZE, insertQuery) { preparedStmt ->
            values.each { row ->
                preparedStmt.addBatch(row)
            }
        }
    }

    void persistSuperfamilyMatches(values, matchTable) {
        String insertQuery = """INSERT INTO ${matchTable} (
            ANALYSIS_ID, ANALYSIS_NAME, RELNO_MAJOR, RELNO_MINOR,
            UPI, METHOD_AC, MODEL_AC, SEQ_START, SEQ_END, FRAGMENTS,
            SEQEVALUE, HMM_LENGTH
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        this.sql.withBatch(INSERT_SIZE, insertQuery) { preparedStmt ->
            values.each { row ->
                preparedStmt.addBatch(row)
            }
        }
    }

    void persistJob(List value) {
        String insertQuery = """INSERT INTO ANALYSIS_JOBS (
            ANALYSIS_ID, UPI_FROM, UPI_TO, CREATED_TIME,
            START_TIME, END_TIME, MAX_MEMORY, LIM_MEMORY,
            CPU_TIME, SUCCESS, SEQUENCES
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

        this.sql.executeInsert(insertQuery, value)
    }
}
