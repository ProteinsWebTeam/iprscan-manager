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
        String url = "${this.DRIVERS[engine].prefix}:${uri}"
        String driver = this.DRIVERS[engine].driver
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
        def query = "SELECT MAX(UPI) FROM iprscan.protein"
        return this.sql.rows(query)[0][0]
    }

    void dropProteinTable() {
        this.sql.execute("DROP TABLE IF EXISTS iprscan.protein;")
    }

    void buildProteinTable() {
        this.sql.execute(
            """
            CREATE TABLE iprscab.protein
            (
                upi CHAR(13) NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                sequence TEXT NOT NULL,
                length NUMBER(6) NOT NULL,
                crc64 CHAR(16) NOT NULL,
                md5 VARCHAR2(32) NOT NULL
            ) NOLOGGING
            """
        )
    }

    void configureProteinTable() {
        this.sql.execute("GRANT SELECT ON iprscan.PROTEIN TO PUBLIC")
        this.sql.execute("CREATE UNIQUE INDEX ON iprscan.PROTEIN (UPI)")
    }

    List<String> iterProteins(String gt = null, String le = null, Closure rowHandler) {
        String sqlQuery = """
            SELECT UPI, TIMESTAMP, SEQ_SHORT, SEQ_LONG, LEN, CRC64, MD5
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
            INSERT /*+ APPEND */ INTO iprscan.protein
            VALUES (?, ?, ?, ?, ?, ?)
        """
        this.sql.withBatch(INSERT_SIZE, insertQuery) { preparedStmt ->
            records.each { row ->
                preparedStmt.addBatch(row)
            }
        }
    }

    List<String> getAnalyses() {
        def query = """
            SELECT A.max_upi, A.i6_dir, A.INTERPRO_VERSION, A.name,
                T.match_table, T.site_table,
                A.id, A.version, A.gpu
            FROM iprscan.analysis A
            INNER JOIN iprscan.analysis_tables T
                ON LOWER(A.name) = LOWER(T.name)
            WHERE A.active AND
                A.i6_dir IS NOT NULL
        """
        return this.sql.rows(query)
    }

    Map<String, Map> getPartitions(table_name) {
        // The parition bound is "DEFAULT" or "FOR VALUES IN ($analysis_id)"
        String query ="""
            SELECT
                p.relname parent_name,
                c.relname child_name,
                pg_get_expr(c.relpartbound, c.oid) AS partition_bound
            FROM pg_class p
            JOIN pg_inherits i ON p.oid = i.inhparent
            JOIN pg_class c ON i.inhrelid = c.oid
            WHERE c.relpartbound IS NOT NULL
            AND p.relname = ?
            ORDER BY parent_name, child_name;
        """
        Map<String, Map> partitions = [:]

        this.sql.eachRow(query, [table_name.toLowerCase()]) { row ->
            partitions[row.child_name] = [
                    parent         : row.parent_name,
                    partition_bound: row.partition_bound,
            ]
        }

        return partitions
    }

    Integer getJobCount(Integer analysis_id, String max_upi) {
        return this.sql.rows(
            "SELECT COUNT (*) FROM IPRSCAN.ANALYSIS_JOBS WHERE ANALYSIS_ID = ? AND UPI_FROM > ?",
            [analysis_id, max_upi]
        )[0]
    }

    List<String> getUpiRange(String upi_from, String upi_to) {
        return this.sql.rows(
            "SELECT UPI FROM IPRSCAN.PROTEIN WHERE UPI > ? AND UPI <= ? ORDER BY UPI",
            [upi_from, upi_to]
        ).collect { it.UPI }
    }

    Integer writeFasta(String upi_from, String upi_to, String fasta) {
        // Build a fasta file of protein seqs. Batch for speed.
        def writer = new File(fasta.toString()).newWriter()
        Integer seqCount = 0
        Integer offset = 0
        Integer batchSize = 1000
        String query = """
        SELECT upi, sequence
        FROM iprscan.protein
        WHERE upi BETWEEN ? AND ?
        ORDER BY upi
        LIMIT ? OFFSET ?
        """

        while (true) {
            def batch = this.sql.rows(query, [upi_from, upi_to, batchSize, offset])
            for (row: batch) {
                if (row.sequence) {
                    writer.writeLine(">${row.upi}")
                    for (int i = 0; i < row.sequence.length(); i += 60) {
                        int end = Math.min(i + 60, row.sequence.length())
                        writer.writeLine(row.sequence.substring(i, end))
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
        String insertQuery = """INSERT INTO iprscan.${matchTable} (
            analysis_id, analysis_name, relno_major, relno_minor,
            upi, method_ac, model_ac, seq_start, seq_end, fragments,
            seqscore, seqevalue, hmm_bounds, hmm_start, hmm_end,
            hmm_length, env_start, env_end, score, evalue
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
        String insertQuery = """INSERT INTO iprscan.${siteTable} (
            analysis_id, upi, md5, seq_length, analysis_name, method_ac,
            loc_start, loc_end, num_sites, residue, res_start, res_end, description
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        this.sql.withBatch(INSERT_SIZE, insertQuery) { preparedStmt ->
            values.each { row ->
                preparedStmt.addBatch(row)
            }
        }
    }

    void persistMinimalistMatches(values, matchTable) {
        String insertQuery = """INSERT INTO iprscan.${matchTable} (
            analysis_id, analysis_name, relno_major, relno_minor,
            upi, method_ac, model_ac, seq_start, seq_end, fragments
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
        String insertQuery = """INSERT INTO iprscan.${matchTable} (
            analysis_id, analysis_name, relno_major, relno_minor,
            upi, method_ac, model_ac, seq_start, seq_end, fragments,
            seqscore, seqevalue
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
        String insertQuery = """INSERT INTO iprscan.${matchTable} (
            analysis_id, analysis_name, relno_major, relno_minor,
            upi, method_ac, model_ac, seq_start, seq_end, fragments,
            seqscore, alignment
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
        String insertQuery = """INSERT INTO iprscan.${matchTable} (
            analysis_id, analysis_name, relno_major, relno_minor,
            upi, method_ac, model_ac, seq_start, seq_end,
            fragments, seq_feature
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
        String insertQuery = """INSERT INTO iprscan.${matchTable} (
            analysis_id, analysis_name, relno_major, relno_minor,
            upi, method_ac, model_ac, seq_start, seq_end, fragments,
            seqscore, seqevalue, hmm_bounds, hmm_start, hmm_end,
            hmm_length, env_start, env_end, an_node_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        this.sql.withBatch(INSERT_SIZE, insertQuery) { preparedStmt ->
            values.each { row ->
                preparedStmt.addBatch(row)
            }
        }
    }

    void persistPirsrMatches(values, matchTable) {
        String insertQuery = """INSERT INTO iprscan.${matchTable} (
            analysis_id, analysis_name, relno_major, relno_minor,
            upi, method_ac, model_ac, seq_start, seq_end, fragments,
            seqscore, seqevalue, hmm_bounds, hmm_start, hmm_end,
            hmm_length, score, evalue, env_start, env_end
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
        String insertQuery = """INSERT INTO iprscan.${matchTable} (
            analysis_id, analysis_name, relno_major, relno_minor,
            upi, method_ac, model_ac, seq_start, seq_end, fragments,
            seqscore, seqevalue, motif_number, pvalue, graphscan
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
        String insertQuery = """INSERT INTO iprscan.${matchTable} (
            analysis_id, analysis_name, relno_major, relno_minor,
            upi, method_ac, model_ac, seq_start, seq_end, fragments,
            location_level, alignment
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
        String insertQuery = """INSERT INTO iprscan.${matchTable} (
            analysis_id, analysis_name, relno_major, relno_minor,
            upi, method_ac, model_ac, seq_start, seq_end, fragments,
            alignment
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
        String insertQuery = """INSERT INTO iprscan.${matchTable} (
            analysis_id, analysis_name, relno_major, relno_minor,
            upi, method_ac, model_ac, seq_start, seq_end, fragments,
            seqscore
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
        String insertQuery = """INSERT INTO iprscan.${matchTable} (
            analysis_id, analysis_name, relno_major, relno_minor,
            upi, method_ac, model_ac, seq_start, seq_end, fragments,
            seqscore, seqevalue, hmm_bounds, hmm_start, hmm_end,
            hmm_length, score, evalue
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
        String insertQuery = """INSERT INTO iprscan.${matchTable} (
            analysis_id, analysis_name, relno_major, relno_minor,
            upi, method_ac, model_ac, seq_start, seq_end, fragments,
            seqevalue, hmm_length
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
        String insertQuery = """INSERT INTO iprscan.analysis_jobs (
            analysis_id, upi_from, upi_to, created_time,
            start_time, end_time, max_memory, lim_memory,
            cpu_time, success, sequences
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        this.sql.executeInsert(insertQuery, value)
    }
}
