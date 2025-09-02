process CLEAN_OBSOLETE_DATA {
    input:
    val iprscan_conf
    val analysis_ids

    exec:
    Database db = new Database(iprscan_conf.uri, iprscan_conf.user, iprscan_conf.password)

    def activeAnalysesRows = db.getAnalyses()
    def analysis2maxUpi = [:]                    // analysis id : max UPI
    def table2analyses = [:].withDefault { [] }  // table: [analysis ids]
    activeAnalysesRows.each { analysis ->
        analysis2maxUpi[analysis.ID] = analysis.MAX_UPI

        [analysis.MATCH_TABLE, analysis.SITE_TABLE].each { table ->
            if (table) {
                table2analyses[table] << analysis.ID
            }
        }
    }

    def tables = table2analyses.keySet().sort()
    def actions = []
    def analysisId = null
    def maxUpi = null
    def jobCount = null
    tables.each { table ->
        table = table.toUpperCase()
        def partitions = db.getPartitions("IPRSCAN", table)

        partitions.each { part ->
            if (part.value == "DEFAULT") {
                return
            }
            analysisId = part.value.toInteger()
            maxUpi = analysis2maxUpi[analysisId]

            if (analysis_ids && !analysis_ids.contains(analysisId)) {
                return
            } else if (!analysis2maxUpi.containsKey(analysisId)) {
                actions << [
                   String.format("  - analysis ID %s, partition %-20s: delete data", analysisId, part['name']),
                   [[ String.format("ALTER TABLE %s DROP PARTITION %s", table, part['name']), [] ]]
               ]
            } else if (!table2analyses[table].contains(analysisId)) {
                actions << [
                   String.format("  - analysis ID %s, partition %-20s: delete data", analysisId, part['name']),
                   [[ String.format("ALTER TABLE %s DROP PARTITION %s", table, part['name']), [] ]]
               ]
            } else if (maxUpi) {
                jobCount = db.getJobCount(analysisId, maxUpi)
                if (jobCount > 0) {
                    actions << [
                        String.format("  - analysis ID %s, partition %-20s: delete jobs/data > %s", analysisId, part['name'], maxUpi),
                        [
                            [
                                """
                                DELETE FROM IPRSCAN.ANALYSIS_JOBS
                                WHERE ANALYSIS_ID = ?
                                AND UPI_FROM > ?
                                """.stripIndent(),
                                [analysisId, maxUpi]
                            ],
                            [
                                String.format("""
                                DELETE FROM %s PARTITION (%s)
                                WHERE UPI_FROM > ?
                                """.stripIndent(), table, part['name']),
                                [maxUpi]
                            ]
                        ]
                    ]
                }
            } else {
                actions << [
                    String.format("  - analysis ID %s, partition %-20s: delete jobs/data", analysisId, part['name']),
                    [
                        [
                            "DELETE FROM IPRSCAN.ANALYSIS_JOBS WHERE ANALYSIS_ID = ?",
                            [analysisId]
                        ],
                        [
                            String.format("ALTER TABLE %s TRUNCATE PARTITION %s", table, part['name']),
                            []
                        ]
                    ]
                ]
            }
        } // end of partitions.each
    } // end of tables.each

    if (actions) {
        println "The following actions will be performed:"
        actions.each { desc, queries ->
            println(desc)
        }

        println "proceed? [y/N]"
        def response = System.console().readLine()?.toLowerCase()?.trim()
        if (response == "y") {
            actions.each { desc, queries ->
                queries.each { sql, params ->
                    db.query(sql, params)
                }
            }
        } else {
            println "Canceled"
        }
    } else {
        println "No obsolete data to clean"
    }

    db.close()
}

process REBUILD_INDEXES {
    executor 'local'
    errorStrategy 'ignore'

    /* Prepare the InterProScan database for persisting the matches.
    When partitions have been edited or lost we need to rebuild the indexes. */
    input:
    tuple val(meta), val(job), val(gpu), val(matches_path), val(slurm_id_path)
    val ispro_conf

    output:
    tuple val(meta), val(job), val(gpu), val(matches_path), val(slurm_id_path)

    exec:
    def uri = ispro_conf.uri
    def user = ispro_conf.user
    def pswd = ispro_conf.password
    Database db = new Database(uri, user, pswd)

    def tables = [] as Set
    tables << job.application.matchTable
    if (job.application.siteTable) {
        tables << job.application.siteTable
    }

    def indexes = []
    for (table: tables) {
        index_rows = db.getIndexStatuses("IPRSCAN", table)
        for (row in index_rows) {
            def index_name = row.INDEX_NAME
            def index_status = row.STATUS
            if (index_status == "UNUSABLE" && !indexes.contains(index_name)) {
                indexes << index_name
            }
        }
        for (index: indexes) {
            db.rebuildIndex(index)
        }
    }
}
