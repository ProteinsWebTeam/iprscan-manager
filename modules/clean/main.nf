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
