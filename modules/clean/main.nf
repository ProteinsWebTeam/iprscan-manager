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

    println "table2analyses: ${table2analyses}"

    def tables = table2analyses.keySet().sort()
    def actions = []
    def analysisId = null
    def maxUpi = null
    def jobCount = null
    tables.each { table ->
        table = table.toUpperCase()
        partitions = db.getPartitions("IPRSCAN", table) // e.g. [[name:HAMAP2025_01, position:1, value:130, column:ANALYSIS_ID], [name:OTHER, position:2, value:DEFAULT, column:ANALYSIS_ID]]

        partitions.each { part ->
            if (part.value == "DEFAULT") {
                return
            }
            analysisId = part.value.toInteger()
            maxUpi = analysis2maxUpi[analysisId]

            if (analysis_ids && !analysis_ids.contains(analysisId)) {
                return
            } else if (!analysis2maxUpi.containsKey(analysisId)) {
                // obsolete analysis - remove the data
                actions << [
                   String.format("  - %-30s: delete data", part['name']),
                   [[ String.format("ALTER TABLE %s DROP PARTITION %s", table, part['name']), [] ]]
               ]
            } else if (!table2analyses[table].contains(analysisId)) {
                // obsolete analysis - remove the data
                actions << [
                   String.format("  - %-30s: delete data", part['name']),
                   [[ String.format("ALTER TABLE %s DROP PARTITION %s", table, part['name']), [] ]]
               ]
            } else if (maxUpi) {
                jobCount = db.getJobCount(analysisId, maxUpi)
                if (jobCount > 0) {
                    // Delete jobs after the max UPI
                    actions << [
                        String.format("  - %-30s: delete jobs/data > %s", part['name'], maxUpi),
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
                // no max UPI: remove the data
                actions << [
                    String.format("  - %-30s: delete jobs/data", part['name']),
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
            println "table: $table --> partitions: $partitions"
        } // end of partitions.each
    } // end of tables.each
    println "actions: $actions"
}