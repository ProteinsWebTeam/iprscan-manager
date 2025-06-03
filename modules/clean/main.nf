process CLEAN_OBSOLETE_DATA {
    input:
    val iprscan_conf
    val analyses

    exec:
    Database db = new Database(iprscan_conf.uri, iprscan_conf.user, iprscan_conf.password)

    def analysesRows = db.getAnalyses()

    def table2analyses = [:].withDefault { [] }  // table: [analysis ids]
    analysesRows.each { analysis ->
        [analysis.MATCH_TABLE, analysis.SITE_TABLE].each { table ->
            if (table) {
                table2analyses[table] << analysis.ID
            }
        }
    }

    println "table2analyses: ${table2analyses}"

    def tables = table2analyses.keySet().sort()
    def actions = []
    tables.each { table ->
        table = table.toUpperCase()
        partitions = db.getPartitions("IPRSCAN", table)
        println "table: $table --> partitions: $partitions"
    }
}