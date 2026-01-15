import uk.ac.ebi.interpro.Database
import java.nio.file.*
import static java.nio.file.FileVisitResult.*

process CLEAN_OBSOLETE_DATA {
    input:
    val iprscan_conf
    val analysis_ids

    exec:
    Database db = new Database(
        iprscan_conf.uri,
        iprscan_conf.user,
        iprscan_conf.password,
        iprscan_conf.engine
    )

    def activeAnalysesRows = db.getAnalyses()
    def analysis2maxUpi = [:]                    // analysis id : max UPI
    def table2analyses = [:].withDefault { [] }  // table: [analysis ids]
    activeAnalysesRows.each { analysis ->
        analysis2maxUpi[analysis.id] = analysis.max_upi

        [analysis.match_table, analysis.site_table].each { table ->
            if (table) {
                table2analyses[table] << analysis.id
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
        def partitions = db.getPartitions(table)

        partitions.each { child_name, part_data ->
            def bound = part_data.partition_bound?.toUpperCase().trim()

            if (bound in ["default", "default_pkey"]) {
                return
            }

            def matcher = bound =~ /FOR VALUES IN \((\d+)\)/
            if (matcher.matches()) {
                analysisId = matcher[0][1] as Integer
            } else {
                println "Error: Unexpected partition_bound format for child table ${child_name} — expected 'FOR VALUES IN (X)', got '${part_data.partition_bound}'"
                return
            }

            maxUpi = analysis2maxUpi[analysisId]

            if (analysis_ids && !analysis_ids.contains(analysisId)) {
                return
            } else if (!analysis2maxUpi.containsKey(analysisId)) {
                actions << [
                   String.format("  - analysis ID %s, partition %-20s: delete data", analysisId, child_name),
                   [[ String.format("DROP TABLE %s CASCADE", child_name), [] ]]
               ]
            } else if (!table2analyses[table].contains(analysisId)) {
                actions << [
                   String.format("  - analysis ID %s, partition %-20s: delete data", analysisId, child_name),
                   [[ String.format("DROP TABLE %s CASCADE", child_name), [] ]]
               ]
            } else if (maxUpi) {
                jobCount = db.getJobCount(analysisId, maxUpi)
                if (jobCount > 0) {
                    actions << [
                        String.format("  - analysis ID %s, partition %-20s: delete jobs/data > %s", analysisId, child_name, maxUpi),
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
                                """.stripIndent(), table, child_name),
                                [maxUpi]
                            ]
                        ]
                    ]
                }
            } else {
                actions << [
                    String.format("  - analysis ID %s, partition %-20s: delete jobs/data", analysisId, child_name),
                    [
                        [
                            "DELETE FROM iprscan.analysis_jobs WHERE analysis_id = ?",
                            [analysisId]
                        ],
                        [
                            String.format("TRUNCATE TABLE %s", child_name),
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
            println "Cancelled"
        }
    } else {
        println "No obsolete data to clean"
    }

    db.close()
}

process CLEAN_FASTAS {
    // Delete the FASTA files used for the InterProScan6 jobs
    executor 'local'

    input:
    val all_cpu_jobs  // each = tuple val(meta), val(job), val(gpu)
    val all_gpu_jobs  // each = tuple val(meta), val(job), val(gpu)

    output:
    val all_cpu_jobs
    val all_gpu_jobs

    exec:
    all_fastas = (all_cpu_jobs + all_gpu_jobs).collect { x, job, y -> job.fasta } as Set
    all_fastas.each { fastaPath ->
        try {
            new File(fastaPath).delete()
        } catch (Exception e) {
            println "Failed to delete ${fastaPath}: ${e.message}"
        }
    }
}


process CLEAN_WORKDIRS {
    // Delete InterProScan6 workdirs
    exceutor 'local'

    input:
    val all_cpu_jobs  // each = tuple val(meta), val(job), val(gpu)
    val all_gpu_jobs  // each = tuple val(meta), val(job), val(gpu)

    exce:
    def workDir = task.workdir.parent.parent
    Files.walkFileTree(workDir, new SimpleFileVisitor<Path>() {
        @Override
        FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
            if (dir.fileName.toString() == 'work') {
                println "Deleting: $dir"
                dir.toFile().deleteDir()   // recursive delete
                return SKIP_SUBTREE        // do NOT walk inside it
            }
            return CONTINUE
        }
    })
}