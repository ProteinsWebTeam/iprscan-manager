process RUN_INTERPROSCAN_CPU {
    /* Don't use errorStratgey 'ignore' because no output would be produced,
    so there would be no IprscanJob instances to pass to LOG_JOBS */
    label 'interproscan'

    input:
    val job
    val iprscan_exe
    val profile
    val work_dir
    val max_workers
    val iprscan_config

    output:
    val job
    path "*.json", optional: true
    path "${slurmJobPath.toString()}", optional: true

    script:
    def profileArgs = profile ? "-profile ${profile}" : ""
    def maxWorkers = max_workers ? "--max-workers ${max_workers}" : ""
    def configPath = iprscan_config ? "-c ${iprscan_config}" : ""
    def slurmJobPath = task.workDir.resolve("${job.name}")

    fasta = task.workDir.resolve("${upiFrom}.faa")

    def nfCmd = """
        nextflow run ${iprscan_exe} \\
            --skip-interpro \\
            --formats json \\
            --no-matches-api \\
            --interpro ${job.interproVersion} \\
            --input ${job.fasta} \\
            --applications ${job.application.name} \\
            --datadir ${job.dataDir} \\
            -work-dir ${work_dir} \\
            ${profileArgs} ${maxWorkers} ${configPath}
    """.stripIndent().trim()

    """
    echo \$SLURM_JOB_ID > ${slurmJobPath.toString()}
    ${nfCmd}
    """
}
