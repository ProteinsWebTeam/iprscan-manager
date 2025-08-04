process RUN_INTERPROSCAN_CPU {
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
    def profileArgs  = profile ? "-profile ${profile}" : ""
    def maxWorkers   = max_workers ? "--max-workers ${max_workers}" : ""
    def configPath   = iprscan_config ? "-c ${iprscan_config}" : ""
    def slurmJobPath = job.name

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

process RUN_INTERPROSCAN_CPU {
    label 'interproscan', 'use_gpu'

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
    def profileArgs  = profile ? "-profile ${profile}" : ""
    def maxWorkers   = max_workers ? "--max-workers ${max_workers}" : ""
    def configPath   = iprscan_config ? "-c ${iprscan_config}" : ""
    def slurmJobPath = job.name

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
