process RUN_INTERPROSCAN_CPU {
    // errorStrategy needs to be here not the profiles for retry -> ignore: https://github.com/nextflow-io/nextflow/issues/563
    errorStrategy { (task.attempt <= 2) ? 'retry' : 'ignore' }
    label 'interproscan'

    input:
    tuple val(meta), val(job), val(gpu)
    val iprscan_exe
    val profile
    val work_dir
    val max_workers
    val iprscan_config

    output:
    tuple val(meta), val(job), val(gpu), path("slurmJobId"), path("i6matches.json")

    script:
    def profileArgs  = profile ? "-profile ${profile}" : ""
    def maxWorkers   = max_workers ? "--max-workers ${max_workers}" : ""
    def configPath   = iprscan_config ? "-c ${iprscan_config}" : ""

    """
    echo \$SLURM_JOB_ID > slurmJobId
    nextflow run ${iprscan_exe} \\
        --skip-interpro \\
        --formats json \\
        --no-matches-api \\
        --interpro ${job.interproVersion} \\
        --input ${job.fasta} \\
        --outprefix i6matches \\
        --applications ${job.application.name} \\
        --datadir ${job.dataDir} \\
        -work-dir ${work_dir} \\
        ${profileArgs} ${maxWorkers} ${configPath}
    """
}

process RUN_INTERPROSCAN_GPU {
    // errorStrategy needs to be here not the profiles for retry -> ignore: https://github.com/nextflow-io/nextflow/issues/563
    errorStrategy { (task.attempt <= 2) ? 'retry' : 'ignore' }
    label 'interproscan', 'use_gpu'

    input:
    tuple val(meta), val(job), val(gpu)
    val iprscan_exe
    val profile
    val work_dir
    val max_workers
    val iprscan_config

    output:
    tuple val(meta), val(job), val(gpu), path("i6matches.json"), path("slurmJobId")

    script:
    def profileArgs  = profile ? "-profile ${profile}" : ""
    def maxWorkers   = max_workers ? "--max-workers ${max_workers}" : ""
    def configPath   = iprscan_config ? "-c ${iprscan_config}" : ""

    """
    echo \$SLURM_JOB_ID > slurmJobId
    nextflow run ${iprscan_exe} \\
        --skip-interpro \\
        --formats json \\
        --no-matches-api \\
        --interpro ${job.interproVersion} \\
        --input ${job.fasta} \\
        --outprefix i6matches \\
        --applications ${job.application.name} \\
        --datadir ${job.dataDir} \\
        -work-dir ${work_dir} \\
        ${profileArgs} ${maxWorkers} ${configPath}
    """
}
