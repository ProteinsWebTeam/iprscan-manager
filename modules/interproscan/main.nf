process RUN_INTERPROSCAN_CPU {
    // errorStrategy needs to be here not the profiles for retry -> ignore: https://github.com/nextflow-io/nextflow/issues/563
    errorStrategy { (task.attempt <= 2) ? 'retry' : 'ignore' }
    label 'interproscan'

    input:
    tuple val(meta), val(job), val(gpu)

    cpus {
        job.iprscan.resources.cpus
    }
    memory {
        job.iprscan.resources.mem
    }
    time {
        job.iprscan.resources.time
    }

    output:
    tuple val(meta), val(job), val(gpu), path("i6matches.json"), path("slurmJobId")

    script:
    def profileArgs  = job.iprscan.profile ? "-profile ${job.iprscan.profile}" : ""
    def maxWorkers   = job.iprscan.maxWorkers ? "--max-workers ${job.iprscan.maxWorkers}" : ""
    def configPath   = job.iprscan.configFile ? "-c ${job.iprscan.configFile}" : ""

    """
    echo \$SLURM_JOB_ID > slurmJobId
    nextflow run ${job.iprscan.executable} \\
        --skip-interpro \\
        --formats json \\
        --no-matches-api \\
        --interpro ${job.interproVersion} \\
        --input ${job.fasta} \\
        --outprefix i6matches \\
        --applications ${job.application.name} \\
        --datadir ${job.dataDir} \\
        -work-dir ${job.iprscan.workDir} \\
        ${profileArgs} ${maxWorkers} ${configPath}
    """
}

process RUN_INTERPROSCAN_GPU {
    // errorStrategy needs to be here not the profiles for retry -> ignore: https://github.com/nextflow-io/nextflow/issues/563
    errorStrategy { (task.attempt <= 8) ? 'retry' : 'ignore' }
    label 'interproscan', 'gpu'

    input:
    tuple val(meta), val(job), val(gpu)

    cpus {
        job.iprscan.resources.cpus
    }
    memory {
        job.iprscan.resources.mem
    }
    time {
        job.iprscan.resources.time
    }

    output:
    tuple val(meta), val(job), val(gpu), path("i6matches.json"), path("slurmJobId")

    script:
    def profileArgs  = job.iprscan.profile ? "-profile ${job.iprscan.profile}" : ""
    def maxWorkers   = job.iprscan.maxWorkers ? "--max-workers ${job.iprscan.maxWorkers}" : ""
    def configPath   = job.iprscan.configFile ? "-c ${job.iprscan.configFile}" : ""

    """
    echo \$SLURM_JOB_ID > slurmJobId
    nextflow run ${job.iprscan.executable} \\
        --skip-interpro \\
        --formats json \\
        --no-matches-api \\
        --interpro ${job.interproVersion} \\
        --input ${job.fasta} \\
        --outprefix i6matches \\
        --applications ${job.application.name} \\
        --datadir ${job.dataDir} \\
        -work-dir ${job.iprscan.workDir} \\
        ${profileArgs} ${maxWorkers} ${configPath} \\
        --use-gpu
    """
}
