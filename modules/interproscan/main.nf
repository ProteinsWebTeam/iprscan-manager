process RUN_INTERPROSCAN_CPU {
    // errorStrategy needs to be here not the profiles for retry -> ignore: https://github.com/nextflow-io/nextflow/issues/563
    errorStrategy { (task.attempt <= 2) ? 'retry' : 'ignore' }
    label 'interproscan'

    input:
    tuple val(meta), val(job), val(gpu)

    memory {
        def (value, unit) = (params.appsConfig.resources[job.iprscan.resources].memory.toString() =~ /(\d+(?:\.\d+)?)(?:\s*\.?\s*(\w+))?/)[0][1,2]
        "${(value.toDouble() * task.attempt).round(1)} ${unit ?: 'GB'}"
    }
    time {
        def (value, unit) = (params.appsConfig.resources[job.iprscan.resources].time.toString() =~ /(\d+(?:\.\d+)?)(?:\s*\.?\s*(\w+))?/)[0][1,2]
        "${(value.toDouble() * task.attempt).round(1)}${unit ?: 'h'}"
    }


    output:
    tuple val(meta), val(job), val(gpu), path("slurmJobId"), path("i6matches.json")

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
    errorStrategy { (task.attempt <= 2) ? 'retry' : 'ignore' }
    label 'interproscan', 'gpu'

    input:
    tuple val(meta), val(job), val(gpu)

    memory {
        return params.appsConfig.resources[job.iprscan.resources].memory
    }
    time {
        return params.appsConfig.resources[job.iprscan.resources].time
    }

    output:
    tuple val(meta), val(job), val(gpu), path("slurmJobId"), path("i6matches.json")

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
