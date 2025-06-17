process RUN_INTERPROSCAN {
    input:
    val job
    val iprscan_exe
    val profile
    val work_dir
    val max_workers
    val sbatch_params
    val iprscan_config

    output:
    tuple val(job), path("*.json")

    script:
    def cmd = ""

    def profileArgs = profile ? "-profile ${profile}" : ""
    def maxWorkers = max_workers ? "--max-workers ${max_workers}" : ""
    def configPath = iprscan_config ? "-c ${iprscan_config}" : ""
    def applications = job.applications.keySet().collect { it.replace(" ", "-").toLowerCase() }.join(',')
    def nfCmd = """
nextflow run ${iprscan_exe} \
    --skip-interpro \
    --formats json \
    --offline \
    --interpro ${job.interproVersion} \
    --input ${job.fasta} \
    --applications ${applications} \
    --datadir ${job.dataDir} \
    -work-dir ${work_dir} \
    ${profileArgs} ${maxWorkers} ${configPath}
"""

    if (sbatch_params.enabled) {
        // submit interproscan as it's own job (with its own resources) to slurm
        def sbatchFileName = "run_interproscan6.sh"
        def sbatchFile = new File(sbatchFileName)
        sbatchFile.text = "#!/bin/bash\n${nfCmd}"
        sbatchFile.setExecutable(true)

        def batchParams = [
                    "--job-name=${sbatch_params.jobName}",
                    "--cpus-per-task=${sbatch_params.cpus}",
                    "--mem=${sbatch_params.memory}",
                    "--time=${sbatch_params.time}"
        ]
        if (sbatch_params.nodes) batchParams << "--nodes=${sbatch_params.nodes}"
        if (sbatch_params.jobLog) batchParams << "--output=${sbatch_params.jobLog}"
        if (sbatch_params.jobErr) batchParams << "--error=${sbatch_params.jobErr}"

        cmd = "sbatch $sbatchFileName ${batchParams.join(' ')}"
    } else {
        // use resources within the IPM job
        cmd = nfCmd
    }
    """
    $cmd
    """
}
