import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

process RUN_INTERPROSCAN {
    errorStrategy 'ignore'

    input:
    val job
    val iprscan_exe
    val profile
    val work_dir
    val max_workers
    val sbatch_params
    val iprscan_config

    output:
    tuple val(job), path("*.json", optional: true)

    script:
    def profileArgs = profile ? "-profile ${profile}" : ""
    def maxWorkers = max_workers ? "--max-workers ${max_workers}" : ""
    def configPath = iprscan_config ? "-c ${iprscan_config}" : ""

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

    def now = LocalDateTime.now()
    def formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    job.createdTime = now.format(formatter)

    if (sbatch_params.enabled) {
        // Submit the run as it's own job (with its own resources) to slurm
        // srun (instead of sbatch) so it runs synchronously and nextflow will wait
        def batchParams = [
                    "--job-name=${job.jobName}",
                    "--cpus-per-task=${sbatch_params.cpus}",
                    "--mem=${sbatch_params.memory}",
                    "--time=${sbatch_params.time}"
        ]
        if (sbatch_params.nodes) batchParams << "--nodes=${sbatch_params.nodes}"
        if (sbatch_params.jobLog) batchParams << "--output=${sbatch_params.jobLog}"
        if (sbatch_params.jobErr) batchParams << "--error=${sbatch_params.jobErr}"

        """
        echo '#!/bin/bash' > run_interproscan6.sh
        echo "${nfCmd.replace('"', '\\"')}" >> run_interproscan6.sh
        chmod +x run_interproscan6.sh
        sbatch --wait ${batchParams.join(' ')} ./run_interproscan6.sh
        """
    } else {
        // Use the resources within the IPM job
        """
        ${nfCmd}
        """
    }
}
