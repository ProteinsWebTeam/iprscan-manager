import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

process RUN_INTERPROSCAN {
    errorStrategy 'ignore'  // allows us to mark a successful/unsuccessful run in ISPRO

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
    // Compile the nextflow command
    def profileArgs = profile ? "-profile ${profile}" : ""
    def maxWorkers = max_workers ? "--max-workers ${max_workers}" : ""
    def configPath = iprscan_config ? "-c ${iprscan_config}" : ""

    def nfCmd = """
nextflow run ${iprscan_exe} \
    --skip-interpro \
    --formats json \
    --no-matches-api \
    --interpro ${job.interproVersion} \
    --input ${job.fasta} \
    --applications ${job.application.name} \
    --datadir ${job.dataDir} \
    -work-dir ${work_dir} \
    ${profileArgs} ${maxWorkers} ${configPath}
"""

    // Mark the time the job was created - for the ANALYSIS_JOB table
    def now = LocalDateTime.now()
    def formatter = DateTimeFormatter.ofPattern("yyyy-MM-2 HH:mm:ss")
    formattedDateTime = now.format(formatter)
    job.createdTime = formattedDateTime

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

        /*
        Create a bash script to run iprscan, and if it fails on the first attemp
        we automatically resubmit the job with more time and memory.
        The exact cmd that is run is stored in the .command.sh file in the workdir.
        */
        """
        #!/bin/bash
        set -e

        attempt=1
        max_attempts=2
        mem="${sbatch_params.memory}"
        time="${sbatch_params.time}"

        while [ \$attempt -le \$max_attempts ]; do
            echo "Attempt \$attempt: Running InterProScan with memory=\$mem and time=\$time"

            # Use srun synchronously - this will block until completion
            if srun --job-name=${job.jobName} \\
                --cpus-per-task=${sbatch_params.cpus} \\
                --mem=\$mem \\
                --time=\$time \\
                ${sbatch_params.nodes ? "--nodes=${sbatch_params.nodes}" : ""} \\
                ${sbatch_params.jobLog ? "--output=${sbatch_params.jobLog}" : ""} \\
                ${sbatch_params.jobErr ? "--error=${sbatch_params.jobErr}" : ""} \\
                bash -c "${nfCmd.replace('"', '\\"')}"; then
                echo "Run succeeded on attempt \$attempt"
                exit 0
            else
                echo "Run failed on attempt \$attempt"
                attempt=\$((attempt + 1))
                if [ \$attempt -le \$max_attempts ]; then
                    # Calculate new memory and time with 25% increase
                    mem=\$(echo "\$mem" | sed 's/[^0-9]//g')
                    time_num=\$(echo "\$time" | sed 's/[^0-9]//g')
                    
                    # Get the original unit (e.g., G, M for memory and format for time)
                    mem_unit=\$(echo "${sbatch_params.memory}" | sed 's/[0-9]//g')
                    time_unit=\$(echo "${sbatch_params.time}" | sed 's/[0-9]//g')
                    
                    # Calculate new values (25% increase)
                    new_mem=\$((mem * 5 / 4))
                    new_time=\$((time_num * 5 / 4))
                    
                    mem="\${new_mem}\${mem_unit}"
                    time="\${new_time}\${time_unit}"
                    
                    echo "Retrying with increased resources: memory=\$mem, time=\$time"
                fi
            fi
        done

        echo "All attempts failed. Proceeding with pipeline (errorStrategy=ignore)."
        exit 1
        """
    } else {
        // Use the resources within the IPM job
        """
        ${nfCmd}
        """
    }
}
