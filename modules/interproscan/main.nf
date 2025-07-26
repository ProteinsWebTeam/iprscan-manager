import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

process RUN_INTERPROSCAN {
    /* Don't use errorStratgey 'ignore' because no output would be produced,
    so there would be no IprscanJob instances to pass to LOG_JOBS */

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
        def batchParams = [
            "--job-name=${job.jobName}",
            "--cpus-per-task=${sbatch_params.cpus}",
            "--mem=${sbatch_params.memory}",
            "--time=${sbatch_params.time}"
        ]
        if (sbatch_params.nodes) batchParams << "--nodes=${sbatch_params.nodes}"
        if (sbatch_params.jobLog) batchParams << "--output=${sbatch_params.jobLog}"
        if (sbatch_params.jobErr) batchParams << "--error=${sbatch_params.jobErr}"

        def runScript = """
        #!/bin/bash
        ${nfCmd}
        """.stripIndent()

        /* If InterProScan crashes try the same sbatch run
        with more memory and time */

        def sbatchCmd1 = "sbatch --wait ${batchParams.join(' ')} ./run_interproscan6.sh"

        def sbatchCmd2 = """
        mem_val="${sbatch_params.memory.replaceAll('[^0-9]', '')}"
        mem_unit="${sbatch_params.memory.replaceAll('[0-9]', '')}"
        new_mem=\$(echo "\$mem_val * 1.25" | bc | awk '{print int(\$1+0.5)}')

        new_time=\$(python3 -c "import re; t='${sbatch_params.time}'; m=re.match(r'(\\\\d+)-(\\\\d+):(\\\\d+)', t); print(f'{m.group(1)}-{int(int(m.group(2)*1.25))}:{int(int(m.group(3)*1.25))}')")

        sbatch --wait --job-name=${job.jobName}_retry --cpus-per-task=${sbatch_params.cpus} --mem=\${new_mem}\${mem_unit} --time=\$new_time ./run_interproscan6.sh
        """.stripIndent().trim()

        """
        set +e  # Disable exit on error so we can pass the job object downstream

        echo "#!/bin/bash" > run_interproscan6.sh
        echo "${nfCmd.replace('"', '\\"')}" >> run_interproscan6.sh
        chmod +x run_interproscan6.sh

        # First attempt
        ${sbatchCmd1}
        exit_code=\$?
        echo "First sbatch attempt exit code: \$exit_code"

        # Initialize retry exit code (assume success unless retry is needed)
        retry_exit_code=0

        if [ \$exit_code -ne 0 ]; then
            echo "Retrying with increased resources..."
            ${sbatchCmd2}
            retry_exit_code=\$?
            echo "Retry sbatch attempt exit code: \$retry_exit_code"
        fi

        # Only create failed.json if BOTH attempts failed
        if [ \$exit_code -ne 0 ] && [ \$retry_exit_code -ne 0 ]; then
            echo '{}' > failed.json  # Create a dummy JSON
        fi

        echo "Process completed, moving on..."
        exit 0
        """
    } else {
        """
        ${nfCmd} || exit 0
        """
    }
}