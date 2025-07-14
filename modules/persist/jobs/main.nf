// Insert and persist the job data in the IS-db (e.g. ISPRO) ANALYSIS_JOBS table

process LOG_JOB {
    input:
    val job

    exec:
    def cmd = "sacct --name=${job.jobName} --format=JobID,jobName,State,Elapsed,MaxRSS"
    def process = cmd.execute()
    process.waitFor()
    def stdout = process.text.trim().readLines()
    println "stdout: ${stdout}"
}
