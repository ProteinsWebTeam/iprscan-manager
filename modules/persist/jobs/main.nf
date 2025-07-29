// Insert and persist the job data in the IS-db (e.g. ISPRO) ANALYSIS_JOBS table

process LOG_JOB {
    input:
    tuple val(job), val(persist_matches_success)
    val iprscan_db_conf
    val sbatch_params

    exec:
    def cmd = "sacct --name=${job.jobName} --format=JobID,JobName,State,Elapsed,Start,End,TotalCPU,MaxRSS --parsable2"
    def process = cmd.execute()
    process.waitFor()
    def stdout = process.text.trim().readLines()
    // Skip header
    def dataLines = stdout.findAll { it && !it.startsWith("JobID") }
    // Filter lines matching the batch job pattern
    def batchLines = dataLines.findAll { it.split("\\|")[0] ==~ /.*\.ba.*/ }
    // Sort batch lines by start time (field index 4)
    def sortedBatchLines = batchLines.sort { a, b ->
        def startA = java.sql.Timestamp.valueOf(a.split("\\|")[4].replace("T", " "))
        def startB = java.sql.Timestamp.valueOf(b.split("\\|")[4].replace("T", " "))
        return startB <=> startA  // descending order
    }
    // Pick the latest one
    def batchLine = sortedBatchLines ? sortedBatchLines[0] : null

    if (batchLine) {
        def fields     = batchLine.split("\\|")
        def state      = (fields[2] == "COMPLETED" && persist_matches_success == true) ? "Y" : "N"
        def startTime  = java.sql.Timestamp.valueOf(fields[4].replace("T", " "))
        def endTime    = java.sql.Timestamp.valueOf(fields[5].replace("T", " "))
        def maxRss = fields[7]
        def cpuTimeStr = fields[6]
        float cpuTimeMinutes

        if (cpuTimeStr.contains("-")) {
            // Format: D-HH:MM:SS
            def (daysPart, timePart) = cpuTimeStr.split("-")
            def timeParts = timePart.split(":")
            if (timeParts.size() == 3) {
                def (hh, mm, ssStr) = timeParts
                def ss = ssStr.toFloat()
                int days = daysPart.toInteger()
                cpuTimeMinutes = ((days * 86400) + (hh.toInteger() * 3600) + (mm.toInteger() * 60) + ss) / 60
            } else {
                throw new RuntimeException("Unexpected time format in: ${cpuTimeStr}")
            }
        } else {
            def timeParts = cpuTimeStr.split(":")
            if (timeParts.size() == 3) {
                def (hh, mm, ssStr) = timeParts
                def ss = ssStr.toFloat()
                cpuTimeMinutes = (hh.toInteger() * 3600 + mm.toInteger() * 60 + ss) / 60
            } else if (timeParts.size() == 2) {
                def (mm, ssStr) = timeParts
                def ss = ssStr.toFloat()
                cpuTimeMinutes = (mm.toInteger() * 60 + ss) / 60
            } else {
                throw new RuntimeException("Unexpected CPU time format: ${cpuTimeStr}")
            }
        }

        def maxRssKb = fields[7]
        maxRssMb = (maxRssKb[0..-2].toInteger() / 1024).intValue()
        def memGb = sbatch_params.memory  // e.g., "16GB"
        memMb = (memGb[0..-3].toInteger() * 1024)

        value = [
            job.analysis_id,
            job.upiFrom,
            job.upiTo,
            java.sql.Timestamp.valueOf(job.createdTime.replace("T", " ")),
            startTime,
            endTime,
            maxRssMb,
            memMb,
            cpuTimeSec,
            state,
            job.seqCount
        ]

        Database db = new Database(iprscan_db_conf.uri, iprscan_db_conf.user, iprscan_db_conf.password)
        db.persistJob(value)
        db.close()
    } else {
        throw new RuntimeException("SLURM batch job not found for job name: ${job.jobName}. Cannot log this job in the ANALYSIS_JOBS table.")
    }
}

