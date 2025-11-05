// Insert and persist the job data in the interproscan-db ANALYSIS_JOBS table

import uk.ac.ebi.interpro.Database

process LOG_JOBS {
    executor 'local'

    input:
    val successful_persist_matches_jobs // each = tuple val(meta), val(job), val(gpu), val(slurm_id_path)
    val successful_iprscan_jobs         // each = tuple val(meta), val(job), val(gpu), val(slurm_id_path)
    val all_cpu_jobs                    // each = tuple val(meta), val(job), val(gpu)
    val all_gpu_jobs                    // each = tuple val(meta), val(job), val(gpu)
    val iprscan_db_conf

    exec:
    Database db = new Database(
        iprscan_db_conf.uri,
        iprscan_db_conf.user,
        iprscan_db_conf.password,
        iprscan_db_conf.engine
    )

    def allJobsMap = [cpu: [:], gpu: [:]]
    def addToJobMap = { jobList, defaultSuccess, defaultSlurmFile = null ->
        jobList.each { job ->
            def (jobId, jobObj, jobHardware, jobSlurmFile) = job
            def hardware = jobHardware ? 'gpu' : 'cpu'
            if (!allJobsMap[hardware].containsKey(jobId)) {
                allJobsMap[hardware][jobId] = [
                    job        : jobObj,
                    slurmIdFile: jobSlurmFile ?: defaultSlurmFile,
                    success    : defaultSuccess
                ]
            }
        }
    }

    // Add successful persisted jobs
    addToJobMap(successful_persist_matches_jobs, true)

    // Add successful iprscan jobs that failed persisting
    addToJobMap(successful_iprscan_jobs, false)

    // Add all CPU jobs not already in map
    addToJobMap(all_cpu_jobs.collect { it + [null] }, false)

    // Add all GPU jobs not already in map
    addToJobMap(all_gpu_jobs.collect { it + [null] }, false)

    allJobsMap.cpu.each { jobId, jobMap ->
        // Get cluster job info. If not a cluster job all values will be null
        (startTime, endTime, maxMemory, limMemory, cpuTime) = getSlurmJobData(
            jobMap.slurmIdFile.toString(),
            jobMap.job.analysisId
        )
        value = [
            jobMap.job.analysisId,
            jobMap.job.upiFrom,
            jobMap.job.upiTo,
            java.sql.Timestamp.valueOf(jobMap.job.createdTime),
            startTime,
            endTime,
            maxMemory,
            limMemory,
            cpuTime,
            jobMap.success,
            jobMap.job.seqCount
        ]
        db.persistJob(value)
    }
    
    db.close()
}

def getSlurmJobData(String slurm_id_file, int analysis_id) {
    def slurmId   = null
    def startTime = null // timestamp
    def endTime   = null // timestamp
    def maxMemory = null // int
    def limMemory = null // int
    def cpuTime   = null // int

    def batchLine = null
    def mainJobLine = null

    try {
        slurmId = new File(slurm_id_file).text.trim()
    } catch (Exception e) {
        return [startTime, endTime, maxMemory, limMemory, cpuTime]
    }

    def cmd = "sacct -j ${slurmId} --format=JobID,ReqMem,MaxRSS,Elapsed,Start,End,TotalCPU --parsable2"
    def process = cmd.execute()
    process.waitFor()
    def stdout = process.text.trim().readLines()
    // Skip header
    def dataLines = stdout.findAll { it && !it.startsWith("JobID") }
    // Get the batch line (e.g. ends with .batch or contains .ba)
    batchLine = dataLines.find { it.split("\\|")[0] ==~ /.*\.ba.*/ }
    // Get the main job line (exact match with slurmId, no suffix)
    mainJobLine = dataLines.find { it.split("\\|")[0] == slurmId }
    
    if (batchLine && mainJobLine) {
        mainLineFields = mainJobLine.split("\\|")
        batchFields    = batchLine.split("\\|")
        startTime  = java.sql.Timestamp.valueOf(batchFields[4].replace("T", " "))
        endTime    = java.sql.Timestamp.valueOf(batchFields[5].replace("T", " "))
        maxMemory  = parseMemory(batchFields[2], analysis_id, slurmId)    // maxRss
        limMemory  = parseMemory(mainLineFields[1], analysis_id, slurmId) // ReqMem
        cpuTimeStr = batchFields[6]                                       // TotalCPU
        cpuTime    = parseTime(cpuTimeStr).toInteger()
    } else {
        throw new RuntimeException("SLURM batch job not found for job id: ${slurmId} (analysis id: ${analysis_id}). Cannot log this job in the ANALYSIS_JOBS table.")
    }

    return [startTime, endTime, maxMemory, limMemory, cpuTime]
}

def parseMemory(String mem_str, int analysis_id, String slurm_id) {
    memMb = null
    if (mem_str.endsWith("K")) {
        memMb = (mem_str[0..-2].toInteger() / 1024).intValue().toInteger()
    } else if (mem_str.endsWith("G")) {
        memMb = (mem_str[0..-2].toInteger() * 1024).toInteger()
    } else if (mem_str.endsWith("M")) {
        memMb = mem_str.toInteger()
        println "[Warning] - Check the recorded memory for analysis ${analysis_id}, slurm job id ${slurm_id}"
    } else {
        throw new RuntimeException("Unsupported memory unit in '${mem_str}' (analysis id: ${analysis_id}, slurm id ${slurm_id})")
    }
    return memMb
}

def parseTime(String cpuTimeStr) {
    def (days, hh, mm, ss) = [0, 0, 0, 0.0]

    def timeStr = cpuTimeStr
    if (cpuTimeStr.contains("-")) {
        def parts = cpuTimeStr.split("-")
        days = parts[0].toInteger()
        timeStr = parts[1]
    }

    def timeParts = timeStr.split(":")
    switch (timeParts.size()) {
        case 3:
            (hh, mm, ss) = [timeParts[0].toInteger(), timeParts[1].toInteger(), timeParts[2].toFloat()]
            break
        case 2:
            (mm, ss) = [timeParts[0].toInteger(), timeParts[1].toFloat()]
            break
        default:
            throw new RuntimeException("Unexpected CPU time format: ${cpuTimeStr}")
    }

    return ((days * 86400) + (hh * 3600) + (mm * 60) + ss) / 60
}
