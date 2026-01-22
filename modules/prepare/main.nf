import uk.ac.ebi.interpro.Application
import uk.ac.ebi.interpro.Database
import uk.ac.ebi.interpro.FastaFile
import uk.ac.ebi.interpro.Iprscan
import uk.ac.ebi.interpro.Job

process GET_ANALYSES {
    // Identify build a job for each analysis
    executor 'local'

    input:
    val iprscan_db_conf
    val analysis_ids

    output:
    val analyses
    val analysisList

    exec:
    Database db = new Database(
        iprscan_db_conf.uri,
        iprscan_db_conf.user,
        iprscan_db_conf.password,
        iprscan_db_conf.engine
    )

    // if --list is used, format the analyses for display
    def widths = [6, 20, 0]
    def headers = ["Id", "Name", "Version"]
    def formatRow = { values ->
        values
            .indexed()
            .collect { i, v ->
                v.toString().padRight(widths[i])
            } .join("\t")
    }

    // Group jobs by UPI so that we only need query to define the batches once per unique upi range
    analyses = [:].withDefault { [] } // [upiFrom-upiTo: [jobs]]

    // If --analysis-ids is used, filter the jobs to run
    analysisList = [formatRow(headers)] as Set
    def selectedIds = [] as Set
    selectedIds = analysis_ids.toString()
        .split(",")
        .collect { it.trim() }
        .findAll { it }
        .collect { it.toInteger() }
        .toSet()

    // Get the new analyses from the iprscan.analysis table
    def analysis_rows = db.getAnalyses(selectedIds)
    def resubmission = false
    upiTo = null
    for (row: analysis_rows) {
        (upiFrom, dataDir, interproVersion, dbName, matchTable, siteTable, analysisId, dbVersion, gpu) = row
        application = new Application(dbName, dbVersion, matchTable, siteTable)
        job = new Job(
            analysisId.toInteger(),
            resubmission,
            upiFrom,
            dataDir,
            interproVersion,
            gpu,
            application
        )
        analyses["${upiFrom}-${upiTo}"] << job
        analysisList.add(formatRow([job.analysisId, job.application.name, job.application.version]))
    }

    // Get analyses/jobs that failed to run previously so that they can be re-run/resubmit
    def job_rows = db.getFailedJobs(selectedIds)
    resubmission = true
    for (row: job_rows) {
        (_, analysisId, upiFrom, upiTo, seqCount, dataDir, interproVersion, dbName, matchTable, siteTable, dbVersin, gpu) = row
        application = new Application(dbName, dbVersion, matchTable, siteTable)
        job = new Job(
            analysisId.toInteger(),
            resubmission,
            upiFrom,
            dataDir,
            interproVersion,
            gpu,
            application,
            seqCount,
            upiFrom,
            upiTo
        )
        analyses["${upiFrom}-${upiTo}"] << job
        analysisList.add(formatRow([job.analysisId, job.application.name, job.application.version]))
    }

    db.close()
}

process BUILD_JOBS {
    // Get sequences to analyse. Return two lists of Jobs: cpu and gpu jobs
    executor 'local'

    input:
    val iprscan_db_conf
    val apps_config
    val analyses
    val cpu_iprscan
    val gpu_iprscan
    val batch_size
    val max_jobs_per_analysis

    output:
    val cpuJobs
    val gpuJobs

    exec:
    // Convert the closure-based config to simple values immediately to avoid "cannot serialise context map" warning
    def simpleAppsConfig = [
        applications: [:],
        resources: [:]
    ]
    apps_config.applications.each { key, value ->
        simpleAppsConfig.applications[key.toLowerCase()] = value
    }
    apps_config.resources.each { resourceType, closure ->
        def result = [:]
        closure.delegate = result
        closure.resolveStrategy = Closure.DELEGATE_FIRST
        closure()
        simpleAppsConfig.resources[resourceType] = result
    }
    apps_config = null

    Database db = new Database(
        iprscan_db_conf.uri,
        iprscan_db_conf.user,
        iprscan_db_conf.password,
        iprscan_db_conf.engine
    )

    // For each UPI range (from - to) identify the upi ranges for the batches
    def maxUpiTo = db.getMaxUPI()
    def allBatches = [:].withDefault { [] } // [[upiFrom: str, upiTo: str, seqCount: int]]
    analyses.each { key, analysisList ->
        def upiFrom = analysisList[0].maxUpi
        def upiTo = analysisList[0].upiTo ?: maxUpiTo
        allBatches[key] = [
            upiFrom: upiFrom,
            upiTo: upiTo,
            batches: db.defineBatches(upiFrom, upiTo, task.workDir, batch_size)
        ]
    }

    // Create a job for each batch per analysis, and persist the jobs in the interproscan.iprscan.analysis_jobs table
    cpuJobs = []
    gpuJobs = []
    def jobRecords = []
    def emptyJobRecords = []
    def jobsByAnalysis = [:].withDefault { [] }
    allBatches.each { String key, Map batchMap ->
        analyses[key].each { Job job ->
            if (batchMap.batches.isEmpty()) { // no seqs to analyse, mark these jobs as successful
                def seqCount = 0
                def success = true
                def batchJob = new Job(job.analysisId)
                emptyJobRecords.add( [
                        job.analysisId,
                        batchMap.upiFrom,
                        batchMap.upiTo,
                        java.sql.Timestamp.valueOf(batchJob.createdTime),
                        seqCount,
                        success
                    ]
                )
            }

            batchMap.batches.each { Map batch ->
                def iprscanSource = job.gpu ? gpu_iprscan : cpu_iprscan
                Iprscan iprscanConfig = new Iprscan(
                    iprscanSource.executable,
                    iprscanSource.profile,
                    iprscanSource.maxWorkers,
                    iprscanSource.configFile,
                )

                iprscanConfig.addResources(
                    simpleAppsConfig,
                    job.application.name.toLowerCase().replace("-", "_"),
                    job.gpu
                )

                def batchJob = new Job(
                    job.analysisId, job.resubmission, job.maxUpi,
                    job.dataDir, job.interproVersion, job.gpu,
                    job.application, iprscanConfig,
                    batch.seqCount, batch.upiFrom, batch.upiTo
                )

                if (max_jobs_per_analysis <= 0 || jobsByAnalysis[job.analysisId].size() < max_jobs_per_analysis) {
                    (job.gpu ? gpuJobs : cpuJobs) << batchJob

                    jobRecords.add(
                        [
                            batchJob.analysisId,
                            batchJob.upiFrom,
                            batchJob.upiTo,
                            java.sql.Timestamp.valueOf(batchJob.createdTime),
                            batchJob.seqCount
                        ]
                    )

                    jobsByAnalysis[job.analysisId] << [job: job, batch: batch]
                }
            }
        }
    }
    if (!emptyJobRecords.isEmpty()) {
        db.insertEmptyJobs(emptyJobRecords)
    }
    if (!jobRecords.isEmpty()) {
        db.insertJobs(jobRecords)
    }

    // Identify analysis records whose maxUpi needs updating
    def analysisRecords = []
    groupedAnalyses = jobsByAnalysis.collectEntries { Integer analysisId, List<Map> entries ->
        // If there is a new analysis (a non-resubmission) use the maximum upi of the db
        def anyNewAnalyses = entries.any { !it.job.resubmission }
        if (anyNewAnalyses) {
            analysisRecords.add( [int_to_upi(maxUpiTo), analysisId])
        }

        // We are only handling resubmitted jobs, grab the max upi from these resubmitted jobs
        // and only update if it is greater than the current max_upi in iprscan.analysis
        def newMaxUpi = entries.collect {
            upi_to_int(it.batch.upiTo) + 1
        }.max()
        currentMaxUpi = upi_to_int(db.getAnalysisMaxUpi(analysisId))
        if (currentMaxUpi < newMaxUpi) {
            analysisRecords.add( [int_to_upi(newMaxUpi), analysisId] )
        }
    }
    if (!analysisRecords.isEmpty()) {
        db.updateAnalyses(analysisRecords)
    }

    db.close()
}


def upi_to_int(String upi) {
    if (upi == null || !upi.startsWith("UPI") || upi.length() != 13) {
        throw new IllegalArgumentException("Invalid UniParc ID: ${upi}")
    }
    def hexPart = upi.substring(3)
    if (!(hexPart ==~ /^[0-9A-Fa-f]{10}$/)) {
        throw new IllegalArgumentException("Invalid UniParc ID hex: ${upi}")
    }
    return Long.parseLong(hexPart, 16)
}


def int_to_upi(long value) {
    if (value < 0) {
        throw new IllegalArgumentException("UniParc ID value must be non-negative: ${value}")
    }
    def hexPart = Long.toHexString(value).toUpperCase()
    if (hexPart.length() > 10) {
        throw new IllegalArgumentException("UniParc ID hex exceeds 10 chars: ${hexPart}")
    }
    return "UPI" + hexPart.padLeft(10, '0')
}


process EXPORT_FASTA {
    // Build the fasta file for each upi-range
    // This saves writing duplicate files when multiple analyses are activated
    executor 'local'
    maxForks 10

    input:
    val iprscan_db_conf
    tuple val(meta), val(job), val(gpu)

    output:
    tuple val(meta), val(job), val(gpu)

    exec:
    Database db = new Database(
        iprscan_db_conf.uri,
        iprscan_db_conf.user,
        iprscan_db_conf.password,
        iprscan_db_conf.engine
    )

    def fastaPath = task.workDir.resolve("${job.upiFrom}_${job.upiTo}.faa")
    def seqCount = db.writeFasta(job.upiFrom, job.upiTo, fastaPath.toString())
    job.fasta = fastaPath.toString()

    db.close()
}
