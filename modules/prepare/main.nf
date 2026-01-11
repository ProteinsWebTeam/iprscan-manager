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

    exec:
    Database db = new Database(
        iprscan_db_conf.uri,
        iprscan_db_conf.user,
        iprscan_db_conf.password,
        iprscan_db_conf.engine
    )

    analyses = []
    def analysis_rows = db.getAnalyses()
    def selectedIds = [] as Set
    if (analysis_ids) {
        selectedIds = analysis_ids.toString()
            .split(",")
            .collect { it.trim() }
            .findAll { it }
            .collect { it.toInteger() }
            .toSet()
    }

    if (selectedIds) {
        def rowsById = analysis_rows.collectEntries { row ->
            [row[6].toInteger(), row]
        }
        def missing = selectedIds - rowsById.keySet()
        if (missing) {
            throw new IllegalArgumentException(
                "Unknown analysis IDs: ${missing.toList().sort().join(', ')}"
            )
        }
        analysis_rows = selectedIds.toList().sort().collect { rowsById[it] }
    }
    for (row: analysis_rows) {
        (maxUpi, dataDir, interproVersion, dbName, matchTable, siteTable, analysisId, dbVersion, gpu) = row
        application = new Application(dbName, dbVersion, matchTable, siteTable)
        job = new Job(analysisId.toInteger(), maxUpi, dataDir, interproVersion, gpu, application)
        analyses << job
    }
    db.close()

    analyses.sort { a, b ->
        a.application.name <=> b.application.name ?: a.analysisId <=> b.analysisId
    }
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
        subbatched: [],
        resources: [:]
    ]
    apps_config.applications.each { key, value ->
        simpleAppsConfig.applications[key.toLowerCase()] = value
    }
    apps_config.subbatched.each { app ->
        simpleAppsConfig.subbatched << app.toLowerCase()
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

    def maxUPI = db.getMaxUPI()
    db.close()

    def maxUpiInt = upi_to_int(maxUPI)
    def batchSize = batch_size as long

    cpuJobs = []
    gpuJobs = []

    analyses.each { Job analysisJob ->
        def num_jobs = 0
        def startUpi = analysisJob.maxUpi
        if (startUpi == null || startUpi.isEmpty()) {
            startUpi = "UPI0000000000"
        }
        def startInt = upi_to_int(startUpi)

        def iprscanSource = analysisJob.gpu ? gpu_iprscan : cpu_iprscan
        Iprscan iprscanConfig = new Iprscan(
            iprscanSource.executable,
            iprscanSource.profile,
            iprscanSource.workDir,
            iprscanSource.maxWorkers,
            iprscanSource.configFile,
        )

        iprscanConfig.addResources(
            simpleAppsConfig,
            analysisJob.application.name.toLowerCase().replace("-", "_"),
            analysisJob.gpu
        )

        while (startInt < maxUpiInt &&
            (max_jobs_per_analysis <= 0 || num_jobs < max_jobs_per_analysis)) {
            def endInt = (startInt + batchSize <= maxUpiInt) ? (startInt + batchSize) : maxUpiInt
            def upiFrom = int_to_upi(startInt)
            def upiTo = int_to_upi(endInt)

            def batchJob = new Job(
                analysisJob.analysisId, upiFrom,
                analysisJob.dataDir, analysisJob.interproVersion,
                analysisJob.gpu, analysisJob.application,
                iprscanConfig,
                null, null,
                upiFrom, upiTo
            )

            (analysisJob.gpu ? gpuJobs : cpuJobs) << batchJob

            startInt = endInt
            num_jobs += 1
        }
    }
}

process EXPORT_FASTA {
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
    job.seqCount = seqCount
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
