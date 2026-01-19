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

    output:
    val analyses

    exec:
    Database db = new Database(
        iprscan_db_conf.uri,
        iprscan_db_conf.user,
        iprscan_db_conf.password,
        iprscan_db_conf.engine
    )

    // Group jobs by UPI so that we only need to write one FASTA for each unique max UPI
    analyses = [:].withDefault { [] } // upiFrom-upiTo: [jobs]

    // Get the new analyses from the iprscan.analysis table
    def analysis_rows = db.getAnalyses()
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
    }

    // Get analyses/jobs that failed to run previously so that they can be re-run/resubmit
    def job_rows = db.getFailedJobs()
    resubmission = true
    for (row: job_rows) {
        (analysisId, upiFrom, upiTo, seqCount, dataDir, interproVersion, dbName, matchTable, siteTable, dbVersin, gpu) = row
        application = new Application(dbName, dbVersion, matchTable, siteTable)
        job = new Job(
            analysisId.toInteger(),
            resubmision,
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

    // for each UPI range (from - to) identify the upi ranges for the batches
    def maxUpiTo = db.getMaxUPI()
    def batches = [:].withDefault { [] } // [[upiFrom: str, upiTo: str, seqCount: int]]
    analyses.each { key, analysis ->
        upiTo = analysis.upiTo ?: maxUpiTo
        batches[key].addAll( db.defineBatches(analysis.upiFrom, upiTo, task.workDir, batch_size) )
    }

    // Create a job for each batch per analysis
    cpuJobs = []
    gpuJobs = []
    jobRecords = []
    analysisRecords = []
    batches.each { String key, List<Map> batchMaps ->
        analyses[key].each { Job job ->
            batchMaps.each { Map batch ->

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

                (job.gpu ? gpuJobs : cpuJobs) << batchJob

                // create a new record for the job in the interproscan.iprscan.analysis_jobs table
                jobRecords.add(
                    [
                        batchJob.analysisId,
                        batchJob.upiFrom,
                        batchJob.upiTo,
                        java.sql.Timestamp.valueOf(batchJob.createdTime),
                        batchJob.seqCount
                    ]
                )

                // update the maxupi for analyses in the interproscan.iprscan.analyis table
                // so that future jobs automatically pick up from where this analysis ended
                upiTo_updated_hash = int_to_upi(upi_to_int(batchJob.upiTo) + 1)
                if (!job.resubmision) {
                    analysisRecords.add(
                        [
                            upiTo_updated_hash,
                            batchJob.analysisId
                        ]
                    )
                }
            }
        }
    }

    db.insertJobs(jobRecords)
    db.updateAnalyses(analysisRecords)
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
