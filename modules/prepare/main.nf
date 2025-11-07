import uk.ac.ebi.interpro.Application
import uk.ac.ebi.interpro.Database
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
    analyses = [:].withDefault { [] }  // UPI: [Jobs]
    def analysis_rows = db.getAnalyses()
    for (row: analysis_rows) {
        (maxUpi, dataDir, interproVersion, dbName, matchTable, siteTable, analysisId, dbVersion, gpu) = row
        application = new Application(dbName, dbVersion, matchTable, siteTable)
        job = new Job(analysisId.toInteger(), maxUpi, dataDir, interproVersion, gpu, application)
        job.compileJobName()
        analyses[maxUpi] << job
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
        simpleAppsConfig.applications[key] = value
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

    // for each UPI range (from - to) build FASTA files of the protein sequences of a maxium batch_size
    def upiTo = db.getMaxUPI()
    def maxUPIs = analyses.keySet()
    def fastaFiles = [:].withDefault { [] }
    maxUPIs.each { String upiFrom ->
        def allUpis = db.getUpiRange(upiFrom, upiTo)
        def batched_upis = allUpis.collate(batch_size)
        for (batch in batched_upis) {
            def batchUpiFrom = batch[0]
            def batchUpiTo = batch[-1]
            def fasta = task.workDir.resolve("upiFrom_${batchUpiFrom}_upiTo_${batchUpiTo}.faa")
            def seqCount = db.writeFasta(batchUpiFrom, batchUpiTo, fasta.toString())
            fastaFiles[upiFrom] << ['path': fasta, 'count': seqCount, 'upiFrom': batchUpiFrom, 'upiTo': batchUpiTo]
        }
    }
    db.close()

    // Create a job for each batch per analysis, and assign the batches fasta file to the job
    cpuJobs = []
    gpuJobs = []
    fastaFiles.each { upiFrom, fastaFilesList ->
        analyses[upiFrom].each { job ->
            fastaFilesList.each { fasta ->
                def resources = simpleAppsConfig.resources.get(job.application.name.toLowerCase(), "light")
                def useGpu = job.gpu
                def iprscanSource = useGpu ? gpu_iprscan : cpu_iprscan

                def iprscanConfig = new Iprscan(
                    iprscanSource.executable,
                    iprscanSource.profile,
                    iprscanSource.workDir,
                    iprscanSource.maxWorkers,
                    iprscanSource.configFile,
                    resources,
                    useGpu
                )

                def batchJob = new Job(
                    job.analysisId, fasta['upiFrom'],
                    job.dataDir, job.interproVersion,
                    job.gpu, job.application,
                    iprscanConfig,
                    fasta['path'].toString(), fasta['count'],
                    fasta['upiFrom'], fasta['upiTo']
                )

                (useGpu ? gpuJobs : cpuJobs) << batchJob
            }
        }
    }
}
