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
        job = new Job(analysisId.toInteger(), maxUpi, dataDir, interproVersion, gpu)
        job.application = new Application(dbName, dbVersion, matchTable, siteTable)
        job.compileJobName()
        analyses[maxUpi] << job
    }
    db.close()
}

process GET_JOBS {
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
    Map<String, String> appResources = [:]
    analyses.each { String maxUpi, Job job ->
        resources = apps_conf.get(job.application.name.toLowerCase(), "light")
        appResources[job.application.name] = resources
    } 

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
        allUpis = db.getUpiRange(upiFrom, upiTo)
        batched_upis = allUpis.collate(batch_size)
        for (batch in batched_upis) {
            batchUpiFrom = batch[0]
            batchUpiTo = batch[-1]
            fasta = task.workDir.resolve("upiFrom_${batchUpiFrom}_upiTo_${batchUpiTo}.faa")
            seqCount = db.writeFasta(batchUpiFrom, batchUpiTo, fasta.toString())
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
                resources = appResources[job.application.name]
                useGpu = job.gpu
                iprscanSource = useGpu ? gpu_iprscan : cpu_iprscan

                iprscanConfig = new Iprscan(
                    iprscanSource.executable,
                    iprscanSource.workDir,
                    iprscanSource.maxWorkers,
                    iprscanSource.configFile,
                    resources,
                    useGpu
                )

                batchJob = new Job(
                    job.analysisId, fasta['upiFrom'],
                    job.dataDir, job.interproVersion,
                    job.gpu, job.application,
                    iprscanConfig,
                    fasta['path'].toString(), fasta['count'],
                    fasta['upiFrom'], fasta['upiTo']
                )

                (useGpu ? gpuJobs : cpuJobs) << batchJob
                println "batchJob: ${batchJob.analysisId} - ${batchJob.application.name} -- ${batchJob.iprscan.resources}"
            }
        }
    }
}
