import java.security.MessageDigest


process GET_ANALYSES {
    // Identify build a job for each analysis
    input:
    val iprscan_conf
    val job_conf

    output:
    val analyses

    exec:
    Database db = new Database(
        iprscan_conf.uri,
        iprscan_conf.user,
        iprscan_conf.password,
        iprscan_conf.engine
    )

    // Group jobs by UPI so that we only need to write one FASTA for each unique max UPI
    analyses = [:]  // UPI: [Jobs]
    def analysis_rows = db.getAnalyses()
    for (row: analysis_rows) {
        (maxUpi, dataDir, interproVersion, dbName, matchTable, siteTable, analysisId, dbVersion) = row
        job = new IprscanJob(analysisId.toInteger(), maxUpi, dataDir, interproVersion)
        job.application = new Application(dbName, dbVersion, matchTable, siteTable)
        job.compileJobName(job_conf.jobPrefix)

        if (analyses.containsKey(maxUpi)) {
            analyses[maxUpi] << job
        } else {
            analyses[maxUpi] = [job]
        }
    }
    db.close()
}


process GET_SEQUENCES {
    // Get sequences to analyse. Return a list of IprscanJobs
    input:
    val iprscan_conf
    val analyses

    output:
    val jobs

    exec:
    Database db = new Database(
        iprscan_conf.uri,
        iprscan_conf.user,
        iprscan_conf.password,
        iprscan_conf.engine
    )

    // for each UPI range (from - to) build a FASTA file of the protein sequences
    def upiTo = db.getMaxUPI()
    def maxUPIs = analyses.keySet()
    def fastaFiles = [:]  // upi: fasta
    maxUPIs.each { String upiFrom ->
        fasta = task.workDir.resolve("${upiFrom}.faa")
        seqCount = db.writeFasta(upiFrom, upiTo, fasta.toString())
        fastaFiles[upiFrom] = ['fasta': fasta, 'count': seqCount, 'upiFrom': upiFrom, 'upiTo': upiTo]
    }

    // assign the fasta file to each IproscanJob
    jobs = [] // Only return the IprscanJobs
    fastaFiles.each { upi, data ->
        analyses[upi].each { job ->
            job.setSeqData(data['fasta'].toString(), data['count'], data['upiFrom'], data['upiTo'])
            jobs << job
        }
    }
}
