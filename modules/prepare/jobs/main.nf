import java.security.MessageDigest
import java.util.Random


process GET_ANALYSES {
    // Identify and group analyses to be run
    // Analyses with the same maxUpi, datadir and interpro version can run in the same iprscan job
    input:
    val iprscan_conf

    output:
    val analyses

    exec:
    def uri = iprscan_conf.uri
    def user = iprscan_conf.user
    def pswd = iprscan_conf.password
    Database db = new Database(uri, user, pswd)

    Random random = new Random()

    analyses = [:]
    def analysis_rows = db.getAnalyses()
    for (row: analysis_rows) {
        (maxUpi, dataDir, interproVersion, dbName, matchTable, siteTable, analysisId, dbVersion) = row
        key = [maxUpi, dataDir, interproVersion]
        analyses[key] = analyses.get(key, new IprscanJob(maxUpi, dataDir, interproVersion.toString()))
        randomisedAnalysisId = analysisId.toInteger() + random.nextInt(100)  // just for dev, so I don't need to worry about unique constraints being violated
        analyses[key].addApplication(dbName, randomisedAnalysisId, dbVersion, matchTable, siteTable)
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
    def uri = iprscan_conf.uri
    def user = iprscan_conf.user
    def pswd = iprscan_conf.password
    Database db = new Database(uri, user, pswd)

    // for each UPI range (from - to) build a FASTA file of the protein sequences
    def upiTo = db.getMaxUPI()
    def maxUPIs = analyses.keySet().collect { it[0] }.toSet()
    def fastaFiles = [:]
    maxUPIs.each { String upiFrom ->
        fasta = task.workDir.resolve("${upiFrom}.faa")
        db.writeFasta(upiFrom, upiTo, fasta.toString())
        fastaFiles[upiFrom] = fasta
    }

    // assign the fasta file to each IproscanJob
    analyses.each { key, value ->
        value.setFasta(fastaFiles[key[0]].toString())
    }

    // Only return the IprscanJobs
    jobs = analyses.values()
}
