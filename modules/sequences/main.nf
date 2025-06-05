process IMPORT_SEQUENCES {
    input:
    val iprscan_conf
    val uniparc_conf
    val top_up
    val max_upi

    exec:
    Database iprscan_db = new Database(iprscan_conf.uri, iprscan_conf.user, iprscan_conf.password)
    Database uniparc_db = new Database(uniparc_conf.uri, uniparc_conf.user, uniparc_conf.password)

    def currentMaxUpi = null
    if (top_up) {
        currentMaxUpi = iprscan_db.getMaxUPI()
    } else {
        iprscan_db.wipeProteinTable()
    }

    def protCount = 0
    def records = []

    // for testing
    currentMaxUpi = "UPI003C6BD65E"

    uniparc_db.iterProteins(currentMaxUpi, max_upi) { rec ->
        records << rec
        protCount += 1

        if (records.size() == 1000) {
            iprscan_db.insertProteins(records)
            records.clear()
        }
    }

    if (records) {
        iprscan_db.insertProteins(records)
        records.clear()
    }

    if (!top_up) {
        iprscan_db.configureProteinTable()
    }

    currentMaxUpi = iprscan_db.getMaxUPI()
    println "The new highest UPI: ${currentMaxUpi ?: 'N/A'}"
    println "${protCount} sequences imported"

    iprscan_db.close()
    uniparc_db.close()
}
