process IMPORT_SEQUENCES {
    input:
    val iprscan_conf
    val uniparc_conf
    val top_up
    val max_upi

    exec:
    Database iprscan_db = new Database(
        iprscan_conf.uri,
        iprscan_conf.user,
        iprscan_conf.password,
        iprscan_conf.engine
    )
    Database uniparc_db = new Database(
        uniparc_conf.uri,
        uniparc_conf.user,
        uniparc_conf.password,
        uniparc_conf.engine
    )

    def currentMaxUpi = null
    if (top_up) {
        currentMaxUpi = iprscan_db.getMaxUPI()
    } else {
        iprscan_db.dropProteinTable()
        iprscan_db.buildProteinTable()
    }

    println currentMaxUpi != null ? "Highest UPI: ${currentMaxUpi}" : 'N/A - top-up not used so wiped the protein table to import all sequences'
    def protCount = 0
    def records = []

    uniparc_db.iterProteins(currentMaxUpi, max_upi) { rec ->
        def (upi, timestamp, seq_short, seq_long, len, crc64, md5) = rec
        records << [upi, timestamp, seq_short ?: clobToString(seq_long), len, crc64, md5]
        protCount += 1

        if (records.size() == iprscan_db.INSERT_SIZE) {
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
    println "The new highest UPI: ${currentMaxUpi}"
    println "${protCount} sequences imported"

    iprscan_db.close()
    uniparc_db.close()
}

def clobToString(clob) {
    clob?.getCharacterStream()?.text
}
