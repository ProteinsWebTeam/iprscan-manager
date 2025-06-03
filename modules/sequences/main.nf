process IMPORT_SEQUENCES {
    input:
    val iprscan_conf
    val uniparc_conf
    val top_up

    exec:
    Database iprscan_db = new Database(iprscan_conf.uri, iprscan_conf.user, iprscan_conf.password)
    Database uniparc_db = new Database(uniparc_conf.uri, uniparc_conf.user, uniparc_conf.password)

    def currentMaxUpi = null
    if (top_up) {
        currentMaxUpi = iprscan_db.getMaxUPI()
    } else {
        iprscan_db.wipeProteinTable()
    }

    println "currentMaxUpi -- ${currentMaxUpi}"
}
