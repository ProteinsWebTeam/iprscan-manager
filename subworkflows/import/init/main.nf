workflow INIT_PIPELINE {
    take:
    database_params
    max_upi

    main:
    // validate the database configurations
    (dbConfig, error) = IPM.valdidateDbConfig(database_params, ["iprscanUniParc", "uniparc"])
    if (error) {
        log.error error
        exit 1
    }

    if (!max_upi) {
        log.warn "IPM will wipe the IprScan database UNIPARC.PROTEIN table and import all UniParc seqs anew"
    }

    emit:
    dbConfig
}
