workflow INIT_PIPELINE {
    take:
    database_params
    analyses

    main:
    (dbConfig, error) = IPM.valdidateDbConfig(database_params, ["iprscan"])
    if (error) {
        log.error error
        exit 1
    }

    if (!analyses) {
        log.warn "IPM will clean obsolete data for ALL analyses listed in the IprScan database."
    }

    emit:
    dbConfig
}