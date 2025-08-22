workflow INIT_PIPELINE {
    take:
    database_params
    analyses

    main:
    (dbConfig, error) = IPM.validateDbConfig(database_params, ["intprscan"])
    if (error) {
        log.error error
        exit 1
    }

    analysisIds = analyses
    if (!analyses) {
        log.warn "IPM will clean obsolete data for ALL analyses listed in the IprScan database."
        analysisIds = []
    }


    emit:
    dbConfig
    analysisIds
}
