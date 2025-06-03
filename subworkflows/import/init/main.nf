workflow INIT_PIPELINE {
    take:
    database_params

    main:
    // validate the database configurations
    (dbConfig, error) = IPM.valdidateDbConfig(database_params, ["iprscan", "uniparc"])
    if (error) {
        log.error error
        exit 1
    }

    emit:
    dbConfig
}