include { INIT_PIPELINE } from "./init"

workflow IMPORT {
    take:
    database_params

    main:
    INIT_PIPELINE(
        database_params
    )
    db_config   = INIT_PIPELINE.out.dbConfig.val

    db_configc.view()
}
