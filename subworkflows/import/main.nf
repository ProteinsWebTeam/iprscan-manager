include { INIT_PIPELINE    } from "./init"
include { IMPORT_SEQUENCES } from "../../modules/sequences"

workflow IMPORT {
    take:
    database_params

    main:
    INIT_PIPELINE(
        database_params
    )
    db_config   = INIT_PIPELINE.out.dbConfig.val
    println "db_config $db_config"
}
