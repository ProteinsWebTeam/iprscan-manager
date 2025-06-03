include { INIT_PIPELINE } from "./init"

workflow CLEAN {
    take:
    database_params
    analyses

    main:
    INIT_PIPELINE(
        database_params,
        analyses
    )
    db_config = INIT_PIPELINE.out.dbConfig.val
    println "Workflow placeholder -- $db_config"
}