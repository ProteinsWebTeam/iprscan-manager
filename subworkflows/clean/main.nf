include { INIT_PIPELINE       } from "./init"
include { CLEAN_OBSOLETE_DATA } from "../../modules/clean"

workflow CLEAN {
    take:
    database_params
    analyses

    main:
    INIT_PIPELINE(
        database_params,
        analyses
    )
    db_config    = INIT_PIPELINE.out.dbConfig.val
    analysis_ids = INIT_PIPELINE.out.analysisIds.val

    CLEAN_OBSOLETE_DATA(
        db_config['intprscan'],
        analysis_ids
    )
}