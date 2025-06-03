include { INIT_PIPELINE                        } from "./init"

workflow ANALYSE {
    take:
    database_params
    interproscan_params

    main:
    INIT_PIPELINE(
        database_params,
        interproscan_params
    )
    iprscan_exe = INIT_PIPELINE.out.iprscan.val
    profile     = INIT_PIPELINE.out.profile.val
    work_dir    = INIT_PIPELINE.out.workDir.val
    db_config   = INIT_PIPELINE.out.dbConfig.val
}