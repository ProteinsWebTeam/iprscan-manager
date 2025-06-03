include { INIT_PIPELINE                        } from "./init"
include { GET_ANALYSES; GET_SEQUENCES          } from "../../modules/prepare/jobs"
include { RUN_INTERPROSCAN                     } from "../../modules/interproscan"
include { SEPARATE_MEMBER_DBS; REBUILD_INDEXES } from "../../modules/prepare/matches"
include { PERSIST_MATCHES                      } from "../../modules/persist_matches"

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

    analyses    = GET_ANALYSES(db_config.iprscan)
    sequences   = GET_SEQUENCES(db_config.uniprot, analyses)
    jobs = sequences.flatten()  // gather the groovy objects into a channel

    RUN_INTERPROSCAN(
        jobs,
        iprscan_exe,
        profile,
        work_dir,
        interproscan_params.runtime.maxWorkers,
        interproscan_params.sbatch
    )
    matches           = RUN_INTERPROSCAN.out
    separated_matches = SEPARATE_MEMBER_DBS(matches)
    prepared_matches  = REBUILD_INDEXES(separated_matches, db_config.iprscan)
    PERSIST_MATCHES(separated_matches, db_config.iprscan)
}
