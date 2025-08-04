include { INIT_PIPELINE               } from "./init"
include { GET_ANALYSES; GET_SEQUENCES } from "../../modules/prepare"
include { RUN_INTERPROSCAN_CPU        } from "../../modules/interproscan"
include { REBUILD_INDEXES             } from "../../modules/clean"
include { PERSIST_MATCHES             } from "../../modules/persist/matches"
include { LOG_JOB                     } from "../../modules/persist/jobs"

workflow ANALYSE {
    take:
    database_params
    interproscan_params

    main:
    INIT_PIPELINE(
        database_params,
        interproscan_params
    )
    iprscan_exe    = INIT_PIPELINE.out.iprscan.val
    profile        = INIT_PIPELINE.out.profile.val
    work_dir       = INIT_PIPELINE.out.workDir.val
    db_config      = INIT_PIPELINE.out.dbConfig.val
    iprscan_config = INIT_PIPELINE.out.iprscanConfig.val

    analyses    = GET_ANALYSES(db_config["intprscan-intprscan"])
    sequences   = GET_SEQUENCES(db_config["intprscan-uniparc"], analyses)
    jobs = sequences.flatten()  // gather the groovy objects into a channel

    RUN_INTERPROSCAN_CPU(
        jobs,
        iprscan_exe,
        profile,
        work_dir,
        interproscan_params.maxWorkers,
        iprscan_config
    )
    interproscan_out = RUN_INTERPROSCAN_CPU.out

    // /*
    // If RUN_INTERPROSCAN is succesful, persist the matches,
    // then update the ANALYSIS_JOBS table.
    // If PERSIST_MATCHES fails, mark the job as unsuccessful in
    // the ANALYSIS_JOBS table.
    // If RUN_INTERPROSCAN fails skip straight to updating the
    // ANALYSIS_JOBS table.
    // */

    // interproscan_out
    //     .branch {
    //         success: it[1] != "failed.json"
    //         failed: it[1] == "failed.json"
    //     }
    //     .set { run_status }

    // // iprscan run failed
    // run_status.failed.map { it[0] }.set { update_only }
    // run_status.success.view { "run_status - success: $it" }
    // run_status.failed.view { "run_status - failed: $it" }

    // // iprscan ran successfully
    // matches        = REBUILD_INDEXES(run_status.success, db_config["intprscan-intprscan"])
    // persist_result = PERSIST_MATCHES(matches, db_config["intprscan-intprscan"])

    // // mark if persisting the matches was successful
    // persist_result
    //     .branch {
    //         persist_success: it[1] == true
    //         persist_failed: it[1] == false
    //     }
    //     .set { persist_status }
    // persist_status.persist_success.map { it[0] }.set { update_success }
    // persist_status.persist_failed.map { it[0] }.set { update_failure }

    // // identify jobs that ran successfully all the way through
    // // Identify jobs that ran successfully all the way through
    // update_success
    //     .map { job -> [job, true] }
    //     .set { update_jobs_success }

    // // Only create update_jobs_failed if there are any failed jobs
    // update_only
    //     .mix(update_failure)
    //     .map { job -> [job, false] }
    //     .set { update_jobs_failed }

    // // Merge both success and failure updates
    // update_jobs_failed
    //     .mix(update_jobs_success)
    //     .set { all_updates }

    // update_only.view { "update_only: $it" }
    // update_failure.view { "update_failure: $it" }
    // update_success.view { "update_success: $it" }
    // update_jobs_failed.view { "update_jobs_failed: $it" }
    // update_jobs_success.view { "update_jobs_success: $it" }
    // all_updates.view()
    
    // LOG_JOB(all_updates, db_config["intprscan-intprscan"], interproscan_params.sbatch)
}
