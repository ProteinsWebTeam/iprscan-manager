include { INIT_PIPELINE                              } from "./init"
include { GET_ANALYSES; GET_SEQUENCES                } from "../../modules/prepare"
include { RUN_INTERPROSCAN_CPU; RUN_INTERPROSCAN_GPU } from "../../modules/interproscan"
include { PERSIST_MATCHES                            } from "../../modules/persist/matches"
include { LOG_JOB                                    } from "../../modules/persist/jobs"

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

    analyses       = GET_ANALYSES(db_config["intprscan-intprscan"])
    all_jobs       = GET_SEQUENCES(db_config["intprscan-uniparc"], analyses)
    // Index the jobs and retrieve all the indexes
    // so we can separate successfully from failed jobs -> [[index, job], [index, job]]
    cpu_jobs    = all_jobs[0]
    ch_cpu_jobs = cpu_jobs
        .map { cpu_jobs -> cpu_jobs.indexed() }
        .flatMap()
        .map { entry -> [entry.key, entry.value, "cpu"] }
    all_cpu_job_ids = ch_cpu_jobs.map { it[0] }.collect()  // e.g. [0, 1, 2]

    gpu_jobs    = all_jobs[1]
    ch_gpu_jobs = gpu_jobs
        .map { gpu_jobs -> gpu_jobs.indexed() }
        .flatMap()
        .map { entry -> [entry.key, entry.value, "gpu"] }
    all_gpu_job_ids = ch_gpu_jobs.map { it[0] }.collect()  // e.g. [0, 1]

    iprscan_cpu_out = RUN_INTERPROSCAN_CPU(
        ch_cpu_jobs,
        iprscan_exe,
        profile,
        work_dir,
        interproscan_params.maxWorkers,
        iprscan_config
    )

    iprscan_gpu_out = RUN_INTERPROSCAN_GPU(
        ch_gpu_jobs,
        iprscan_exe,
        profile,
        work_dir,
        interproscan_params.maxWorkers,
        iprscan_config
    )

    /*
    If RUN_INTERPROSCAN is succesful, persist the matches and update the ANALYSIS_JOBS table.
    If PERSIST_MATCHES fails, mark the job as unsuccessful in the ANALYSIS_JOBS table.
    If RUN_INTERPROSCAN fails skip straight to updating the ANALYSIS_JOBS table.
    */

    ch_iprscan_results = iprscan_cpu_out.merge(iprscan_gpu_out)
    ch_iprscan_results.view { "ch_iprscan_results: ${it}" }

    persisted_results = PERSIST_MATCHES(iprscan_cpu_out, db_config["intprscan-intprscan"])

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

    // update_only.// { "update_only: $it" }
    // update_failure.view { "update_failure: $it" }
    // update_success.view { "update_success: $it" }
    // update_jobs_failed.view { "update_jobs_failed: $it" }
    // update_jobs_success.view { "update_jobs_success: $it" }
    // all_updates.view()
    
    // LOG_JOB(all_updates, db_config["intprscan-intprscan"], interproscan_params.sbatch)
}
