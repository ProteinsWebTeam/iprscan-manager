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
    // so we can separate successfully from failed jobs -> [[index, job, gpu[bool]], [index, job, gpu[bool]]]
    cpu_jobs    = all_jobs[0]
    ch_cpu_jobs = cpu_jobs
        .map { cpu_jobs -> cpu_jobs.indexed() }
        .flatMap()
        .map { entry -> [entry.key, entry.value, false] }
    all_cpu_job_ids = ch_cpu_jobs.map { it[0] }.collect()  // e.g. [0, 1, 2]

    gpu_jobs    = all_jobs[1]
    ch_gpu_jobs = gpu_jobs
        .map { gpu_jobs -> gpu_jobs.indexed() }
        .flatMap()
        .map { entry -> [entry.key, entry.value, true] }
    all_gpu_job_ids = ch_gpu_jobs.map { it[0] }.collect()  // e.g. [0, 1]

    /*
    If RUN_INTERPROSCAN is succesful, persist the matches and update the ANALYSIS_JOBS table.
    If PERSIST_MATCHES fails, mark the job as unsuccessful in the ANALYSIS_JOBS table.
    If RUN_INTERPROSCAN fails skip straight to updating the ANALYSIS_JOBS table.
    */

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

    ch_iprscan_results = iprscan_cpu_out
        .mix(iprscan_gpu_out)
    ch_iprscan_results.view { "ch_iprscan_results: ${it}" }

    successful_jobs = PERSIST_MATCHES(ch_iprscan_results, db_config["intprscan-intprscan"])
        .collect()
        .flatten()
    successful_jobs.view { "successful_jobs: ${successful_jobs} "}
    // LOG_JOB(successful_jobs, all_cpu_job_ids, all_gpu_job_ids, db_config["intprscan-intprscan"])
}
