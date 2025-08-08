include { INIT_PIPELINE                              } from "./init"
include { GET_ANALYSES; GET_SEQUENCES                } from "../../modules/prepare"
include { RUN_INTERPROSCAN_CPU; RUN_INTERPROSCAN_GPU } from "../../modules/interproscan"
include { PERSIST_MATCHES                            } from "../../modules/persist/matches"
include { LOG_JOBS                                   } from "../../modules/persist/jobs"

workflow ANALYSE {
    take:
    database_params
    interproscan_params

    main:
    INIT_PIPELINE(
        database_params,
        interproscan_params
    )
    iprscan_exe    = INIT_PIPELINE.out.iprscan.val        // path to iprscan executable
    profile        = INIT_PIPELINE.out.profile.val        // iprscan container and runtime profiles
    work_dir       = INIT_PIPELINE.out.workDir.val        // path to write out the iprscan workdir
    db_config      = INIT_PIPELINE.out.dbConfig.val       // map of interpro oracle/postrgresql db info (user, pwd, etc.)
    iprscan_config = INIT_PIPELINE.out.iprscanConfig.val  // iprscan config file, if one is provided, e.g. to run with a gpu

    analyses       = GET_ANALYSES(db_config["intprscan-intprscan"])
    all_jobs       = GET_SEQUENCES(db_config["intprscan-uniparc"], analyses)

    // Index the jobs so we can identify successfully and failed jobs -> [[index, job, gpu[bool]], [index, job, gpu[bool]]]
    cpu_jobs    = all_jobs[0]
    ch_cpu_jobs = cpu_jobs
        .map { cpu_jobs -> cpu_jobs.indexed() }
        .flatMap()
        .map { entry -> [entry.key, entry.value, false] }

    gpu_jobs    = all_jobs[1]
    ch_gpu_jobs = gpu_jobs
        .map { gpu_jobs -> gpu_jobs.indexed() }
        .flatMap()
        .map { entry -> [entry.key, entry.value, true] }

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

    successful_jobs = PERSIST_MATCHES(ch_iprscan_results, db_config["intprscan-intprscan"])
        .map { t -> [t] }  // Wrap each emitted tuple in its own list
        .collect()
        .ifEmpty { [] }    // Emit an empty list if no jobs succeeded


    // Wrap each emit tuple in its own list
    successful_iprscan_jobs = ch_iprscan_results
        .map { t -> [t] }
        .collect()
        .ifEmpty { [] }  // Emit an empty list if no jobs succeeded

    all_cpu_jobs = ch_cpu_jobs
        .map { t -> [t] }
        .collect()
        .ifEmpty { [] }  // Emit an empty list if no jobs succeeded
    all_gpu_jobs = ch_gpu_jobs
        .map { t -> [t] }
        .collect()
        .ifEmpty { [] }  // Emit an empty list if no jobs succeeded

    // Log the job success/failures in the postgresql interproscan db
    LOG_JOBS(
        successful_jobs,
        successful_iprscan_jobs,
        all_cpu_jobs,
        all_gpu_jobs,
        db_config["intprscan-intprscan"]
    )
}
