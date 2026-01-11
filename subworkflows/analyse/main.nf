include { INIT_PIPELINE                                                      } from "./init"
include { GET_ANALYSES; BUILD_JOBS;                                          } from "../../modules/prepare"
include { EXPORT_FASTA as EXPORT_FASTA_CPU; EXPORT_FASTA as EXPORT_FASTA_GPU } from "../../modules/prepare"
include { RUN_INTERPROSCAN_CPU; RUN_INTERPROSCAN_GPU                         } from "../../modules/interproscan"
include { PERSIST_MATCHES                                                    } from "../../modules/persist/matches"
include { LOG_JOBS                                                           } from "../../modules/persist/jobs"
include { CLEAN_FASTAS                                                       } from "../../modules/clean"

workflow ANALYSE {
    take:
    database_params
    interproscan_params
    applications_params
    batch_size
    max_jobs_per_analysis
    list
    analysis_ids

    main:
    INIT_PIPELINE(
        database_params,
        interproscan_params
    )
    cpu_iprscan    = INIT_PIPELINE.out.cpuIprscan.val     // Iprscan instance for CPU execution
    gpu_iprscan    = INIT_PIPELINE.out.gpuIprscan.val     // Iprscan instance for GPU execution
    db_config      = INIT_PIPELINE.out.dbConfig.val       // map of interpro oracle/postrgresql db info (user, pwd, etc.)

    analyses = GET_ANALYSES(
        db_config["intprscan"],
        analysis_ids
    )

    if (list) {
        analyses
            .flatMap()
            .view { job -> "${job.analysisId.toString().padRight(6)}\t${job.application.name.padRight(20)}\t$job.application.version" }
    } else {
        all_jobs = BUILD_JOBS(
            db_config["intprscan"],
            applications_params,
            analyses,
            cpu_iprscan,
            gpu_iprscan,
            batch_size,
            max_jobs_per_analysis
        )

        // Index the jobs so we can identify successfully and failed jobs -> [[index, job, gpu[bool]], [index, job, gpu[bool]]]
        ch_cpu_jobs = all_jobs[0]
            .map { cpu_jobs -> cpu_jobs.indexed() }
            .flatMap()
            .map { entry -> [entry.key, entry.value, false] }

        ch_gpu_jobs = all_jobs[1]
            .map { gpu_jobs -> gpu_jobs.indexed() }
            .flatMap()
            .map { entry -> [entry.key, entry.value, true] }

        ch_cpu_jobs = EXPORT_FASTA_CPU(db_config["intprscan"], ch_cpu_jobs)
        ch_gpu_jobs = EXPORT_FASTA_GPU(db_config["intprscan"], ch_gpu_jobs)

        // TODO: skip jobs that have already been run successfully
        // TODO: skip jobs that have zero sequences, and persist the jobs as successful with zero matches

        /*
        If RUN_INTERPROSCAN is succesful, persist the matches and update the ANALYSIS_JOBS table.
        If PERSIST_MATCHES fails, mark the job as unsuccessful in the ANALYSIS_JOBS table.
        If RUN_INTERPROSCAN fails skip straight to updating the ANALYSIS_JOBS table.
        */
        iprscan_cpu_out = RUN_INTERPROSCAN_CPU(ch_cpu_jobs)

        iprscan_gpu_out = RUN_INTERPROSCAN_GPU(ch_gpu_jobs)

        ch_iprscan_results = iprscan_cpu_out
            .mix(iprscan_gpu_out)

        successful_jobs = PERSIST_MATCHES(ch_iprscan_results, db_config["intprscan"])
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
            db_config["intprscan"]
        )

        CLEAN_FASTAS(LOG_JOBS.out)
    }
}
