workflow INIT_PIPELINE {
    take:
    database_params
    interproscan_params

    main:
    // [1] Validate the iprscan executable
    if (!interproscan_params.runtime.executable) {
        log.error "Please specify a IPS6 executable - e.g. 'ebi-pf-team/interproscan6' or a path to the ips6 main.nf file"
        exit 1
    }
    iprscan = IPM.resolveExecutable(interproscan_params.runtime.executable)
    if (!iprscan) {
        log.error "Cannot find iprscan executable at ${interproscan_params.runtime.executable}"
        exit 1
    }

    // [2] Validate the container runtime and scheduler
    (profile, error) = IPM.getIprscanProfiles(
        interproscan_params.runtime.executor,
        interproscan_params.runtime.container
    )
    if (error) {
        log.error error
        exit 1
    }

    // [3] Validate the work dir
    (workDir, error) = IPM.resolveDirectory(interproscan_params.runtime.workdir, true)
    if (!workDir) {
        log.error error
        exit 1
    }

    // [4] Validate database configuration
    (dbConfig, error) = IPM.valdidateDbConfig(database_params, [["intprscan", "intprscan"], ["intprscan", "uniparc"]])
    if (error) {
        log.error error
        exit 1
    }

    // [5] Validate the iprscan config file is one is specified
    (iprscanConfig, error) = IPM.validateConfig(interproscan_params.runtime.config)
    if (error) {
        log.error error
        exit 1
    }

    emit:
    iprscan
    profile
    workDir
    dbConfig
    iprscanConfig
}
