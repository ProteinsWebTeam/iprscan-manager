uk.ac.ebi.interpro.ProductionManager
uk.ac.ebi.interpro.Iprscan

workflow INIT_PIPELINE {
    take:
    database_params
    interproscan_params

    main:
    // [1] Validate the iprscan executable
    cpuExeutable = ProductionManager.resolveExecutable(interproscan_params.cpu.executable)
    if (error) {
        log.error error
        exit 1
    }
    gpuExecutable = ProductionManager.resolveExecutable(interproscan_params.gpu.executable)
    if (error) {
        log.error error
        exit 1
    }

    // [2] Validate the container runtime and scheduler
    (cpuProfile, gpuProfile, error) = ProductionManager.getIprscanProfiles(
        interproscan_params.cpu.executor,
        interproscan_params.gpu.executor,
        interproscan_params.container
    )
    if (error) {
        log.error error
        exit 1
    }

    // [3] Validate the work dir
    (cpuWorkDir, error) = ProductionManager.resolveDirectory(interproscan_params.cpu.workdir, true)
    if (!workDir) {
        log.error error
        exit 1
    }
    (gpuWorkDir, error) = ProductionManager.resolveDirectory(interproscan_params.gpu.workdir, true)
    if (!workDir) {
        log.error error
        exit 1
    }

    // [4] Validate database configuration
    (dbConfig, error) = ProductionManager.validateDbConfig(database_params, ["intprscan"])
    if (error) {
        log.error error
        exit 1
    }

    // [5] Validate the iprscan config file is one is specified
    (cpuIprscanConfig, error) = ProductionManager.validateConfig(interproscan_params.cpu.config)
    if (error) {
        log.error error
        exit 1
    }
    (gpuIprscanConfig, error) = ProductionManager.validateConfig(interproscan_params.gpu.config)
    if (error) {
        log.error error
        exit 1
    }

    cpuIprscan = Iprscan(
        cpuExeutable,
        cpuWorkDir,
        interproscan_params.cpu.maxWorkers,
        cpuIprscanConfig,
        False
    )
    gpuIprscan = Iprscan(
        gpuExeutable,
        gpuWorkDir,
        interproscan_params.cpu.maxWorkers,
        gpuIprscanConfig,
        True
    )

    emit:
    cpuIprscan
    gpuIprscan
    dbConfig
    iprscanConfig
}
