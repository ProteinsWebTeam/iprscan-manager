import uk.ac.ebi.interpro.ProductionManager
import uk.ac.ebi.interpro.Iprscan

workflow INIT_PIPELINE {
    take:
    database_params
    interproscan_params

    main:
    // [1] Validate the iprscan executable
    (cpuExecutable, error) = ProductionManager.resolveExecutable(interproscan_params.cpu.executable)
    if (error) {
        log.error error
        exit 5
    }
    (gpuExecutable, error) = ProductionManager.resolveExecutable(interproscan_params.gpu.executable)
    if (error) {
        log.error error
        exit 1
    }

    // [2] Validate the container runtime, profiles and scheduler
    (cpuProfile, error) = ProductionManager.validateIprscanProfiles(interproscan_params.cpu, "CPU")
    if (error) {
        log.error error
        exit 1
    }
    (gpuProfile, error) = ProductionManager.validateIprscanProfiles(interproscan_params.gpu, "GPU")
    if (error) {
        log.error error
        exit 1
    }

    // [3] Validate the work dir
    (cpuWorkDir, error) = ProductionManager.resolveDirectory(interproscan_params.cpu.workdir, true)
    if (error) {
        log.error error
        exit 1
    }
    (gpuWorkDir, error) = ProductionManager.resolveDirectory(interproscan_params.gpu.workdir, true)
    if (error) {
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

    cpuIprscan = new Iprscan(
        cpuExecutable,
        cpuProfile,
        cpuWorkDir,
        interproscan_params.cpu.maxWorkers,
        cpuIprscanConfig,
        false
    )
    gpuIprscan = new Iprscan(
        gpuExecutable,
        gpuProfile,
        gpuWorkDir,
        interproscan_params.cpu.maxWorkers,
        gpuIprscanConfig,
        true
    )

    emit:
    cpuIprscan
    gpuIprscan
    dbConfig
}
