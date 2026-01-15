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

    // [3] Validate database configuration
    (dbConfig, error) = ProductionManager.validateDbConfig(database_params, ["intprscan"])
    if (error) {
        log.error error
        exit 1
    }

    // [4] Validate the iprscan config file is one is specified
    (cpuIprscanConfig, error) = ProductionManager.validateLicenseConfig(interproscan_params.cpu.licensedConfig)
    if (error) {
        log.error error
        exit 1
    }
    (gpuIprscanConfig, error) = ProductionManager.validateLicenseConfig(interproscan_params.gpu.licensedConfig)
    if (error) {
        log.error error
        exit 1
    }

    cpuIprscan = new Iprscan(
        cpuExecutable,
        cpuProfile,
        interproscan_params.cpu.maxWorkers,
        cpuIprscanConfig,
        false
    )
    gpuIprscan = new Iprscan(
        gpuExecutable,
        gpuProfile,
        interproscan_params.cpu.maxWorkers,
        gpuIprscanConfig,
        true
    )

    emit:
    cpuIprscan
    gpuIprscan
    dbConfig
}
