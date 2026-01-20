import uk.ac.ebi.interpro.ProductionManager

include { ANALYSE } from "./subworkflows/analyse"
include { CLEAN   } from "./subworkflows/clean"
include { IMPORT  } from "./subworkflows/import"

workflow {
    println "# ${workflow.manifest.name} ${workflow.manifest.version}"
    println "# ${workflow.manifest.description}\n"

    if (params.keySet().any { it.equalsIgnoreCase("help") }) {
        ProductionManager.printHelp()
        exit 0
    }

    ProductionManager.validateParams(params, log)
    (methods, error) = ProductionManager.validateMethods(params.methods)
    if (error) {
        log.error error
        exit 1
    }

    for (method in methods) {
        if (method == "analyse") {
            ANALYSE(
                params.databases,
                params.interproscan,
                params.appsConfig,
                params.batchSize,
                params.maxJobsPerAnalysis,
                params.list,
                params.analysisIds
                params.keep
            )
        } else if (method == "clean") {
            CLEAN(
                params.databases,
                params.analyses
            )
        } else if (method == "import") {
            IMPORT(
                params.databases,
                params.topUp,
                params.maxUpi
            )
        }
    }

}
