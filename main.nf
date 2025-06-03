include { ANALYSE } from "./subworkflows/analyse"
include { CLEAN   } from "./subworkflows/clean"
include { IMPORT  } from "./subworkflows/import"

workflow {
    println "# ${workflow.manifest.name} ${workflow.manifest.version}"
    println "# ${workflow.manifest.description}\n"

    if (params.keySet().any { it.equalsIgnoreCase("help") }) {
        IPM.printHelp()
        exit 0
    }

    IPM.validateParams(params, log)
    (methods, error) = IPM.validateMethods(params.methods)
    if (error) {
        log.error error
        exit 1
    }

    for (method in methods) {
        if (method == "analyse") {
            ANALYSE(
                params.databases,
                params.interproscan
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

workflow.onComplete = {
    if (workflow.success) {
        println "IPM methods ${params.methods} completed successfully."
        println "Duration: ${workflow.duration}"
    }
}
