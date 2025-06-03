include { ANALYSE } from "./subworkflows/analyse"
include { CLEAN   } from "./subworkflows/clean"
include { IMPORT  } from "./subworkflows/import"

workflow {
    println "# ${workflow.manifest.name} ${workflow.manifest.version}"
    println "# ${workflow.manifest.description}\n"

    def method = params.method.toLowerCase()

    if (!method) {
        log.error "Please specify a method (--method): 'analyse', 'clean' or 'import'"
        exit 1
    } else if (method == "analyse") {
        ANALYSE(
            params.databases,
            params.interproscan
        )
    } else if (method == "clean") {
        CLEAN()
    } else if (method == "import") {
        IMPORT(
            params.databases
        )
    } else {
        log.error "Unrecognised method: '$method'. Expected 'analyse', 'clean', or 'import'"
        exit 1
    }
}

workflow.onComplete = {
    if (workflow.success) {
        println "IPM method ${params.method} completed successfully."
        println "Duration: ${workflow.duration}"
    }
}
