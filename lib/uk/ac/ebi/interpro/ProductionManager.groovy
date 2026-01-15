package uk.ac.ebi.interpro

import java.nio.file.*

class ProductionManager {
    static final def METHODS = ["analyse", "clean", "import", "index"]
    static final def CLUSTER_CONTAINERS = ["singularity"]

    static final def PARAMS = [
            [
                    name: "methods",
                    required: true,
                    metavar: "<METHODS>",
                    dscription: "Methods to be run as a comma separated list. Choose from: 'import', 'analyse', and 'clean'"
            ],
            [
                    name: "top-up",
                    description: "[IMPORT] Import new sequences only - only used by the 'import' method."
            ],
            [
                    name: "max-upi",
                    description: "[IMPORT] Maximum sequence UPI to import - only used by the 'import' method."
            ],
            [
                    name: "batch-size",
                    description: "[ANALYSE] Max number of sequences per InterProScan job"
            ],
            [
                    name: "analyses",
                    description: "[CLEAN] IDs of analyses to clean (default: all) - only used by the 'clean' method."
            ],
            [
                    name: "help",
                    description: "Print the help message and exit."
            ],
            /*
            If an option's description is set to null, it will be hidden from the help message
            and no "Unrecognised option" warning will be produced.
            Use this for params defined in config files that should not be available on the command line
            */
            [
                name: "apps-config",
                description: null
            ],
            [
                    name: "databases",
                    description: null
            ],
            [
                    name: "interproscan",
                    description: null
            ]
    ]

    static void printHelp() {
        def result = new StringBuilder()
        result << "Name: InterProScan Production Manager (IPM)\n"
        result << "Usage: nextflow run main.nf -c <CONF> --method <METHOD>\n"
        result << "Mandatory parameters:\n"
        result << "-c <CONF>: config file with database uri and InterProScan configuration\n"
        result << "--methods <METHOD>: Methods to be run as a comma separated list. Choose from:\n"
        result << "    --methods import: Import UniParc sequences into IPPRO\n"
        result << "    --methods analyse: Analyse UniParc sequences in IPPRO using InterProScan\n"
        result << "    --methods clean: Delete obsolete data\n"

        result << "\nOptional parameters:\n"
        this.PARAMS.findAll{ !it.required && it.description }.each { param ->
            result << this.formatOption(param) << "\n"
        }

        print result.toString()
    }

    static String formatOption(option) {
        def text = "  --${option.name}"
        if (option.metavar) {
            text += " ${option.metavar}"
        }

        return text.padRight(40) + ": ${option.description}"
    }

    static void validateParams(params, log) {
        def allowedParams = this.PARAMS.collect { it.name.toLowerCase() }

        // Check that all params are recognized
        for (e in params) {
            def paramName = e.key
            def paramValue = e.value

            if (paramName.contains("-")) {
                /*
                    From https://www.nextflow.io/docs/latest/cli.html#pipeline-parameters
                    When the parameter name is formatted using `camelCase`,
                    a second parameter is created with the same
                    value using kebab-case, and vice versa.

                    However, we don't want to evalue the `kebab-case` params.
                    And they will eventually be ignored by NF directly,
                    see https://github.com/nextflow-io/nextflow/pull/4702.
                */
                continue
            }

            // Convert to kebab-case
            def kebabParamName = this.camelToKebab(paramName)
            if (allowedParams.contains(kebabParamName.toLowerCase())) {
                def paramObj = this.PARAMS.find { it.name.toLowerCase() == kebabParamName.toLowerCase() }
                assert paramObj != null
                if (paramObj?.metavar != null && !paramObj?.canBeNull && (paramValue instanceof Boolean)) {
                    log.error "'--${paramObj.name} ${paramObj.metavar}' is mandatory and cannot be empty."
                    System.exit(1)
                }
            } else {
                log.warn "Unrecognised option: '--${paramName}'. Try '--help' for more information."
            }
        }

        // Check that required params (--input, --datadir) are provided
        this.PARAMS.findAll{ it.required }.each { param ->
            def paramName = kebabToCamel(param.name)
            def paramValue = params[paramName]

            if (paramValue == null) {
                log.error "'--${param.name} ${param.metavar}' is mandatory."
                System.exit(1)
            }
        }
    }

    static String kebabToCamel(String kebabName) {
        return kebabName.split('-').toList().indexed().collect { index, word ->
            index == 0 ? word : word.capitalize()
        }.join('')
    }

    static String camelToKebab(String camelName) {
        return camelName.replaceAll(/([a-z])([A-Z])/, '$1-$2').toLowerCase()
    }

    static List validateMethods(String methods) {
        def error = null
        def methodsToRun = methods.split(",") .collect { it.trim().toLowerCase() }.toSet()
        def invalidMethods = methodsToRun.findAll { !METHODS.contains(it) }
        if (invalidMethods) {
            error = invalidMethods.collect { "Unrecognised method: '${it}'" }.join("\n")

        }
        return [methodsToRun, error]
    }

    static List resolveExecutable(String executable) {
        String error = ""
        if (!executable) {
            error = "Please specify a IPS6 executable - e.g. 'ebi-pf-team/interproscan6' or a path to the ips6 main.nf file"
            return [executable, error]
        }
        if (executable.startsWith("ebi-pf-team/interproscan6")) {
            return [executable, error]
        }
        
        Path path = Paths.get(executable)
        String executablePath = Files.isRegularFile(path) ? path.toAbsolutePath().toString() : null
        return [executablePath, error]
    }

    static String resolveFile(String filePath) {
        Path path = Paths.get(filePath)
        return Files.isRegularFile(path) ? path.toRealPath() : null
    }

    static validateLicenseConfig(String configPath) {
        if (!configPath) {
            return [null, null]
        }
        def path = configPath ? resolveFile(configPath) : null
        return path ? [path, null] : [null, "Could not locate iprscan configuration file '${configPath}'"]
    }

    static validateDbConfig(Map databaseConfig, List<String> databases) {
        def creds = null
        if (!databaseConfig) {
            return [null, "No database configurations provided.\nTip: Use the -c option to provide the path to a config file"]
        }
        String error = ""
        Map<String, Map> config = [:]
        databases.each { String db ->
            creds = databaseConfig.get(db, null)

            if (!creds || !creds.uri || !creds.user || !creds.password || !creds.engine) {
                error += "Missing or incomplete ${db} credentials in the conf file" + "\n"
            } else {
                config[db] = [
                    "uri"     : creds.uri,
                    "user"    : creds.user,
                    "password": creds.password,
                    "engine"  : creds.engine
                ]
            }
        }
        return [config, error ?: null]
    }

    static validateIprscanProfiles(Map config, String device) {
        String error = ""
        String profile = ""

        if (!config.executor || config.executor == "local" ) {
            profile = config.profile ? "${config.profile},${config.container}" : config.container
        } else if (config.executor == "slurm") {
            profile = config.executor
            if (config.container && !CLUSTER_CONTAINERS.contains(config.container)) {
                error = "Unrecognised container '${config.container}' for ${device} on the cluster"
            } else if (config.container) {
                profile += ",${config.container}"
            }
        } else {
            error = "Unrecognised executor '${config.executor}' for the InterProScan ${device} configuration"
        }

        return [profile, error]
    }
}
