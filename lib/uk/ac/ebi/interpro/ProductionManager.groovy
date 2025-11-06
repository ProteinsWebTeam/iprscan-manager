package uk.ac.ebi.interpro

import java.nio.file.*

class ProductionManager {
    static final def METHODS = ["analyse", "clean", "import", "index"]
    static final def LOCAL_CONTAINERS = ["docker", "singularity"]
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

    static resolveDirectory(String dirPath, boolean mustBeWritable = false) {
        Path path = Paths.get(dirPath)
        if (Files.exists(path)) {
            if (!Files.isDirectory(path)) {
                return [null, "Not a directory: ${dirPath}"]
            } else if (mustBeWritable && !Files.isWritable(path)) {
                return [null, "Directory not writable: ${dirPath}."]
            }
            return [path.toRealPath(), null]
        } else {
            try {
                Files.createDirectories(path)
                return [path.toRealPath(), null]
            } catch (IOException) {
                return [null, "Cannot create directory: ${dirPath}"]
            }
        }
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

    static validateConfig(String configPath) {
        def path = configPath ? resolveFile(configPath) : null
        return path ? [path, null] : [null, "Could not locate iprscan configuration file '${path}'"]
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

    static getIprscanProfiles(String cpu_executor, String gpu_executor, String container) {
        String error = ""
        String cpuProfile = ""
        String gpuProfile = ""

        if (!cpu_executor && !gpu_executor) {
            error = "No InterProSCan 6 executor provided for CPU or GPU devices"
            return [cpuProfile, gpuProfile, error]
        }

        if (cpu_executor) {
            (cpuProfile, gpuProfile, error) = validateProfile(cpu_executor, "CPU", container)
            if (error) { return [cpuProfile, gpuProfile, error] }
        } // else, local and no container defined - will run on baremetal
        if (gpu_executor) {
            (cpuProfile, gpuProfile, error) = validateProfile(gpu_executor, "GPU", container)
            if (error) { return [cpuProfile, gpuProfile, error] }
        } // else, local and no container defined - will run on baremetal

        return [cpuProfile, gpuProfile, error]
    }

    static validateProfile(String executor, String device, String container) {
        String errorMessage = ""
        String profile = ""
        
        if (executor == "local" && container) {
            if (!LOCAL_CONTAINERS.contains(container)) {
                errorMessage = "Unrecognised container '${container}' for the InterProScan local ${device} configuration"
                return [profile, errorMessage]
            }
            profile = container
        } else if (executor == "slurm") {
            profile = executor
            if (container) {
                if (!CLUSTER_CONTAINERS.contains(container)) {
                    errorMessage = "Unrecognised container '${container}' for the InterProScan cluster ${device} configuration"
                    return [profile, errorMessage]
                }
                profile += ",${container}"
            }
        } else {
            errorMessage = "Unrecognised executor '${executor}' for the InterProScan ${device} configuration"
            return [profile, errorMessage]
        }

        return [profile, errorMessage]
    }
}
