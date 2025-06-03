import java.nio.file.*

class IPM {
    static final def METHODS = ["analyse", "clean", "import", "index"]

    static final def PARAMS = [
            [
                    name: "method",
                    required: true,
                    metavar: "<METHODS>",
                    dscription: "Methods to be run as a comma separated list. Choose from: 'import', 'analyse', and 'clean'"
            ],
            [
                    name: "top-up",
                    description: "Import new sequences only - only used by the 'import' method."
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
                    name: "databases",
                    description: null
            ]
    ]

    static void printHelp() {
        def result = new StringBuilder()
        result << "Name: InterProScan Production Manager (IPM)\n"
        result << "Usage: nextflow run main.nf -c <CONF> --method <METHOD>\n"
        result << "Mandatory parameters:\n"
        result << "-c <CONF>: config file with database uri and InterProScan configuration\n"
        result << "--method <METHOD>: Methods to be run as a comma separated list. Choose from:\n"
        result << "    --method import: Import UniParc sequences into IPPRO\n"
        result << "    --method analyse: Analyse UniParc sequences in IPPRO using InterProScan\n"
        result << "    --method clean: Delete obsolete data\n"

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

    static String resolveExecutable(String executable) {
        if (executable.startsWith("ebi-pf-team/interproscan6")) {
            return executable
        }
        Path path = Paths.get(executable)
        return Files.isRegularFile(path) ? path.toRealPath() : null
    }

    static String resolveFile(String filePath) {
        Path path = Paths.get(filePath)
        return Files.isRegularFile(path) ? path.toRealPath() : null
    }

    static valdidateDbConfig(Map databaseConfig, List<String> databases) {
        if (!databaseConfig) {
            return [null, "No database configurations provided.\nTip: Use the -c option to provide the path to a config file"]
        }
        String error = ""
        Map<String, Map> config = [:]
        databases.each {String db ->
            if (!databaseConfig.containsKey(db)) {
                error += "Missing or incomplete ${db} credentials in the conf file\n"
            } else if (!databaseConfig[db]["uri"] || !databaseConfig[db]["user"] || !databaseConfig[db]["password"]) {
                error += "Missing or incomplete ${db} credentials in the conf file\n"
            } else {
                config[db] = [
                        "uri": databaseConfig[db]["uri"],
                        "user": databaseConfig[db]["user"],
                        "password": databaseConfig[db]["password"]
                ]
            }
        }
        return [config, error ?: null]
    }

    static getIprscanProfiles(String executor, String container) {
        // Will need to adapt when i6 can run on bare metal
        String error = ""
        String profile = ""
        if (!container) {
            error += "No IPS6 container runtime specified" // rm when i6 can run on bare metal
        } else {
            profile = container
        }

        if (executor && executor != "local") {
            if (executor != "slurm") {
                error += "Executor '${executor}' not recognised"
            }
            profile += ",${executor}"
        }
        return [profile, error]
    }
}
