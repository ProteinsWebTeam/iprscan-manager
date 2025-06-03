import java.nio.file.*

class IPM {
    static final def METHODS = ["analyse", "clean", "import", "index"]

    static void printHelp() {
        def result = new StringBuilder()
        result << "Name: InterProScan Production Manager (IPM)\n"
        result << "Usage: nextflow run main.nf -c <CONF> --method <METHOD>\n"
        result << "Mandatory parameters:\n"
        result << "-c <CONF>: config file with database uri and InterProScan configuration\n"
        result << "--method <METHOD>: Method to be run. Choose from:\n"
        result << "    --method import: Import UniParc sequences into IPPRO\n"
        result << "    --method analyse: Analyse UniParc sequences in IPPRO using InterProScan\n"
        result << "    --method clean: Delete obsolete data\n"
        print result.toString()
    }

    static List validateMethods(String methods) {
        if (!methods) {
            return [null, "Please specify at least one method (--method): 'analyse', 'clean' or 'import'"]
        }
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
        String error = ""
        Map<String, Map> config = [:]
        databases.each {String db ->
            if (!databaseConfig[db]["uri"] || !databaseConfig[db]["user"] || !databaseConfig[db]["password"]) {
                error += "Missing or incomplete ${db} credentials\n"
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
