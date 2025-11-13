package uk.ac.ebi.interpro

import java.nio.file.*

class Iprscan implements Serializable {
    String executable    // Executable cmd/path to run iprscan
    String profile       // Comma separated list of interproscan profiles
    Path workDir         // Str repr of the path to the work dir
    String maxWorkers    // Max number of running parallel jobs in this iprscan instance
    String configFile    // Str repr of the path to the iprscan config file
    Map<String, String> resources     // Name of the resource configuration

    Iprscan(
        String executable,
        String profile, 
        Path workDir,
        String maxWorkers,
        String configFile,
    ) {
        this.executable = executable
        this.profile = profile
        this.workDir = workDir
        this.maxWorkers = maxWorkers
        this.configFile = configFile
    }

    Iprscan(
        String executable,
        String profile,
        Path workDir,
        def maxWorkers, 
        String configFile,
        Boolean gpu
    ) {
        this.executable = executable
        this.profile = profile
        this.workDir = workDir
        this.maxWorkers = maxWorkers ? maxWorkers.toString() : null
        this.configFile = configFile
        this.resources = ["gpu": gpu]
    }

    void addResources(Map resourceMap, String appName, Boolean gpu) {
        def subbatched = resourceMap.subbatched.contains(appName) ? "subbatched" : "notSubbatched"
        cpus = resourceMap[subbatched].cpus.toString()

        def label = resourceMap.resources.get(appName, "light")
        
        def (memValue, unit) = (resourceMap.resources[label].memory.toString() =~ /(\d+(?:\.\d+)?)(?:\s*\.?\s*(\w+))?/)[0][1,2]
        mem = "${(memValue.toDouble() * task.attempt).round(1)} ${unit ?: 'GB'}"
        
        def (timeValue, unit) = (presourceMap.resources[label].time.toString() =~ /(\d+(?:\.\d+)?)(?:\s*\.?\s*(\w+))?/)[0][1,2]
        timeLimit = "${(timeValue.toDouble() * task.attempt).round(1)} ${unit ?: 'h'}"
        
        this.resources = [
            cpus: cpus,
            mem: mem,
            time: timeLimit,
            gpu: gpu
        ]
    }
}
