package uk.ac.ebi.interpro

import java.nio.file.*

class Iprscan implements Serializable {
    String executable    // Executable cmd/path to run iprscan
    String profile       // Comma separated list of interproscan profiles
    Path workDir         // Str repr of the path to the work dir
    String maxWorkers    // Max number of running parallel jobs in this iprscan instance
    String configFile    // Str repr of the path to the iprscan config file
    Map resources        // Name of the resource configuration

    Iprscan(
        String executable,
        String profile, 
        Path workDir,
        String maxWorkers,
        String configFile
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
        println "hello"
        def subbatched = resourceMap.subbatched.contains(appName) ? "subbatched" : "notSubbatched"
        def cpus = resourceMap.resources[subbatched]["cpus"].toString()
        def label = resourceMap.resources.get(appName, "light")        
        def (memValue, memUnit) = (resourceMap.resources[label].memory.toString() =~ /(\d+(?:\.\d+)?)(?:\s*\.?\s*(\w+))?/)[0][1,2]
        def (timeValue, timeUnit) = (resourceMap.resources[label].time.toString() =~ /(\d+(?:\.\d+)?)(?:\s*\.?\s*(\w+))?/)[0][1,2]

        this.resources = [
            cpus: cpus,
            mem: [
                value: memValue.toDouble(),
                unit: memUnit,
            ],
            time: [
                value: timeValue.toDouble(),
                unit: timeUnit
            ],
            gpu: gpu
        ]
    }
}
