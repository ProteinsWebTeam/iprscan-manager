package uk.ac.ebi.interpro

import java.nio.file.*

class Iprscan implements Serializable {
    String executable    // Executable cmd/path to run iprscan
    String profile       // Comma separated list of interproscan profiles
    Path workDir         // Str repr of the path to the work dir
    String maxWorkers    // Max number of running parallel jobs in this iprscan instance
    String configFile    // Str repr of the path to the iprscan config file
    String resources     // Name of the resource configuration
    Boolean gpu          // Cpu or Gpu

    Iprscan(String executable, String profile, Path workDir, def maxWorkers, String configFile, Boolean gpu) {
        this.executable = executable
        this.profile = profile
        this.workDir = workDir
        this.maxWorkers = maxWorkers ? maxWorkers.toString() : null
        this.configFile = configFile
        this.gpu = gpu
    }

    Iprscan(String executable, String profile, Path workDir, String maxWorkers, String configFile, String resources, Boolean gpu) {
        this.executable = executable
        this.profile = profile
        this.workDir = workDir
        this.maxWorkers = maxWorkers
        this.configFile = configFile
        this.resources = resources
        this.gpu = gpu
    }
}
