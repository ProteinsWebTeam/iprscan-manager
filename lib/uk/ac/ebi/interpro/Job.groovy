package uk.ac.ebi.interpro

import uk.ac.ebi.interpro.Iprscan

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class Job {
    Integer analysisId             // Analysis ID in the ISPRO db
    String maxUpi                  // Max UPI to analyse
    String dataDir                 // Str repr of the path to the interproscan 6 data dir
    String interproVersion         // InterPro database release to use
    Boolean gpu                    // Enable disable GPU acceleration
    String fasta = null            // Str repr of the path to the FASTA file to be analysed
    Integer seqCount = null        // Number of sequences being analysed - for insertion into the ANALYSIS_JOBS table
    String name = null             // Name given to the SLURM job - so we can retrieve information for the ANALYSIS_JOBS table
    String upiFrom = null          // Upper range of the analysed sequences - for insertion into the ANALYSIS_JOBS table
    String upiTo = null            // Lower range of the analysed sequences - for insertion into the ANALYSIS_JOBS table
    Application application = null // Member database
    String createdTime = null      // Time sbatch job is created - for insertion into the ANALYSIS_JOBS table
    Iprscan iprscan = null         // Iprscan class instance - stores executable and configuration for iprscan

    Job(Integer analysis_id, String max_upi, String data_dir, String interpro_version, Boolean gpu, String resources) {
        this.analysisId = analysis_id
        this.maxUpi = max_upi
        this.dataDir = data_dir
        this.interproVersion = interpro_version
        this.gpu = gpu
        this.resources = resources
    }

    Job(
        Integer analysis_id,
        String max_upi,
        String data_dir,
        String interpro_version,
        Boolean gpu,
        Application application,
        Iprscan iprscan,
        String fasta,
        Integer seqCount,
        String upiFrom,
        String upiTo
    ) {
        def now = LocalDateTime.now()
        def formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        def createdTime = now.format(formatter)

        this.analysisId = analysis_id
        this.maxUpi = max_upi
        this.dataDir = data_dir
        this.interproVersion = interpro_version
        this.gpu = gpu
        this.application = application
        this.iprscan = iprscan
        this.fasta = fasta
        this.seqCount = seqCount
        this.upiFrom = upiFrom
        this.upiTo = upiTo
        this.createdTime = createdTime
    }

    void compileJobName() {
        // File a name for the SLURM job so we can retrieve information for this job later
        this.name = "analysis.id-${this.analysisId}_interpro.v-${interproVersion}_app-${application.name}_upi-${this.maxUpi}"
    }
}

class Application { // Represents a member database release
    String name
    String version
    String matchTable = null
    String siteTable = null

    Application(String name, String version, String matchTable = null, String siteTable = null) {
        // Lower case the appName to rm case-sensitive mismatches in the i6 output
        def appName = name.toLowerCase().replace(" ","-")
        this.name = appName
        this.version = version
        this.matchTable = matchTable
        this.siteTable = siteTable
    }

    List<String> getRelease() {
        def relMajor = null
        def relMinor = null
        
        if (this.version.contains(".")) {
            def parts = this.version.split("\\.")  // Escape dot since it is a regex metacharacter
            if (parts.size() >= 2) {
                relMajor = parts[0]
                relMinor = parts[1]
            }
        } else if (this.version.contains("_")) {
            def parts = this.version.split("_")
            if (parts.size() >= 2) {
                relMajor = parts[0]
                relMinor = parts[1]
            }
        } else {
            relMajor = this.version
        }
        
        return [relMajor.toInteger(), relMinor]
    }
}
