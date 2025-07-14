class IprscanJob {
    Integer analysis_id
    String maxUpi
    String dataDir
    String interproVersion
    String fasta = null
    Integer seqCount = null
    String jobName = null
    Application application = null // member database
    String createdTime = null

    IprscanJob(Integer analysis_id, String max_upi, String data_dir, String interpro_version) {
        this.analysis_id = analysis_id
        this.maxUpi = max_upi
        this.dataDir = data_dir
        this.interproVersion = interpro_version
    }

    void compileJobName(String prefix = null) {
        def prefixPart = prefix ? "${prefix}_" : ""
        this.jobName = "${prefixPart}IPM_A.ID-${this.analysis_id}_InterPro-${interproVersion}_DB-${application.name}_UPI-${this.maxUpi}"
    }

    void setFasta(String fasta, Integer seqCount) {
        this.fasta = fasta
        this.seqCount = seqCount
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
        }
        
        return [relMajor.toInteger(), relMinor]
    }
}
