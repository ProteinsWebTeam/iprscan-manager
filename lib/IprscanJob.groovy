class IprscanJob {
    Integer analysis_id
    String maxUpi
    String dataDir
    String interproVersion
    String fasta = null
    String jobName = null
    Application application = null // member database

    IprscanJob(Integer analysis_id, String max_upi, String data_dir, String interpro_version) {
        this.analysis_id
        this.maxUpi = max_upi
        this.dataDir = data_dir
        this.interproVersion = interpro_version
    }

    void compileJobName() {
        this.jobName = "IPM_${this.analysis_id}_InterPro-${interproVersion}_DB-${application.name}_UPI-${this.maxUpi}"
    }

    void setFasta(String fasta) {
        this.fasta = fasta
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
}
