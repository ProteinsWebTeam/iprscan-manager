class IprscanJob {
    String maxUpi
    String dataDir
    String interproVersion
    String fasta = null
    Map<String, Map> applications = [:]  // memberdb: [meta data]

    IprscanJob(String max_upi, String data_dir, String interpro_version) {
        this.maxUpi = max_upi
        this.dataDir = data_dir
        this.interproVersion = interpro_version
    }

    void addApplication(
        String application,
        Integer analysis_id,
        String version,
        String matchTable = null,
        String siteTable = null
    ) {
        // Lower case the appName to rm case-sensitive mismatches in the i6 output
        this.applications[application.toLowerCase().replace(" ","-")] = [
            id: analysis_id,        // id in the iprscan.analysis table
            name: application,      // to make the ispro analysis_name formating
            version: version,       // member db release
            matchTable: matchTable, // table to store matches in the interpro db
            siteTable: siteTable    // table to store sites in the interpro db
        ]
    }

    void setFasta(String fasta) {
        this.fasta = fasta
    }
}