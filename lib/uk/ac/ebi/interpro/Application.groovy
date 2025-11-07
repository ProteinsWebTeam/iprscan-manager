package uk.ac.ebi.interpro

class Application implements Serializable { // Represents a member database release
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
