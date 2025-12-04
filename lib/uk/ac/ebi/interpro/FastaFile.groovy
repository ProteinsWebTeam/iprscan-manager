package uk.ac.ebi.interpro

class FastaFile {
    String path
    String upiFrom
    String upiTo
    Integer seqCount

    FastaFile(String path, String upiFrom, String upiTo, Integer seqCount) {
        this.path = path
        this.upiFrom = upiFrom
        this.upiTo = upiTo
        this.seqCount = seqCount
    }
}