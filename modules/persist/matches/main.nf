// Insert and persist the matches in the IS-db (e.g. ISPRO)

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.node.ArrayNode
import groovy.json.JsonOutput

import uk.ac.ebi.interpro.Database

process PERSIST_MATCHES {
    // Insert and persist the matches into ISPRO/intproscan(db)
    executor 'local'
    // errorStrategy 'ignore'

    input:
    tuple val(meta), val(job), val(gpu), val(slurm_id_path), val(matches_path)
    val iprscan_conf

    output:
    tuple val(meta), val(job), val(gpu), val(slurm_id_path)

    exec:
    def fasta = new File(job.fasta.toString())
    def seqLengths = [:]
    def currentId = null
    def currentLength = 0
    fasta.eachLine { line ->
        if (line.startsWith(">")) {
            if (currentId) {
                seqLengths[currentLength] = currentLength
            }
            currentId = line.trim().substring(1).split(/\s+/)[0]
            currentLength =0
        } else {
            currentLength += line.trim().length()
        }
    }
    if (currentId) {
        seqLengths[currentId] = currentLength
    }

    Database db = new Database(
        iprscan_conf.uri,
        iprscan_conf.user,
        iprscan_conf.password,
        iprscan_conf.engine
    )
    ObjectMapper mapper = new ObjectMapper()

    def application = job.application.name
    def (majorVersion, minorVersion) = job.application.getRelease()
    def formatter
    def matchPersister
    def sitePersister

    switch (application) {
        case "antifam":
        case "cath-gene3d":
        case "gene3d":
        case "cath-funfam":
        case "funfam":
        case "ncbifam":
        case "pfam":
        case "pirsf":
            formatter      = this.&fmtDefaultMatches
            matchPersister = db.&persistDefaultMatches
            sitePersister  = null
            break
        case "coils":
        case "phobius":
        case "tmbed":
        case "tmhmm":
        case "deeptmhmm":
            formatter      = this.&fmtMinimalistMatches
            matchPersister = db.&persistMinimalistMatches
            sitePersister  = null
            break
        case "cdd":
            formatter      = this.&fmtCddMatches
            matchPersister = db.&persistCddMatches
            sitePersister  = db.&persistDefaultSites
            break
        case "hamap":
            formatter      = this.&fmtHamapMatches
            matchPersister = db.&persistHamapMatches
            sitePersister  = null
            break
        case "interpro_n":
            formatter      = this.&fmtInterproNMatches
            matchPersister = db.&persistInterproNMatches
            sitePersister  = null
            break
        case "mobidb lite":
        case "mobidb-lite":
        case "mobidb_lite":
            formatter      = this.&fmtMobidbliteMatches
            matchPersister = db.&persistMobiDBliteMatches
            sitePersister  = null
            break
        case "panther":
            formatter      = this.&fmtPantherMatches
            matchPersister = db.&persistPantherMatches
            sitePersister  = null
            break
        case "pirsr":
            formatter      = this.&fmtPirsrMatches
            matchPersister = db.&persistPirsrMatches
            // sitePersister  = db.&persistDefaultSites
            sitePersister  = null
            break
        case "prints":
            formatter      = this.&fmtPrintsMatches
            matchPersister = db.&persistPrintsMatches
            sitePersister  = null
            break
        case "prosite-patterns":
            formatter      = this.&fmtPrositePatternsMatches
            matchPersister = db.&persistPrositePatternsMatches
            sitePersister  = null
            break
        case "prosite-profiles":
            formatter      = this.&fmtPrositeProfilesMatches
            matchPersister = db.&persistPrositeProfileMatches
            sitePersister  = null
            break
        case "sfld":
            formatter      = this.&fmtSfldMatches
            matchPersister = db.&persistDefaultMatches
            sitePersister  = db.&persistDefaultSites
            break
        case "smart":
            formatter      = this.&fmtSmartMatches
            matchPersister = db.&persistSmartMatches
            sitePersister  = null
            break
        case "superfamily":
            formatter      = this.&fmtSuperfamilyMatches
            matchPersister = db.&persistSuperfamilyMatches
            sitePersister  = null
            break
        case "signalp_euk":
        case "signalp_prok":
            formatter      = this.&fmtSignalpMatches
            matchPersister = db.&persistSignalpMatches
            sitePersister  = null
            break
        default:
            throw new UnsupportedOperationException("Unknown database '${application}'")
    }

    def matchValues = []
    def siteValues  = []
    // don't put inside a try/catch, we want the error to cause the process to end and mark the job as failed
    streamJson(matches_path.toString(), mapper) { results -> // streaming only the "results" Json Array
        def upi = results.get("xref")[0].get("id").asText()
        def seqLength = seqLengths.get(upi)
        results.get("matches").each { match ->
            def graphscan = match.get("graphscan")?.asText(null)  // returns null if key is missing
            def ancestralNodeID = match.get("ancestralNodeID")?.asText(null)  // returns null if key is missing
            def (methodAc, modelAc, seqScore, seqEvalue) = getMatchData(match)
            matchMetaData = [
                analysisId     : job.analysisId.toInteger(),
                application    : application,
                majorVersion   : majorVersion,
                minorVersion   : minorVersion,
                upi            : upi,
                md5            : results.get("md5").asText(),
                methodAc       : methodAc,
                modelAc        : modelAc,
                seqScore       : seqScore,
                seqEvalue      : seqEvalue,
                seqLength      : seqLength ? seqLength.toInteger() : null,
                graphscan      : graphscan,
                ancestralNodeID: ancestralNodeID
            ]
            match.get("locations").each { location ->
                (formattedMatch, formattedSites) = formatter(matchMetaData, location)
                matchValues << formattedMatch
                if (formattedSites) {
                    siteValues.addAll(formattedSites)
                }
                if (matchValues.size() == db.INSERT_SIZE) {
                    matchPersister(matchValues, job.application.matchTable)
                    matchValues.clear()
                }
                if (siteValues.size() == db.INSERT_SIZE) {
                    sitePersister(siteValues, job.application.siteTable)
                    siteValues.clear()
                }
            }
        }
    }

    if (!matchValues.isEmpty()) {
        matchPersister(matchValues, job.application.matchTable)
        matchValues.clear()
    }
    if (!siteValues.isEmpty()) {
        sitePersister(siteValues, job.application.siteTable)
        siteValues.clear()
    }

    db.close()
}

def getBigDecimal(JsonNode match, String key) {
    if (match.has(key) && !match.get(key).isNull()) {
        try { 
            return new BigDecimal(match.get(key).asText())
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid number format for key '${key}': '${valueText}'", e)
        }
    }
    return 0
}

def getMatchData(JsonNode match) {
    def methodAc = match.get("signature").get("accession").asText()
    def modelAc = match.get("model-ac").asText()
    def seqScore = getBigDecimal(match, "score")
    def seqEvalue = getBigDecimal(match, "evalue")
    return [methodAc, modelAc, seqScore, seqEvalue]
}

def ftmFragments(ArrayNode locationFragments) {
    // foramt the location-fragment map into the fragment string used in ISPRO
    Map dcstatus2symbol = [
        "CONTINUOUS"       : "S",
        "C_TERMINAL_DISC"  : "C",
        "N_TERMINAL_DISC"  : "N",
        "NC_TERMINAL_DISC" : "NC"
    ]
    List<String> fragmentStrings = []
    locationFragments.each { frag ->
        fragmentStrings << "${frag.get('start')}-${frag.get('end')}-${dcstatus2symbol[frag.get('dc-status').asText()]}"
    }
    return fragmentStrings.join(",")
}

def reverseHmmBounds(String hmmBounds) {
    return [
           "COMPLETE"            : "[]",
           "N_TERMINAL_COMPLETE" : "[.",
           "C_TERMINAL_COMPLETE" : ".]",
           "INCOMPLETE"          : ".."
    ][hmmBounds]
}

def fmtDefaultMatches(Map matchMetaData, JsonNode location) {
    matchValue = [
        matchMetaData.analysisId,
        matchMetaData.application,
        matchMetaData.majorVersion,
        matchMetaData.minorVersion,
        matchMetaData.upi,
        matchMetaData.methodAc,
        matchMetaData.modelAc,
        location.get("start").asInt(),
        location.get("end").asInt(),
        ftmFragments(location.get('location-fragments')),
        matchMetaData.seqScore,
        matchMetaData.seqEvalue,
        reverseHmmBounds(location.get("hmmBounds").asText()),
        location.get("hmmStart").asInt(),
        location.get("hmmEnd").asInt(),
        location.get("hmmLength").asInt(),
        location.get("envelopeStart").asInt(),
        location.get("envelopeEnd").asInt(),
        getBigDecimal(location, "score"),
        getBigDecimal(location, "evalue")
    ]
    siteValue = null
    return [matchValue, siteValue]
}

def fmtMinimalistMatches(Map matchMetaData, JsonNode location) {
    matchValue = [
        matchMetaData.analysisId,
        matchMetaData.application,
        matchMetaData.majorVersion,
        matchMetaData.minorVersion,
        matchMetaData.upi,
        matchMetaData.methodAc,
        matchMetaData.modelAc,
        location.get("start").asInt(),
        location.get("end").asInt(),
        ftmFragments(location.get('location-fragments'))
    ]
    siteValue = null
    return [matchValue, siteValue]
}

def fmtCddMatches(Map matchMetaData, JsonNode location) {
    matchValue = [
        matchMetaData.analysisId,
        matchMetaData.application,
        matchMetaData.majorVersion,
        matchMetaData.minorVersion,
        matchMetaData.upi,
        matchMetaData.methodAc,
        matchMetaData.modelAc,
        location.get("start").asInt(),
        location.get("end").asInt(),
        ftmFragments(location.get('location-fragments')),
        matchMetaData.seqScore,
        matchMetaData.seqEvalue
    ]
    def siteValues = []
    location.sites.each { site ->
        site.siteLocations.each { siteLoc ->
            siteValues << [
                matchMetaData.analysisId,
                matchMetaData.upi,
                matchMetaData.md5,
                matchMetaData.seqLength,
                matchMetaData.application,
                matchMetaData.methodAc,
                location.get("start").asInt(),
                location.get("end").asInt(),
                site.get("numLocations").asInt(),
                siteLoc.get("residue").asText(),
                siteLoc.get("start").asInt(),
                siteLoc.get("end").asInt(),
                site.get("description").asText()
            ]
        }
    }
    return [matchValue, siteValues]
}

def fmtHamapMatches(Map matchMetaData, JsonNode location) {
    matchValue = [
        matchMetaData.analysisId,
        matchMetaData.application,
        matchMetaData.majorVersion,
        matchMetaData.minorVersion,
        matchMetaData.upi,
        matchMetaData.methodAc,
        matchMetaData.modelAc,
        location.get("start").asInt(),
        location.get("end").asInt(),
        ftmFragments(location.get('location-fragments')),
        getBigDecimal(location, "score"),
        location.get("cigarAlignment").asText()
    ]
    siteValues = null
    return [matchValue, siteValues]
}

def fmtInterproNMatches(Map matchMetaData, JsonNode location) {
    matchValue = [
        matchMetaData.analysisId,
        matchMetaData.application,
        matchMetaData.majorVersion,
        matchMetaData.minorVersion,
        matchMetaData.upi,
        matchMetaData.methodAc,
        matchMetaData.modelAc,
        location.get("start").asInt(),
        location.get("end").asInt(),
        matchMetaData.seqScore,
        ftmFragments(location.get('location-fragments'))
    ]
    siteValue = null
    return [matchValue, siteValue]
}

def fmtMobidbliteMatches(Map matchMetaData, JsonNode location) {
    def seqFeature = location.get("sequence-feature")
    matchValue = [
        matchMetaData.analysisId,
        matchMetaData.application,
        matchMetaData.majorVersion,
        matchMetaData.minorVersion,
        matchMetaData.upi,
        matchMetaData.methodAc,
        matchMetaData.modelAc,
        location.get("start").asInt(),
        location.get("end").asInt(),
        ftmFragments(location.get('location-fragments')),
        seqFeature ? seqFeature.asText() : "-"
    ]
    siteValues = null
    return [matchValue, siteValues]
}

def fmtPantherMatches(Map matchMetaData, JsonNode location) {
    matchValue = [
        matchMetaData.analysisId,
        matchMetaData.application,
        matchMetaData.majorVersion,
        matchMetaData.minorVersion,
        matchMetaData.upi,
        matchMetaData.methodAc,
        matchMetaData.modelAc,
        location.get("start").asInt(),
        location.get("end").asInt(),
        ftmFragments(location.get('location-fragments')),
        matchMetaData.seqScore,
        matchMetaData.seqEvalue,
        reverseHmmBounds(location.get("hmmBounds").asText()),
        location.get("hmmStart").asInt(),
        location.get("hmmEnd").asInt(),
        location.get("hmmLength").asInt(),
        location.get("envelopeStart").asInt(),
        location.get("envelopeEnd").asInt(),
        matchMetaData.ancestralNodeID
    ]
    siteValues = null
    return [matchValue, siteValues]
}

def fmtPirsrMatches(Map matchMetaData, JsonNode location) {
    def matchValue = [
        matchMetaData.analysisId,
        matchMetaData.application,
        matchMetaData.majorVersion,
        matchMetaData.minorVersion,
        matchMetaData.upi,
        matchMetaData.methodAc,
        matchMetaData.modelAc,
        location.get("start").asInt(),
        location.get("end").asInt(),
        ftmFragments(location.get('location-fragments')),
        matchMetaData.seqScore,
        matchMetaData.seqEvalue,
        null,  // do not report hmmbounds
        location.get("hmmStart").asInt(),
        location.get("hmmEnd").asInt(),
        location.get("hmmLength").asInt(),
        location.get("envelopeStart").asInt(),
        location.get("envelopeEnd").asInt(),
        getBigDecimal(location, "score"),
        getBigDecimal(location, "evalue")
    ]
    def siteValues = []
    location.sites.each { site ->
        site.siteLocations.each { siteLoc ->
            siteValues << [
                matchMetaData.analysisId,
                matchMetaData.upi,
                matchMetaData.md5,
                matchMetaData.seqLength,
                matchMetaData.application,
                matchMetaData.methodAc,
                location.get("start").asInt(),
                location.get("end").asInt(),
                site.get("numLocations").asInt(),
                siteLoc.get("residue").asText(),
                siteLoc.get("start").asInt(),
                siteLoc.get("end").asInt(),
                site.get("description").asText()
            ]
        }
    }
    return [matchValue, siteValues]
}

def fmtPrintsMatches(Map matchMetaData, JsonNode location) {
    matchValue = [
        matchMetaData.analysisId,
        matchMetaData.application,
        matchMetaData.majorVersion,
        matchMetaData.minorVersion,
        matchMetaData.upi,
        matchMetaData.methodAc,
        matchMetaData.modelAc,
        location.get("start").asInt(),
        location.get("end").asInt(),
        ftmFragments(location.get('location-fragments')),
        matchMetaData.seqScore,
        matchMetaData.seqEvalue,
        location.get("motifNumber").asInt(),
        getBigDecimal(location, "pvalue"),
        matchMetaData.graphscan
    ]
    siteValue = null
    return [matchValue, siteValue]
}

def fmtPrositePatternsMatches(Map matchMetaData, JsonNode location) {
    matchValue = [
        matchMetaData.analysisId,
        "ProSitePatterns",
        matchMetaData.majorVersion,
        matchMetaData.minorVersion,
        matchMetaData.upi,
        matchMetaData.methodAc,
        matchMetaData.modelAc,
        location.get("start").asInt(),
        location.get("end").asInt(),
        ftmFragments(location.get('location-fragments')),
        0,
        location.get("cigarAlignment").asText()
    ]
    siteValue = null
    return [matchValue, siteValue]
}

def fmtPrositeProfilesMatches(Map matchMetaData, JsonNode location) {
    matchValue = [
        matchMetaData.analysisId,
        "ProSiteProfiles",
        matchMetaData.majorVersion,
        matchMetaData.minorVersion,
        matchMetaData.upi,
        matchMetaData.methodAc,
        matchMetaData.modelAc,
        location.get("start").asInt(),
        location.get("end").asInt(),
        ftmFragments(location.get('location-fragments')),
        location.get("cigarAlignment").asText()
    ]
    siteValue = null
    return [matchValue, siteValue]
}

def fmtSfldMatches(Map matchMetaData, JsonNode location) {
    matchValue = [
        matchMetaData.analysisId,
        matchMetaData.application,
        matchMetaData.majorVersion,
        matchMetaData.minorVersion,
        matchMetaData.upi,
        matchMetaData.methodAc,
        matchMetaData.modelAc,
        location.get("start").asInt(),
        location.get("end").asInt(),
        ftmFragments(location.get('location-fragments')),
        matchMetaData.seqScore,
        matchMetaData.seqEvalue,
        null,  // do not report hmm-bounds
        location.get("hmmStart").asInt(),
        location.get("hmmEnd").asInt(),
        location.get("hmmLength").asInt(),
        location.get("envelopeStart").asInt(),
        location.get("envelopeEnd").asInt(),
        getBigDecimal(location, "score"),
        getBigDecimal(location, "evalue")
    ]
    def siteValues = []
    location.sites.each { site ->
        site.siteLocations.each { siteLoc ->
            siteValues << [
                matchMetaData.analysisId,
                matchMetaData.upi,
                matchMetaData.md5,
                matchMetaData.seqLength,
                matchMetaData.application,
                matchMetaData.methodAc,
                location.get("start").asInt(),
                location.get("end").asInt(),
                site.get("numLocations").asInt(),
                siteLoc.get("residue").asText(),
                siteLoc.get("start").asInt(),
                siteLoc.get("end").asInt(),
                site.get("description").asText()
            ]
        }
    }
    return [matchValue, siteValues]
}

def fmtSmartMatches(Map matchMetaData, JsonNode location) {
    matchValue = [
        matchMetaData.analysisId,
        matchMetaData.application,
        matchMetaData.majorVersion,
        matchMetaData.minorVersion,
        matchMetaData.upi,
        matchMetaData.methodAc,
        matchMetaData.modelAc,
        location.get("start").asInt(),
        location.get("end").asInt(),
        ftmFragments(location.get('location-fragments')),
        matchMetaData.seqScore,
        matchMetaData.seqEvalue,
        reverseHmmBounds(location.get("hmmBounds").asText()),
        location.get("hmmStart").asInt(),
        location.get("hmmEnd").asInt(),
        location.get("hmmLength").asInt(),
        getBigDecimal(location, "score"),
        getBigDecimal(location, "evalue")
    ]
    siteValue = null
    return [matchValue, siteValue]
}

def fmtSuperfamilyMatches(Map matchMetaData, JsonNode location) {
    matchValue = [
        matchMetaData.analysisId,
        matchMetaData.application,
        matchMetaData.majorVersion,
        matchMetaData.minorVersion,
        matchMetaData.upi,
        matchMetaData.methodAc,
        matchMetaData.modelAc,
        location.get("start").asInt(),
        location.get("end").asInt(),
        ftmFragments(location.get('location-fragments')),
        matchMetaData.seqEvalue,
        location.get("hmmLength").asInt()
    ]
    siteValue = null
    return [matchValue, siteValue]
}

def fmtSignalpMatches(Map matchMetaData, JsonNode location) {
    matchValue = [
        matchMetaData.analysisId,
        matchMetaData.application.replace("signalp_prok", "SignalP_PROK").replace("signalp_euk", "SignalP_EUK"),
        matchMetaData.majorVersion,
        matchMetaData.minorVersion,
        matchMetaData.upi,
        matchMetaData.methodAc,
        matchMetaData.modelAc,
        location.get("start").asInt(),
        location.get("end").asInt(),
        null,
        getBigDecimal(location, "pvalue")
    ]
    siteValue = null
    return [matchValue, siteValue]
}

def streamJson(String filePath, ObjectMapper mapper, Closure closure) {
    /* Stream the data under the "results" field in the interproscan6 output JSON file,
    one jsonNode at a time, to reduce memory requirements */
    JsonFactory factory = mapper.getFactory()
    try {
        JsonParser parser = factory.createParser(new File(filePath))
        while (!parser.isClosed()) {
            JsonToken token = parser.nextToken()
            if (token == null) {
                break
            }
            // Look for the 'results' field
            if (token == JsonToken.FIELD_NAME && parser.getCurrentName() == "results") {
                parser.nextToken()  // Move to the start of the array
                if (parser.getCurrentToken() == JsonToken.START_ARRAY) {
                    while (parser.nextToken() != JsonToken.END_ARRAY) {
                        JsonNode result = mapper.readTree(parser)
                        // Call the closure with the result (JsonNode)
                        closure.call(result)
                    }
                }
            }
        }
        parser.close()
    } catch (FileNotFoundException e) {
        throw new Exception("File not found: $filePath -- $e\n${e.getCause()}", e)
    } catch (JsonParseException e) {
        throw new Exception("Error parsing JSON file: $filePath -- $e\n${e.getCause()}", e)
    } catch (JsonMappingException e) {
        throw new Exception("Error mapping JSON content for file: $filePath -- $e\n${e.getCause()}", e)
    } catch (IOException e) {
        throw new Exception("IO error reading file: $filePath -- $e\n${e.getCause()}", e)
    } catch (Exception e) {
        throw new Exception("Error parsing JSON file $filePath -- $e\n${e.getCause()}", e)
    }
}
