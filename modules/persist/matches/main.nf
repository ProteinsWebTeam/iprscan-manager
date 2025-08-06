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

process PERSIST_MATCHES {
    // Insert and persist the matches into ISPRO/intproscan(db)
    errorStrategy 'ignore'

    input:
    tuple val(meta), val(job),  val(operation), val(matches_path), val(slurm_id_path)
    val iprscan_conf

    output:
    tuple val(meta), val(job),  val(operation), val(matches_path), val(slurm_id_path)

    exec:
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
        case "ncbifam":
            formatter      = this.&fmtDefaultMatches
            matchPersister = db.&persistDefaultMatches
            sitePersister  = null
            break
        case "signalp_euk":
        case "signalp_prok":
            formatter      = this.&fmtSignalpMatches
            matchPersister = db.&persistSignalpMatches
            sitePersister  = null
            break
        case "tmhmm":
        case "deeptmhmm":
            formatter      = this.&fmtMinimalistMatches
            matchPersister = db.&persistMinimalistMatches
            sitePersister  = null
            break
        default:
            throw new UnsupportedOperationException("Unknown database '${application}'")
    }

    matchValues = []
    siteValues  = []

    try {
        streamJson(matches_path.toString(), mapper) { results -> // streaming only the "results" Json Array
            def upi = results.get("xref")[0].get("id").asText()
            results.get("matches").each { match ->
                def (methodAc, modelAc, seqScore, seqEvalue) = getMatchData(match)
                matchMetaData = [
                    analysisId  : job.analysisId.toInteger(),
                    application : application,
                    majorVersion: majorVersion,
                    minorVersion: minorVersion,
                    upi         : upi,
                    methodAc    : methodAc,
                    modelAc     : modelAc,
                    seqScore    : seqScore,
                    seqEvalue   : seqEvalue,
                ]
                match.get("locations").each { location ->
                    (formattedMatch, formattedSite) = formatter(matchMetaData, location)
                    matchValues << formattedMatch
                    if (formattedSite) {
                        siteValues << formattedSite
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
        if (siteValues.size() == db.INSERT_SIZE) {
            sitePersister(siteValues, job.application.siteTable)
            siteValues.clear()
        }
    } catch (Exception e) {
        println "Error persisting results for ${application}: ${e}\nCause: ${e.getCause()}"
        e.printStackTrace()
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
