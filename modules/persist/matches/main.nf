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
    // errorStrategy 'ignore'

    // Insert and persist the matches into ISPRO
    input:
    tuple val(job), val(matches_path)
    val ispro_conf

    output:
    tuple val(job), val(matches_path)

    exec:
    def uri = ispro_conf.uri
    def user = ispro_conf.user
    def pswd = ispro_conf.password
    Database db = new Database(uri, user, pswd)
    ObjectMapper mapper = new ObjectMapper()

    def memberDb = job.application.name
    switch (memberDb) {
        case "ncbifam":
            persistDefault(job, matches_path.toString(), db, mapper)
            break
        case "signalp_euk":
        case "signalp_prok":
            persistSignalp(job, matches_path.toString(), db, mapper)
            break
        case "tmhmm":
        case "deeptmhmm":
            persistMinimalist(job, matches_path.toString(), db, mapper)
            break
        default:
            throw new UnsupportedOperationException("Unknown database '${memberDb}'")
    }

    db.close()
}

def getBigDecimal(JsonNode match, String key) {
    if (match.has(key) && !match.get(key).isNull()) {
        try { 
            return new BigDecimal(match.get(key).asText()).toPlainString() 
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

/* Update the streaming to parse the results section,
loading one seq at a time, and store the matches for the seq */

def persistDefault(IprscanJob job, String matches_path, Database db, ObjectMapper mapper) {
    def (majorVersion, minorVersion) = job.application.getRelease()
    def values = []

    streamJson(matches_path, mapper) { result -> // streaming only the "results" Json Array
        def upi = result.get("xref")[0].get("id").asText()
        result.get("matches").each { match ->
            def (methodAc, modelAc, seqScore, seqEvalue) = getMatchData(match)
            match.get("locations").each { location ->
                values << [
                    job.analysis_id.toInteger(),
                    job.application.name,
                    majorVersion,
                    minorVersion,
                    upi,
                    methodAc,
                    modelAc,
                    location.get("start").asInt(),
                    location.get("end").asInt(),
                    ftmFragments(location.get('location-fragments')),
                    seqScore,
                    seqEvalue,
                    reverseHmmBounds(location.get("hmmBounds").asText()),
                    location.get("hmmStart").asInt(),
                    location.get("hmmEnd").asInt(),
                    location.get("hmmLength").asInt(),
                    location.get("envelopeStart").asInt(),
                    location.get("envelopeEnd").asInt(),
                    getBigDecimal(location, "score"),
                    getBigDecimal(location, "evalue")
                ]

                if (values.size() == db.INSERT_SIZE) {
                    db.persistDefaultMatches(values, job.application.matchTable)
                    values.clear()
                }
            }
        }
    }

    if (!values.isEmpty()) {
        db.persistDefaultMatches(values, job.application.matchTable)
    }
}

def persistMinimalist(IprscanJob job, String matches_path, Database db, ObjectMapper mapper) {
    def (majorVersion, minorVersion) = job.application.getRelease()
    values = []

    streamJson(matches_path, mapper) { result -> // streaming only the "results" Json Array
        def upi = result.get("xref")[0].get("id").asText()
        results.get("matches").each { match ->
            def (methodAc, modelAc, seqScore, seqEvalue) = getMatchData(match)
            match.get("locations").each { location ->
                values << [
                    job.analysis_id.toInteger(),
                    job.application.name,
                    majorVersion,
                    minorVersion,
                    upi,
                    methodAc,
                    modelAc,
                    location.get("start").asInt(),
                    location.get("end").asInt(),
                    ftmFragments(location.get('location-fragments'))
                ]
            }

            if (values.size() == db.INSERT_SIZE) {
                db.persistMinimalistMatches(values, job.application.matchTable)
                values.clear()
            }
        }
    }

    if (!values.isEmpty()) {
        db.persistMinimalistMatches(values, job.application.matchTable)
    }
}

def persistSignalp(IprscanJob job, String matches_path, Database db, ObjectMapper mapper) {
    def (majorVersion, minorVersion) = job.application.getRelease()
    values = []

    streamJson(matches_path, mapper) { result -> // streaming only the "results" Json Array
        def upi = result.get("xref")[0].get("id").asText()
        results.get("matches").each{ match ->
            def (methodAc, modelAc, seqScore, seqEvalue) = getMatchData(match)
            matches.get("locations").each { location ->
                values << [
                    job.analysis_id.toInteger(),
                    job.application.name,
                    majorVersion,
                    minorVersion,
                    upi,
                    methodAc,
                    modelAc,
                    location.get("start").asInt(),
                    location.get("end").asInt(),
                    null,
                    getBigDecimal(location, "pvalue")
                ]

                if (values.size() == db.INSERT_SIZE) {
                    db.persistSignalpMatches(values, job.application.matchTable)
                    values.clear()
                }
            }
        }
    }

    if (!values.isEmpty()) {
        db.persistSignalpMatches(values, job.application.matchTable)
    }
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
