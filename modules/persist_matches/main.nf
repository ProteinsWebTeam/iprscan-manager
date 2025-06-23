import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import groovy.json.JsonOutput

process PERSIST_MATCHES {
    // Insert and persist the matches into ISPRO
    input:
    val job
    val ispro_conf

    exec:
    def uri = ispro_conf.uri
    def user = ispro_conf.user
    def pswd = ispro_conf.password
    Database db = new Database(uri, user, pswd)
    ObjectMapper mapper = new ObjectMapper()

    job.applications.each { String memberDb, Map data ->
        if (data.containsKey("json")) {  // if no matches were found the "json" key will be missing
            switch (memberDb) {
                case "signalp_euk":
                case "signalp_prok":
                    persistSignalp(data, db, mapper)
                    break
                case "tmhmm":
                case "deeptmhmm":
                    persistMinimalist(data, db, mapper)
                    break
                default:
                    throw new UnsupportedOperationException("Unknown database '${memberDb}'")
            }
        }
    }

    db.close()
}

def getMatch(JsonNode result, ObjectMapper mapper) {
    def upi = result.get("upi").asText()
    def match = mapper.convertValue(result.get("match"), Map)
    def methodAc = match.signature.accession
    def modelAc = match["model-ac"]
    def seqScore = match.score ? new java.math.BigDecimal(match.score.toString()).toPlainString() : 0
    def seqEvalue = match.evalue ? new java.math.BigDecimal(match.evalue.toString()).toPlainString() : 0
    return [upi, match, methodAc, modelAc, seqScore, seqEvalue]
}

def ftmFragments(List<Map> locationFragments) {
    // foramt the location-fragment map into the fragment string used in ISPRO
    Map dcstatus2symbol = [
        "CONTINUOUS"       : "S",
        "C_TERMINAL_DISC"  : "C",
        "N_TERMINAL_DISC"  : "N",
        "NC_TERMINAL_DISC" : "NC"
    ]
    List<String> fragmentStrings = []
    locationFragments.each { frag ->
        fragmentStrings << "${frag['start']}-${frag['end']}-${dcstatus2symbol[frag['dcStatus']]}"
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

def persistMinimalist(Map analysis_data, Database db, ObjectMapper mapper) {
    values = []
    streamJsonArray(analysis_data.json.toString(), mapper) { result ->
        def (upi, match, methodAc, modelAc, seqScore, seqEvalue) = getMatch(result, mapper)
        match.locations.each { Map location ->
            values << [
                analysis_data.id,
                analysis_data.name,
                null, null,
                upi,
                methodAc,
                modelAc,
                location["start"].toInteger(),
                location["end"].toInteger(),
                ftmFragments(location["location-fragments"]),
            ]
        }
    }
    db.persistMinimalistMatches(values, analysis_data.matchTable)
}

def persistSignalp(Map analysis_data, Database db, ObjectMapper mapper) {
    values = []
    streamJsonArray(analysis_data.json.toString(), mapper) { result ->
        (upi, match, methodAc, modelAc, seqScore, seqEvalue) = getMatch(result, mapper)
        match.locations.each { Map location ->
            values << [
                analysis_data.id,
                analysis_data.name,
                null, null,
                upi,
                methodAc,
                modelAc,
                location["start"].toInteger(),
                location["end"].toInteger(),
                null,
                new java.math.BigDecimal(location["pvalue"].toString()).toPlainString() // Why is this a big decimal???
            ]
        }
    }
    db.persistSignalpMatches(values, analysis_data.matchTable)
}

def streamJsonArray(String filePath, ObjectMapper mapper, Closure closure) {
    // Stream the data, one jsonNode at a time, from a Json file containing a JsonArray to reduce memory requirements
    try {
        JsonFactory factory = mapper.getFactory()
        JsonParser parser = factory.createParser(new File(filePath))
        if (parser.nextToken() == JsonToken.START_ARRAY) {
            while (parser.nextToken() != JsonToken.END_ARRAY) {
                JsonNode node = mapper.readTree(parser)
                closure.call(node)
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
