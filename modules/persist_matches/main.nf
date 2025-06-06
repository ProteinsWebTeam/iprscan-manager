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
                case "antifam":
                    persistDefault(data, db, mapper)
                    break
                case "cath-gene3d":
                case "gene3d":
                    persistDefault(data, db, mapper)
                    break
                case "cath-funfam":
                case "funfam":
                    persistDefault(data, db, mapper)
                    break
                case "cdd":
                    persistCdd(data, db, mapper)
                    break
                case "coils":
                    persistMinimalist(data, db, mapper)
                    break
                case "hamap":
                    persistHamap(data, db, mapper)
                    break
                case "mobidb lite":
                case "mobidb-lite":
                case "mobidb_lite":
                    persistMobiDBlite(data, db, mapper)
                    break
                case "ncbifam":
                    persistDefault(data, db, mapper)
                    break
                case "panther":
                    persistPanther(data, db, mapper)
                    break
                case "pfam":
                    persistDefault(data, db, mapper)
                    break
                case "pirsf":
                    persistDefault(data, db, mapper)
                    break
                case "pirsr":
                    persistPirsr(data, db, mapper)
                    break
                case "prints":
                    persistPrints(data, db, mapper)
                    break
                case "prosite patterns":
                    persistPrositePatterns(data, db, mapper)
                    break
                case "prosite profiles":
                    persistPrositeProfiles(data, db, mapper)
                    break
                case "sfld":
                    persistSfld(data, db, mapper)
                    break
                case "signalp":
                    persistSignalp(data, db, mapper)
                    break
                case "smart":
                    persistSmart(data, db, mapper)
                    break
                case "superfamily":
                    persistSuperfamily(data, db, mapper)
                    break
                case "tmhmm":
                case "deeptmhmm":
                    persistMinimalist(data, db, mapper)
                    break
                default:
                    UnsupportedOperationException("Unknown database '${memberDb}' for protein with UPI ${upi}")
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

def persistDefault(Map analysis_data, Database db, ObjectMapper mapper) {
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
                ftmFragments(location["location-fragments"]),
                seqScore,
                seqEvalue,
                reverseHmmBounds(location["hmmBounds"]),
                location["hmmStart"].toInteger(),
                location["hmmEnd"].toInteger(),
                location["hmmLength"].toInteger(),
                location["envelopeStart"].toInteger(),
                location["envelopeEnd"].toInteger(),
                new java.math.BigDecimal(location["score"].toString()).toPlainString(),
                new java.math.BigDecimal(location["evalue"].toString()).toPlainString()
            ]
        }
    }
    db.persistDefaultMatches(values, analysis_data.matchTable)
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

def persistCdd(Map analysis_data, Database db, ObjectMapper mapper) {
    matchValues = []
    siteValues = []
    streamJsonArray(analysis_data.json.toString(), mapper) { result ->
        def (upi, match, methodAc, modelAc, seqScore, seqEvalue) = getMatch(result, mapper)
        def md5 = result.get("md5").asText()
        def seqLength = result.get("seqLength")
        match.locations.each { Map location ->
            // define Match
            matchValues << [
                analysis_data.id,
                analysis_data.name,
                null,
                null,
                upi,
                methodAc,
                modelAc,
                location["start"].toInteger(),
                location["end"].toInteger(),
                ftmFragments(location["location-fragments"]),
                seqScore,
                seqEvalue
            ]
            // define Site
            location.sites.each { Map site ->
                site.siteLocations.each { Map siteLoc ->
                    siteValues << [
                        analysis_data.id,
                        upi.substring(0, upi.length() - 5),
                        upi,
                        md5,
                        seqLength.asInt(),
                        analysis_data.name,
                        methodAc,
                        location["start"].toInteger(),
                        location["end"].toInteger(),
                        site["numLocations"].toInteger(),
                        siteLoc["residue"],
                        siteLoc["start"].toInteger(),
                        siteLoc["end"].toInteger(),
                        site["description"]
                    ]
                }
            }
        }
    }
    for (v: matchValues) {
        println "$v"
    }
    db.persistCddMatches(matchValues, analysis_data.matchTable)
    db.persistDefaultSites(siteValues, analysis_data.siteTable)
}

def persistHamap(Map analysis_data, Database db, ObjectMapper mapper) {
    values = []
    streamJsonArray(analysis_data.json.toString(), mapper) { result ->
        (upi, match, methodAc, modelAc, seqScore, seqEvalue) = getMatch(result, mapper)
        match.locations.each { Map location ->
            try {
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
                    new java.math.BigDecimal(location["score"].toString()).toPlainString(),
                    location["cigarAlignment"]
                ]
            } catch (Exception e) {
                println "e: $e --> $match"
            }
        }
    }
    db.persistHamapMatches(values, analysis_data.matchTable)
}

def persistMobiDBlite(Map analysis_data, Database db, ObjectMapper mapper) {
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
                ftmFragments(location["location-fragments"]),
                location["sequence-feature"],
            ]
        }
    }
    db.persistMobiDBliteMatches(values, analysis_data.matchTable)
}

def persistPanther(Map analysis_data, Database db, ObjectMapper mapper) {
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
                ftmFragments(location["location-fragments"]),
                seqScore,
                seqEvalue,
                reverseHmmBounds(location["hmmBounds"]),
                location["hmmStart"].toInteger(),
                location["hmmEnd"].toInteger(),
                location["hmmLength"].toInteger(),
                location["envelopeStart"].toInteger(),
                location["envelopeEnd"].toInteger(),
                new java.math.BigDecimal(location["score"].toString()).toPlainString(),
                new java.math.BigDecimal(location["evalue"].toString()).toPlainString(),
                match["ancestralNodeID"] // annotation node - doesn't seem to be in the i6 json, check if this is right
            ]
        }
    }
    db.persistPantherMatches(values, analysis_data.matchTable)
}

def persistPirsr(Map analysis_data, Database db, ObjectMapper mapper) {
    matchValues = []
    siteValues = []
    streamJsonArray(analysis_data.json.toString(), mapper) { result ->
        println "PIRSR"
        def (upi, match, methodAc, modelAc, seqScore, seqEvalue) = getMatch(result, mapper)
        println "(upi, match, methodAc) ${upi} ${methodAc}"
        def md5 = result.get("md5").asText()
        def seqLength = result.get("seqLength")
        match.locations.each { Map location ->
            println "loc: ${location}"
            // define Match
            matchValues << [
                analysis_data.id,
                analysis_data.name,
                null, null,
                upi,
                methodAc,
                modelAc,
                location["start"].toInteger(),
                location["end"].toInteger(),
                ftmFragments(location["location-fragments"]),
                seqScore,
                seqEvalue,
                reverseHmmBounds(location["hmmBounds"]),
                location["hmmStart"].toInteger(),
                location["hmmEnd"].toInteger(),
                location["hmmLength"].toInteger(),
                new java.math.BigDecimal(location["score"].toString()).toPlainString(),
                new java.math.BigDecimal(location["evalue"].toString()).toPlainString(),
                location["envelopeStart"].toInteger(),
                location["envelopeEnd"].toInteger()
            ]
            // define Site
            location.sites.each { Map site ->
                println "site: ${site}"
                println "upi.substring(0, upi.length() - 5): ${upi.substring(0, upi.length() - 5)}"
                site.siteLocations.each { Map siteLoc ->
                    println "siteLoc: ${siteLoc}"
                    siteValues << [
                        analysis_data.id,
                        upi.substring(0, upi.length() - 5),
                        upi,
                        md5,
                        seqLength.asInt(),
                        analysis_data.name,
                        methodAc,
                        location["start"].toInteger(),
                        location["end"].toInteger(),
                        site["numLocations"].toInteger(),
                        siteLoc["residue"],
                        siteLoc["start"].toInteger(),
                        siteLoc["end"].toInteger(),
                        site["description"]
                    ]
                }
            }
        }
    }
    db.persistPirsrMatch(matchValues, analysis_data.matchTable)
    db.persistDefaultSites(siteValues, analysis_data.siteTable)
}

def persistPrints(Map analysis_data, Database db, ObjectMapper mapper) {
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
                ftmFragments(location["location-fragments"]),
                seqScore,
                seqEvalue,
                reverseHmmBounds(location["hmmBounds"]),
                location["motifNumber"],
                location["pvalue"],
                match["graphscan"],
            ]
        }
    }
    db.persistPrintsMatches(values, analysis_data.matchTable)
}

def persistPrositePatterns(Map analysis_data, Database db, ObjectMapper mapper) {
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
                ftmFragments(location["location-fragments"]),
                location["level"],
                location["cigarAlignment"]
            ]
        }
    }
    db.persistPrositePatternsMatches(values, analysis_data.matchTable)
}

def persistPrositeProfiles(Map analysis_data, Database db, ObjectMapper mapper) {
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
                ftmFragments(location["location-fragments"]),
                location["cigarAlignment"]
            ]
        }
    }
    db.persistPrositeProfileMatches(values, analysis_data.matchTable)
}

def persistSfld(Map analysis_data, Database db, ObjectMapper mapper) {
    matchValues = []
    siteValues = []
    streamJsonArray(analysis_data.json.toString(), mapper) { result ->
        def (upi, match, methodAc, modelAc, seqScore, seqEvalue) = getMatch(result, mapper)
        def md5 = result.get("md5").asText()
        def seqLength = result.get("seqLength")
        match.locations.each { Map location ->
            // define Match
            matchValues << [
                analysis_data.id,
                analysis_data.name,
                null, null,
                upi,
                methodAc,
                modelAc,
                location["start"].toInteger(),
                location["end"].toInteger(),
                ftmFragments(location["location-fragments"]),
                seqScore,
                seqEvalue,
                reverseHmmBounds(location["hmmBounds"]),
                location["hmmStart"].toInteger(),
                location["hmmEnd"].toInteger(),
                location["hmmLength"].toInteger(),
                location["envelopeStart"].toInteger(),
                location["envelopeEnd"].toInteger(),
                new java.math.BigDecimal(location["score"].toString()).toPlainString(),
                new java.math.BigDecimal(location["evalue"].toString()).toPlainString()
            ]
            // define Site
            location.sites.each { Map site ->
                site.siteLocations.each { Map siteLoc ->
                    siteValues << [
                        analysis_data.id,
                        upi.substring(0, upi.length() - 5),
                        upi,
                        md5,
                        seqLength.asInt(),
                        analysis_data.name,
                        methodAc,
                        location["start"].toInteger(),
                        location["end"].toInteger(),
                        site["numLocations"].toInteger(),
                        siteLoc["residue"],
                        siteLoc["start"].toInteger(),
                        siteLoc["end"].toInteger(),
                        site["description"]
                    ]
                }
            }
        }
    }
    db.persistDefaultMatches(matchValues, analysis_data.matchTable)
    db.persistDefaultSites(siteValues, analysis_data.siteTable)
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
                ftmFragments(location["location-fragments"]),
                new java.math.BigDecimal(location["score"].toString()).toPlainString()
            ]
        }
    }
    db.persistSignalpMatches(values, analysis_data.matchTable)
}

def persistSmart(Map analysis_data, Database db, ObjectMapper mapper) {
    values = []
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
                ftmFragments(location["location-fragments"]),
                seqScore,
                seqEvalue,
                reverseHmmBounds(location["hmmBounds"]),
                location["hmmStart"].toInteger(),
                location["hmmEnd"].toInteger(),
                location["hmmLength"].toInteger(),
                new java.math.BigDecimal(location["score"].toString()).toPlainString(),
                new java.math.BigDecimal(location["evalue"].toString()).toPlainString()
            ]
        }
    }
    db.persistSmartMatches(values, analysis_data.matchTable)
}

def persistSuperfamily(Map analysis_data, Database db, ObjectMapper mapper) {
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
                ftmFragments(location["location-fragments"]),
                seqEvalue,
                location["hmmLength"].toInteger()
            ]
        }
    }
    db.persistSuperfamilyMatches(values, analysis_data.matchTable)
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
