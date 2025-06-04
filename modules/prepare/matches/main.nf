import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import groovy.json.JsonOutput

process SEPARATE_MEMBER_DBS {
    /* Prepare the InterProScan6 output files for batch inserting matches into the InterProScan database.
    The output file from i6 can be extremely large, and requires too much memory to load into the memory all at once.
    Also, an i6 output file can contain multiple different applications, so streaming the file would mean inserting
    one match at a time. To batch insert matches later on, separate the matches into their respective member dbs. */
    input:
    tuple val(job), val(matches_path)

    output:
    val job

    exec:
    ObjectMapper mapper = new ObjectMapper()
    Map outputFiles = [:]

    streamJsonResults(matches_path.toString(), mapper) { result ->  // stream the file to reduce memory requirements
        def md5 = result.get("md5").asText()
        def seqLength = result.get("sequence").asText().size()
        def upi = result.get("xref")[0].get("id").asText()
        result.get("matches").each { match ->
            def library = match.get("signature").get("signatureLibraryRelease").get("library").asText().toLowerCase()
            if (!outputFiles.containsKey(library)) {
                def filePath = task.workDir.resolve("${library}.json").toString()
                def file = new File(filePath)
                outputFiles[library] = [writer: file.newWriter(), file: filePath]
                outputFiles[library].writer.write("[\n")
            } else {
                outputFiles[library].writer.write(",\n")
            }
            Map matchMap = mapper.convertValue(match, Map)  // so JsonOutput can write the map correctly
            Map upiWithMatch = [upi: upi, md5: md5, seqLength: seqLength, match: matchMap]
            outputFiles[library].writer.write(JsonOutput.toJson(upiWithMatch))
        }
    }


    // Add closing brackets, close all files, and add to the job
    outputFiles.each { library, map ->
        map.writer.write("\n]")
        map.writer.close()
        job.applications[library]["json"] = map.file
    }
}

def streamJsonResults(String filePath, ObjectMapper mapper, Closure closure) {
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