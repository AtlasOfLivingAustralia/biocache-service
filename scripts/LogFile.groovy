@Grab('com.opencsv:opencsv:5.4')
@Grab('org.apache.ivy:ivy:2.4.0')
@Grab(group='commons-collections', module='commons-collections', version='3.2.1')

import com.opencsv.CSVReader
import com.opencsv.CSVWriter
import groovy.json.JsonSlurper;
import groovy.json.JsonOutput;

files = [
        "/tmp/nginx-logs/prod1/access.log.9",
        "/tmp/nginx-logs/prod1/access.log.10",
        "/tmp/nginx-logs/prod2/access.log.11",
        "/tmp/nginx-logs/prod2/access.log.9",
        "/tmp/nginx-logs/prod2/access.log.10",
        "/tmp/nginx-logs/prod3/access.log.11",
        "/tmp/nginx-logs/prod3/access.log.9",
        "/tmp/nginx-logs/prod3/access.log.10",
        "/tmp/nginx-logs/prod-download/access.log.8",
        "/tmp/nginx-logs/prod-download/access.log.9",
        "/tmp/nginx-logs/prod-download/access.log.10",
]

prodApiKey = "apiKey=TO_BE_REPLACED"
testApiKey = "apiKey=TO_BE_REPLACED"

Writer writer = new FileWriter("/tmp/nginx-logs/combined.log")

files.each { fileName ->

    InputStream fileStream = new FileInputStream(fileName);
    Reader decoder = new InputStreamReader(fileStream, "UTF-8");
    BufferedReader buffered = new BufferedReader(decoder);

    com.opencsv.CSVParser parser =
            new com.opencsv.CSVParserBuilder()
                    .withSeparator(' '.charAt(0))
                    .withIgnoreQuotations(true)
                    .build();

    com.opencsv.CSVReader reader = new com.opencsv.CSVReaderBuilder(buffered)
            .withCSVParser(parser)
            .build();

    formatter = new java.text.SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);

    line = reader.readNext()

    while (line){
        url = line[6]
        statusCode = line[8]
        datetime = formatter.parse(line[3].substring(1)).getTime()
        writer.write(datetime + '\t' + fileName + '\t' + statusCode + '\t' + url + '\n')
        line = reader.readNext()
    }
    writer.flush()
    reader.close()
}

writer.close()


def command = 'sort /tmp/nginx-logs/combined.log'
def proc = ['/bin/bash', '-c', command].execute()

FileWriter combinedSorted = new FileWriter("/tmp/nginx-logs/pipelines-access.log")
combinedSorted.write("params\n")
FileWriter combinedSortedSuccessOnly = new FileWriter("/tmp/nginx-logs/pipelines-access-noerrors.log")
combinedSortedSuccessOnly.write("params\n")

proc.getInputStream().withReader {reader ->
    while ((line = reader.readLine()) != null) {

        // replace API key
        parts = line.split('\t')
        url = parts[3].replaceAll(prodApiKey, testApiKey)
        if (parts[2] == "200"){
            combinedSortedSuccessOnly.write(url + '\n')
        }
        combinedSorted.write(url  + '\n')
    }
}
combinedSorted.flush()
combinedSortedSuccessOnly.flush()
combinedSorted.close()
combinedSortedSuccessOnly.close()

println("Finished")





