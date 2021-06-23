    @Grab('com.opencsv:opencsv:5.4')
    @Grab(group='commons-collections', module='commons-collections', version='3.2.1')
    @Grab(group='org.codehaus.groovy', module='groovy-json', version='3.0.0')
    import com.opencsv.CSVReader
    import com.opencsv.CSVWriter
    import groovy.json.JsonSlurper;
    import groovy.json.JsonOutput;

    class Qid {
       String rowKey
       String q
       String displayString
       String wkt
       double[] bbox
       Long lastUse
       String[] fqs
       Long maxAge
       String source
    }

    InputStream fileStream = new FileInputStream("/data/tmp/qid_dump.cql");
    OutputStream fileOutStream = new FileOutputStream("/data/tmp/qid.csv");

    Reader decoder = new InputStreamReader(fileStream, "UTF-8");
    BufferedReader buffered = new BufferedReader(decoder);

    com.opencsv.CSVParser parser =
            new com.opencsv.CSVParserBuilder()
                    .withSeparator('\t'.charAt(0))
                    .withIgnoreQuotations(true)
                    .build();

    com.opencsv.CSVReader reader = new com.opencsv.CSVReaderBuilder(buffered)
            .withCSVParser(parser)
            .build();

    line = reader.readNext()

    println("Headers = " + line)

    line = reader.readNext()
    count = 0

    jsonSlurper = new JsonSlurper()

    def generator = new groovy.json.JsonGenerator.Options()
            .excludeNulls()
            .build()

    boolean isCollectionOrArray(object) {
        [Collection, Object[]].any { it.isAssignableFrom(object.getClass()) }
    }

    while (line){

        //Headers = [rowkey, bbox, displayString, fqs, lastUse, maxAge, q, source, wkt]
        Qid qid = new Qid()

        qid.rowKey = line[0]
        qid.bbox = line[1] && line[1] != 'null' && line[1] != '"null:' ? jsonSlurper.parseText(line[1]) : null
        qid.displayString = line[2] ? line[2].replaceAll('"', '\\"') : null
        qid.fqs = line[3] && line[3] != 'null' && line[3] != '"null:' ? jsonSlurper.parseText(line[3]) : null
        qid.lastUse = line[4] ? Long.parseLong(line[4]): null
        qid.maxAge = line[5] ? Long.parseLong(line[5]): null
        qid.q = line[6] ? line[6].replaceAll('"', '\\"') : null
        qid.source = line[7] ? line[7] : null
        qid.wkt = line[8] ? line[8] : null

        // fqs can contain empty fq which causes generator.toJson() crash
        // for example in prod, qid 1495081066189 has fqs = ["","longitude:[-180 TO 180]","latitude:[-90 TO 90]"]
        if (qid.fqs) {
            qid.fqs = qid.fqs.findAll{it.length() > 0} ?: null
        }

        if (qid.fqs){
            if (qid.fqs){
                qid.fqs = qid.fqs.collect { it.replace('"', '\\"') }
            } else {
                qid.fqs = qid.fqs.replaceAll('"', '\\"')
            }
        }

        def json = generator.toJson(qid)
        def output = (qid.rowKey + "\t" + json + "\n").getBytes()

        fileOutStream.write(output, 0, output.length)

        line = reader.readNext()
        count ++
    }

    fileStream.close()

    fileOutStream.flush()
    fileOutStream.close()

