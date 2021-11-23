package au.org.ala.biocache.stream;

import au.com.bytecode.opencsv.CSVWriter;
import au.org.ala.biocache.dto.SpatialSearchRequestDTO;
import au.org.ala.biocache.util.solr.FieldMappingUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.io.Tuple;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StreamAsCSV implements ProcessInterface {

    private final static Logger logger = Logger.getLogger(StreamAsCSV.class);

    FieldMappingUtil fieldMappingUtil;
    CSVWriter csvWriter;
    SpatialSearchRequestDTO requestParams;

    String[] row;

    byte[] bComma;
    byte[] bNewLine;
    byte[] bDblQuote;

    int count = 0;

    //header field identification
    List<String> header = new ArrayList<String>();
    List<String> responseHeader = new ArrayList<String>();

    public StreamAsCSV(FieldMappingUtil fieldMappingUtil, OutputStream stream, SpatialSearchRequestDTO requestParams) {
        this.fieldMappingUtil = fieldMappingUtil;
        this.csvWriter = new CSVWriter(new OutputStreamWriter(stream));
        this.requestParams = requestParams;

        try {
            bComma = ",".getBytes(StandardCharsets.UTF_8);
            bNewLine = "\n".getBytes(StandardCharsets.UTF_8);
            bDblQuote = "\"".getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    public boolean process(Tuple tuple) {
        //write header when writing the first record
        if (count == 0) {
            if (StringUtils.isNotEmpty(requestParams.getFl())) {
                header = Arrays.asList(requestParams.getFl().split(","));
                responseHeader = new ArrayList<>(header.size());
                for (int i=0;i<header.size();i++) {
                    responseHeader.add(fieldMappingUtil.translateFieldName(header.get(i)));
                }
            } else {
                header = new ArrayList<>(tuple.getMap().keySet());
            }
            csvWriter.writeNext(header.toArray(new String[0]));
            row = new String[header.size()];
        }

        count++;

        //write record
        for (int j = 0; j < header.size(); j++) {
            String field = responseHeader.get(j);
            row[j] = format(field, tuple.get(field));
        }

        csvWriter.writeNext(row);

        return true;
    }

    String format(String field, Object item) {
        if (item == null) return "";

        String formatted = null;
        if (item instanceof List) {
            if (requestParams.getIncludeMultivalues()) {
                for (Object o : (List) item) {
                    if (!formatted.isEmpty()) {
                        formatted += "|";
                    }
                    formatted += fieldMappingUtil.translateFieldValue(field, String.valueOf(o));
                }
                formatted = StringUtils.join((List) item, '|');
            } else if (((List) item).size() > 0) {
                formatted = fieldMappingUtil.translateFieldValue(field, String.valueOf(((List) item).get(0)));
            }
        } else {
            formatted = fieldMappingUtil.translateFieldValue(field, String.valueOf(item));
        }
        if (StringUtils.isEmpty(formatted)) {
            return "";
        } else {
            return formatted;
        }
    }

    public boolean flush() {
        try {
            csvWriter.flush();
        } catch (IOException e) {
            logger.error(e);
        }

        return true;
    }
}