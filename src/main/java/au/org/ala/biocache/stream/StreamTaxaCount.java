package au.org.ala.biocache.stream;

import au.org.ala.biocache.dao.SearchDAOImpl;
import au.org.ala.biocache.dto.SpatialSearchRequestDTO;
import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import au.org.ala.biocache.dto.TaxaCountDTO;
import au.org.ala.biocache.util.SearchUtils;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.io.Tuple;

import java.io.IOException;
import java.io.OutputStream;
import java.util.regex.Pattern;

import static au.org.ala.biocache.dto.OccurrenceIndex.COMMON_NAME_AND_LSID;
import static au.org.ala.biocache.dto.OccurrenceIndex.NAMES_AND_LSID;

public class StreamTaxaCount implements ProcessInterface {

    private final static Logger logger = Logger.getLogger(StreamTaxaCount.class);

    SearchDAOImpl searchDAO;
    SearchUtils searchUtils;
    SpatialSearchRequestDTO request;
    OutputStream outputStream;
    boolean isNamesAndLSID;
    boolean isCommonNameAndLSID;
    Pattern pattern = java.util.regex.Pattern.compile("\\|");
    JsonGenerator jsonGenerator;
    int recordCount = 0;


    public StreamTaxaCount(SearchDAOImpl searchDAO, SearchUtils searchUtils, SpatialSearchRequestDTO request, OutputStream outputStream) {
        this.request = request;
        this.searchDAO = searchDAO;
        this.searchUtils = searchUtils;
        this.outputStream = outputStream;

        isNamesAndLSID = NAMES_AND_LSID.equals(request.getFacets()[0]);
        isCommonNameAndLSID = COMMON_NAME_AND_LSID.equals(request.getFacets()[0]);

        initWriter();
    }

    public boolean process(Tuple tuple) {
        if (request.getStart() == null || recordCount >= request.getStart()) {

            String name = null;
            Long count = 0L;
            for (Object value : tuple.getMap().values()) {
                if (value instanceof String) {
                    name = (String) value;
                } else {
                    count = (Long) value;
                }
            }

            try {
                TaxaCountDTO tcDTO = null;
                if (isNamesAndLSID) {
                    String[] values = pattern.split(name, 5);

                    if (values.length >= 5) {
                        if (!"||||".equals(name)) {
                            tcDTO = new TaxaCountDTO(values[0], count);
                            tcDTO.setGuid(StringUtils.trimToNull(values[1]));
                            tcDTO.setCommonName("null".equals(values[2]) ? "" : values[2]);
                            tcDTO.setKingdom(values[3]);
                            tcDTO.setFamily(values[4]);
                            if (StringUtils.isNotEmpty(tcDTO.getGuid()))
                                tcDTO.setRank(searchUtils.getTaxonSearch(tcDTO.getGuid())[1].split(":")[0]);
                        }
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("The values length: " + values.length + " :" + name);
                        }
                        tcDTO = new TaxaCountDTO(name, count);
                    }

                    if (tcDTO != null && tcDTO.getCount() > 0)
                        write(tcDTO);
                } else if (isCommonNameAndLSID) {
                    String[] values = pattern.split(name, 6);

                    if (values.length >= 5) {
                        if (!"|||||".equals(name)) {
                            tcDTO = new TaxaCountDTO(values[1], count);
                            tcDTO.setGuid(StringUtils.trimToNull(values[2]));
                            tcDTO.setCommonName("null".equals(values[0]) ? "" : values[0]);
                            //cater for the bug of extra vernacular name in the result
                            tcDTO.setKingdom(values[values.length - 2]);
                            tcDTO.setFamily(values[values.length - 1]);
                            if (StringUtils.isNotEmpty(tcDTO.getGuid()))
                                tcDTO.setRank(searchUtils.getTaxonSearch(tcDTO.getGuid())[1].split(":")[0]);
                        }
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("The values length: " + values.length + " :" + name);
                        }
                        tcDTO = new TaxaCountDTO(name, count);
                    }

                    if (tcDTO != null && tcDTO.getCount() > 0) {
                        write(tcDTO);
                    }
                }
            } catch (Exception e) {
                logger.error(e);
            }
        }

        recordCount++;
        return true;
    }

    public boolean flush() {
        try {
            jsonGenerator.writeEndArray();
            jsonGenerator.flush();
        } catch (Exception e) {
            logger.error("cannot finish writing JSON", e);
        }

        return true;
    }

    void initWriter() {
        try {
            JsonFactory jsonFactory = new JsonFactory();
            jsonGenerator = jsonFactory.createGenerator(outputStream, JsonEncoding.UTF8);
            jsonGenerator.setCodec(new ObjectMapper());

            // start output array
            jsonGenerator.writeStartArray();
        } catch (Exception e) {
            logger.error("cannot output JSON", e);
        }
    }
    void write(TaxaCountDTO d) throws IOException {
        jsonGenerator.writeObject(d);
    }
}
