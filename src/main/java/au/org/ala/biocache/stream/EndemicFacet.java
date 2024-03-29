package au.org.ala.biocache.stream;

import au.org.ala.biocache.dto.FieldResultDTO;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.io.Tuple;

import java.util.List;

public class EndemicFacet implements ProcessInterface {

    private final static Logger logger = Logger.getLogger(EndemicFacet.class);

    List<FieldResultDTO> output;
    String facetName;

    public EndemicFacet(List<FieldResultDTO> output, String facetName) {
        this.output = output;
        this.facetName = facetName;
    }

    public boolean process(Tuple tuple) {
        try {
            if (tuple != null) {
                long entryCount = tuple.getLong("count(*)");

                if (entryCount > 0) {
                    String value = tuple.getString(facetName);

                    output.add(new FieldResultDTO(value, value, entryCount));
                }
            }

            return true;
        } catch (IllegalArgumentException e) {
            logger.warn("Failed to convert tuple to FieldResultDTO: " + e.getMessage());
            return false;
        }
    }

    public boolean flush() {
        return true;
    }
}