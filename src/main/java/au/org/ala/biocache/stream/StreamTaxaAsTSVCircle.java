package au.org.ala.biocache.stream;

import au.com.bytecode.opencsv.CSVWriter;
import au.org.ala.biocache.dao.SearchDAOImpl;
import au.org.ala.biocache.dto.SpatialSearchRequestDTO;
import au.org.ala.biocache.dto.TaxaCountDTO;
import au.org.ala.biocache.util.SearchUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

public class StreamTaxaAsTSVCircle extends StreamTaxaCount {

    private final static Logger logger = Logger.getLogger(StreamTaxaAsTSVCircle.class);

    CSVWriter csvWriter;

    public StreamTaxaAsTSVCircle(SearchDAOImpl searchDAO, SearchUtils searchUtils, SpatialSearchRequestDTO request, OutputStream outputStream) {
        super(searchDAO, searchUtils, request, outputStream);
    }

    public boolean flush() {
        try {
            csvWriter.flush();
        } catch (IOException e) {
            logger.error(e);
        }

        return true;
    }

    void initWriter() {
        try {
            csvWriter = new CSVWriter(new OutputStreamWriter(outputStream), '\t', '"');
        } catch (Exception e) {
            logger.error("cannot init CSV output");
        }

        // header
        csvWriter.writeNext(new String[]{"Taxon ID",
                "Kingdom",
                "Family",
                "Scientific name",
                "Common name",
                "Record count"});
    }

    void write(TaxaCountDTO d) {
        String[] record = new String[]{
                d.getGuid(),
                d.getKingdom(),
                d.getFamily(),
                d.getName(),
                d.getCommonName(),
                d.getCount().toString()
        };

        csvWriter.writeNext(record);
    }
}
