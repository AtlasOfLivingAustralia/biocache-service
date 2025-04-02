package au.org.ala.biocache.stream;

import au.com.bytecode.opencsv.CSVWriter;
import au.org.ala.biocache.dao.SearchDAOImpl;
import au.org.ala.biocache.dto.SpatialSearchRequestDTO;
import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import au.org.ala.biocache.dto.TaxaCountDTO;
import au.org.ala.biocache.util.SearchUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

public class StreamTaxaAsCSV extends StreamTaxaCount {

    private final static Logger logger = Logger.getLogger(StreamTaxaAsCSV.class);

    private final String NULL_NAME = "Unknown";

    CSVWriter csvWriter;

    public StreamTaxaAsCSV(SearchDAOImpl searchDAO, SearchUtils searchUtils, SpatialSearchRequestDTO request, Boolean includeRank, OutputStream outputStream) {
        super(searchDAO, searchUtils, request, includeRank, outputStream);
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
            csvWriter = new CSVWriter(new OutputStreamWriter(outputStream));
        } catch (Exception e) {
            logger.error("cannot init CSV output");
        }

        // header
        csvWriter.writeNext(new String[]{"Family", "Scientific name", "Common name", "Taxon rank", "LSID", "# Occurrences"});
    }

    void write(TaxaCountDTO d) {
        String family = d.getFamily();
        String name = d.getName();
        String commonName = d.getCommonName();
        String guid = d.getGuid();
        String rank = d.getRank();

        if (family == null) {
            family = "";
        }
        if (name == null) {
            name = "";
        }
        if (commonName == null) {
            commonName = "";
        }

        if (d.getGuid() == null) {
            //when guid is empty name contains name_lsid value.
            if (d.getName() != null) {
                //parse name
                String[] nameLsid = d.getName().split("\\|");
                if (nameLsid.length >= 2) {
                    name = nameLsid[0];
                    guid = nameLsid[1];
                    rank = "scientific name";

                    if (nameLsid.length >= 3) {
                        commonName = nameLsid[2];
                    }
                } else {
                    name = NULL_NAME;
                }
            }
        }
        if (d.getCount() != null && guid != null) {
            csvWriter.writeNext(new String[] {family, name, commonName, rank, guid, d.getCount().toString() });
        }
    }
}
