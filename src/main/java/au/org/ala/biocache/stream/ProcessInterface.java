package au.org.ala.biocache.stream;

import org.apache.solr.client.solrj.io.Tuple;

public interface ProcessInterface {
    boolean process(Tuple t);

    boolean flush();
}