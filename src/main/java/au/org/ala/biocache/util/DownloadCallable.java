package au.org.ala.biocache.util;

import au.org.ala.biocache.dao.IndexDAO;
import au.org.ala.biocache.dao.SearchDAOImpl;
import au.org.ala.biocache.stream.ProcessDownload;
import org.apache.solr.client.solrj.SolrQuery;

import java.util.List;
import java.util.concurrent.Callable;

public class DownloadCallable implements Callable {

    List<SolrQuery> queries;
    IndexDAO indexDAO;
    ProcessDownload procDownload;

    public DownloadCallable(List<SolrQuery> queries, IndexDAO indexDAO, ProcessDownload procDownload) {
        this.queries = queries;
        this.procDownload = procDownload;
        this.indexDAO = indexDAO;
    }

    @Override
    public Object call() throws Exception {
        // iterate over queries
        for (SolrQuery query : queries) {
            indexDAO.streamingQuery(query, procDownload, null);
        }

        return null;
    }
}
