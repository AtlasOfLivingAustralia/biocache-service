package org.apache.solr.client.solrj.io.stream;

import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;

import java.io.IOException;
import java.util.List;

/**
 * Functionality of CloudSolrStream
 * - remove assert of q, fl and sort
 * - support 'distrib=true' to execute the request on a single SOLR instance
 */
public class AlaCloudSolrStream extends CloudSolrStream {

    public AlaCloudSolrStream(String zkHost, String collectionName, SolrParams params) throws IOException {
        this.init(zkHost, collectionName, params);
    }

    void init(String zkHost, String collectionName, SolrParams params) throws IOException {
        this.zkHost = zkHost;
        this.collection = collectionName;
        this.params = new ModifiableSolrParams(params);

        // super.init constructs this.comp when there is one request per shard, e.g. /export and /select requests
        if (this.params.get("distrib", "false").equals("false")) {
            super.init(collectionName, zkHost, params);
        }
    }

    @Override
    protected void constructStreams() throws IOException {

        if (this.params.get("distrib", "false").equals("false")) {
            // default request, one per shard
            super.constructStreams();
        } else {
            // one request to the collection
            constructStream();
        }
    }

    void constructStream() throws IOException {
        List<String> shardUrls = getShards(this.zkHost, this.collection, this.streamContext, new ModifiableSolrParams());

        // only add the request to the first shard
        SolrStream solrStream = new SolrStream(shardUrls.get(0), this.params);
        solrStream.setStreamContext(this.streamContext);
        this.solrStreams.add(solrStream);
    }
}
