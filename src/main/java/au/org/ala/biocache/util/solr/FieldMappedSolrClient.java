package au.org.ala.biocache.util.solr;

import org.apache.solr.client.solrj.*;
import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.client.solrj.impl.StreamingBinaryResponseParser;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class FieldMappedSolrClient extends SolrClient {

//    final FieldMappingUtil.Builder fieldMappingUtilBuilder;
    final FieldMappingUtil fieldMappingUtil;
    final SolrClient delegate;

    public FieldMappedSolrClient(FieldMappingUtil fieldMappingUtil, SolrClient delegate) {

        this.fieldMappingUtil = fieldMappingUtil;
        this.delegate = delegate;
    }

    public boolean isInstanceOf(Class clazz) {
        return clazz.isAssignableFrom(delegate.getClass());
    }

    @Override
    public UpdateResponse add(Collection<SolrInputDocument> docs) throws SolrServerException, IOException {
        return delegate.add(docs);
    }

    @Override
    public UpdateResponse add(String collection, Collection<SolrInputDocument> docs, int commitWithinMs) throws SolrServerException, IOException {
        return delegate.add(collection, docs, commitWithinMs);
    }

    @Override
    public UpdateResponse add(Collection<SolrInputDocument> docs, int commitWithinMs) throws SolrServerException, IOException {
        return delegate.add(docs, commitWithinMs);
    }

    @Override
    public UpdateResponse add(String collection, SolrInputDocument doc) throws SolrServerException, IOException {
        return delegate.add(collection, doc);
    }

    @Override
    public UpdateResponse add(SolrInputDocument doc) throws SolrServerException, IOException {
        return delegate.add(doc);
    }

    @Override
    public UpdateResponse add(String collection, SolrInputDocument doc, int commitWithinMs) throws SolrServerException, IOException {
        return delegate.add(collection, doc, commitWithinMs);
    }

    @Override
    public UpdateResponse add(SolrInputDocument doc, int commitWithinMs) throws SolrServerException, IOException {
        return delegate.add(doc, commitWithinMs);
    }

    @Override
    public UpdateResponse add(String collection, Iterator<SolrInputDocument> docIterator) throws SolrServerException, IOException {
        return delegate.add(collection, docIterator);
    }

    @Override
    public UpdateResponse add(Iterator<SolrInputDocument> docIterator) throws SolrServerException, IOException {
        return delegate.add(docIterator);
    }

    @Override
    public UpdateResponse addBean(String collection, Object obj) throws IOException, SolrServerException {
        return delegate.addBean(collection, obj);
    }

    @Override
    public UpdateResponse addBean(Object obj) throws IOException, SolrServerException {
        return delegate.addBean(obj);
    }

    @Override
    public UpdateResponse addBean(String collection, Object obj, int commitWithinMs) throws IOException, SolrServerException {
        return delegate.addBean(collection, obj, commitWithinMs);
    }

    @Override
    public UpdateResponse addBean(Object obj, int commitWithinMs) throws IOException, SolrServerException {
        return delegate.addBean(obj, commitWithinMs);
    }

    @Override
    public UpdateResponse addBeans(String collection, Collection<?> beans) throws SolrServerException, IOException {
        return delegate.addBeans(collection, beans);
    }

    @Override
    public UpdateResponse addBeans(Collection<?> beans) throws SolrServerException, IOException {
        return delegate.addBeans(beans);
    }

    @Override
    public UpdateResponse addBeans(String collection, Collection<?> beans, int commitWithinMs) throws SolrServerException, IOException {
        return delegate.addBeans(collection, beans, commitWithinMs);
    }

    @Override
    public UpdateResponse addBeans(Collection<?> beans, int commitWithinMs) throws SolrServerException, IOException {
        return delegate.addBeans(beans, commitWithinMs);
    }

    @Override
    public UpdateResponse addBeans(String collection, Iterator<?> beanIterator) throws SolrServerException, IOException {
        return delegate.addBeans(collection, beanIterator);
    }

    @Override
    public UpdateResponse addBeans(Iterator<?> beanIterator) throws SolrServerException, IOException {
        return delegate.addBeans(beanIterator);
    }

    @Override
    public UpdateResponse commit(String collection) throws SolrServerException, IOException {
        return delegate.commit(collection);
    }

    @Override
    public UpdateResponse commit() throws SolrServerException, IOException {
        return delegate.commit();
    }

    @Override
    public UpdateResponse commit(String collection, boolean waitFlush, boolean waitSearcher) throws SolrServerException, IOException {
        return delegate.commit(collection, waitFlush, waitSearcher);
    }

    @Override
    public UpdateResponse commit(boolean waitFlush, boolean waitSearcher) throws SolrServerException, IOException {
        return delegate.commit(waitFlush, waitSearcher);
    }

    @Override
    public UpdateResponse commit(String collection, boolean waitFlush, boolean waitSearcher, boolean softCommit) throws SolrServerException, IOException {
        return delegate.commit(collection, waitFlush, waitSearcher, softCommit);
    }

    @Override
    public UpdateResponse commit(boolean waitFlush, boolean waitSearcher, boolean softCommit) throws SolrServerException, IOException {
        return delegate.commit(waitFlush, waitSearcher, softCommit);
    }

    @Override
    public UpdateResponse optimize(String collection) throws SolrServerException, IOException {
        return delegate.optimize(collection);
    }

    @Override
    public UpdateResponse optimize() throws SolrServerException, IOException {
        return delegate.optimize();
    }

    @Override
    public UpdateResponse optimize(String collection, boolean waitFlush, boolean waitSearcher) throws SolrServerException, IOException {
        return delegate.optimize(collection, waitFlush, waitSearcher);
    }

    @Override
    public UpdateResponse optimize(boolean waitFlush, boolean waitSearcher) throws SolrServerException, IOException {
        return delegate.optimize(waitFlush, waitSearcher);
    }

    @Override
    public UpdateResponse optimize(String collection, boolean waitFlush, boolean waitSearcher, int maxSegments) throws SolrServerException, IOException {
        return delegate.optimize(collection, waitFlush, waitSearcher, maxSegments);
    }

    @Override
    public UpdateResponse optimize(boolean waitFlush, boolean waitSearcher, int maxSegments) throws SolrServerException, IOException {
        return delegate.optimize(waitFlush, waitSearcher, maxSegments);
    }

    @Override
    public UpdateResponse rollback(String collection) throws SolrServerException, IOException {
        return delegate.rollback(collection);
    }

    @Override
    public UpdateResponse rollback() throws SolrServerException, IOException {
        return delegate.rollback();
    }

    @Override
    public UpdateResponse deleteById(String collection, String id) throws SolrServerException, IOException {
        return delegate.deleteById(collection, id);
    }

    @Override
    public UpdateResponse deleteById(String id) throws SolrServerException, IOException {
        return delegate.deleteById(id);
    }

    @Override
    public UpdateResponse deleteById(String collection, String id, int commitWithinMs) throws SolrServerException, IOException {
        return delegate.deleteById(collection, id, commitWithinMs);
    }

    @Override
    public UpdateResponse deleteById(String id, int commitWithinMs) throws SolrServerException, IOException {
        return delegate.deleteById(id, commitWithinMs);
    }

    @Override
    public UpdateResponse deleteById(String collection, List<String> ids) throws SolrServerException, IOException {
        return delegate.deleteById(collection, ids);
    }

    @Override
    public UpdateResponse deleteById(List<String> ids) throws SolrServerException, IOException {
        return delegate.deleteById(ids);
    }

    @Override
    public UpdateResponse deleteById(String collection, List<String> ids, int commitWithinMs) throws SolrServerException, IOException {
        return delegate.deleteById(collection, ids, commitWithinMs);
    }

    @Override
    public UpdateResponse deleteById(List<String> ids, int commitWithinMs) throws SolrServerException, IOException {
        return delegate.deleteById(ids, commitWithinMs);
    }

    @Override
    public UpdateResponse deleteByQuery(String collection, String query) throws SolrServerException, IOException {
        return delegate.deleteByQuery(collection, query);
    }

    @Override
    public UpdateResponse deleteByQuery(String query) throws SolrServerException, IOException {
        return delegate.deleteByQuery(query);
    }

    @Override
    public UpdateResponse deleteByQuery(String collection, String query, int commitWithinMs) throws SolrServerException, IOException {
        return delegate.deleteByQuery(collection, query, commitWithinMs);
    }

    @Override
    public UpdateResponse deleteByQuery(String query, int commitWithinMs) throws SolrServerException, IOException {
        return delegate.deleteByQuery(query, commitWithinMs);
    }

    @Override
    public SolrPingResponse ping() throws SolrServerException, IOException {
        return delegate.ping();
    }

    @Override
    public QueryResponse query(String collection, SolrParams params) throws SolrServerException, IOException {

        FieldMappedSolrParams translatedParams = new FieldMappedSolrParams(fieldMappingUtil, params);

        QueryResponse queryResponse = delegate.query(collection, translatedParams);

        return new FieldMappedQueryResponse(translatedParams, queryResponse);
    }

    @Override
    public QueryResponse query(SolrParams params) throws SolrServerException, IOException {
        return this.query((String)null, (SolrParams)params);
    }

    @Override
    public QueryResponse query(String collection, SolrParams params, SolrRequest.METHOD method) throws SolrServerException, IOException {

        FieldMappedSolrParams translatedParams = new FieldMappedSolrParams(fieldMappingUtil, params);

        QueryResponse queryResponse = delegate.query(collection, translatedParams, method);

        return new FieldMappedQueryResponse(translatedParams, queryResponse);
    }

    @Override
    public QueryResponse query(SolrParams params, SolrRequest.METHOD method) throws SolrServerException, IOException {
        return this.query((String)null, params, method);
    }

    @Override
    public QueryResponse queryAndStreamResponse(String collection, SolrParams params, StreamingResponseCallback callback) throws SolrServerException, IOException {

        FieldMappedSolrParams translatedParams = new FieldMappedSolrParams(fieldMappingUtil, params);

        QueryResponse queryResponse = delegate.queryAndStreamResponse(collection, translatedParams, callback);

        return new FieldMappedQueryResponse(translatedParams, queryResponse);
    }

    public QueryResponse queryAndStreamResponse(SolrParams params, StreamingResponseCallback callback) throws SolrServerException, IOException {
        return this.queryAndStreamResponse((String)null, params, callback);
    }

    @Override
    public SolrDocument getById(String collection, String id) throws SolrServerException, IOException {
        return delegate.getById(collection, id);
    }

    @Override
    public SolrDocument getById(String id) throws SolrServerException, IOException {
        return delegate.getById(id);
    }

    @Override
    public SolrDocument getById(String collection, String id, SolrParams params) throws SolrServerException, IOException {
        return delegate.getById(collection, id, params);
    }

    @Override
    public SolrDocument getById(String id, SolrParams params) throws SolrServerException, IOException {
        return delegate.getById(id, params);
    }

    @Override
    public SolrDocumentList getById(String collection, Collection<String> ids) throws SolrServerException, IOException {
        return delegate.getById(collection, ids);
    }

    @Override
    public SolrDocumentList getById(Collection<String> ids) throws SolrServerException, IOException {
        return delegate.getById(ids);
    }

    @Override
    public SolrDocumentList getById(String collection, Collection<String> ids, SolrParams params) throws SolrServerException, IOException {
        return delegate.getById(collection, ids, params);
    }

    @Override
    public SolrDocumentList getById(Collection<String> ids, SolrParams params) throws SolrServerException, IOException {
        return delegate.getById(ids, params);
    }

    @Override
    public NamedList<Object> request(SolrRequest solrRequest, String s) throws SolrServerException, IOException {
        return delegate.request(solrRequest, s);
    }

    @Override
    public DocumentObjectBinder getBinder() {
        return delegate.getBinder();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public UpdateResponse add(String collection, Collection<SolrInputDocument> docs) throws SolrServerException, IOException {
        return delegate.add(collection, docs);
    }
}
