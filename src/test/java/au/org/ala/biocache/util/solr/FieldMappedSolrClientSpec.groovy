package au.org.ala.biocache.util.solr

import au.org.ala.biocache.index.IndexDAO
import au.org.ala.biocache.index.SolrIndexDAO
import com.github.tomakehurst.wiremock.WireMockServer
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient
import org.apache.solr.client.solrj.impl.HttpSolrClient
import org.apache.solr.client.solrj.response.QueryResponse
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.web.WebAppConfiguration
import spock.lang.Shared
import spock.lang.Specification

import static com.github.tomakehurst.wiremock.client.WireMock.*
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.*

@WebAppConfiguration
@ContextConfiguration(locations = 'classpath:springTest.xml')
class FieldMappedSolrClientSpec extends Specification {

    @Autowired
    SolrClient solrClient

//    @Autowired
//    FieldMappingUtil fieldMappingUtil

    def setupSpec() {

    }


    def 'basic query'() {

        setup:
        SolrQuery query = new SolrQuery()
        query.fields = [ 'taxon_name' ]
        query.query = 'taxon_name:*'

        when:
        QueryResponse qr = solrClient.query(query)
        println qr

        then:
        qr != null
        qr.results.get(0).getFieldValue('taxon_name') == null
        qr.results.get(0).getFieldValue('scientificName') != null
    }

    def 'query'() {

        setup:
        SolrQuery query = new SolrQuery()
        query.fields = [ 'taxon_name' ]
        query.query = 'taxon_name:* AND -(common_name:"test")'

        when:
        QueryResponse qr = solrClient.query(query)
        println qr

        then:
        qr != null
        qr.results.get(0).getFieldValue('taxon_name') == null
        qr.results.get(0).getFieldValue('scientificName') != null
    }

    def 'basic filterQueries'() {

        setup:
        SolrQuery query = new SolrQuery()
        query.fields = [ 'taxon_name' ]
        query.query = '*:*'
        query.filterQueries = [ 'taxon_name:*', 'scientificName:*' ]

        when:
        QueryResponse qr = solrClient.query(query)

        println qr

        then:
        qr != null
        qr.results.get(0).getFieldValue('taxon_name') == null
        qr.results.get(0).getFieldValue('scientificName') != null
    }

    def 'facet query'() {

        setup:
        SolrQuery query = new SolrQuery()
        query.query = '*:*'
        query.fields = [ 'id' ]
        query.addFacetField('scientificName')

        when:
        QueryResponse qr = solrClient.query(query)

        then:
        qr != null
        qr.facetFields?.size() == 1
        qr.facetFields.get(0).name == 'scientificName'

        when:
        query.facet = false
        query.addFacetField('taxon_name')
        qr = solrClient.query(query)

        then:
        qr != null
        qr.facetFields?.size() == 1
        qr.facetFields.get(0).name == 'taxon_name'
    }
}
