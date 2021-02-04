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
import spock.lang.Unroll

import static com.github.tomakehurst.wiremock.client.WireMock.*
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.*

@WebAppConfiguration
@ContextConfiguration(locations = 'classpath:springTest.xml')
class FieldMappedSolrClientSpec extends Specification {

    @Autowired
    SolrClient solrClient

    @Unroll
    def 'query: #desc'() {

        when:
        QueryResponse qr = solrClient.query(query as SolrQuery)
        println qr
        println result

        then:
        qr != null
        with qr, result

        where:
        desc | query || result
        'basic taxon_name'  | [ fields: [ 'taxon_name' ], query: 'taxon_name:*' ] || { QueryResponse resp ->
            assert resp.results.size() > 0
            assert resp.results.get(0).getFieldValue('taxon_name') == null
            assert resp.results.get(0).getFieldValue('scientificName') != null
        }
        'combination query' | [ fields: [ 'taxon_name' ], query: 'taxon_name:* AND -(common_name:"test")' ] || { QueryResponse resp ->
            assert resp.results.size() > 0
            assert resp.results.get(0).getFieldValue('taxon_name') == null
            assert resp.results.get(0).getFieldValue('scientificName') != null
        }
        'deprecated field'  | [ fields: [ 'aust_conservation' ], query: 'aust_conservation:*' ] || { QueryResponse resp ->
            assert resp.results.size() == 0
        }
        'deprecated field (string)' | [ fields: [ 'aust_conservation' ], query: 'aust_conservation:""' ] || { QueryResponse resp ->
            assert resp.results.size() == 0
        }
        'deprecated field (number)' | [ fields: [ 'aust_conservation' ], query: 'aust_conservation:100' ] || { QueryResponse resp ->
            assert resp.results.size() == 0
        }
        'deprecated field (range)' | [ fields: [ 'aust_conservation' ], query: 'aust_conservation:[* TO 100]' ] || { QueryResponse resp ->
            assert resp.results.size() == 0
        }
        'deprecated field combination' | [ fields: [ 'aust_conservation' ], query: '*:* aust_conservation:*' ] || { QueryResponse resp ->
            assert resp.results.size() > 0
        }
        'deprecated field combination' | [ fields: [ 'aust_conservation' ], query: '*:* AND aust_conservation:*' ] || { QueryResponse resp ->
            assert resp.results.size() == 0
        }
        'basic filter query' | [ fields: [ 'taxon_name' ], query: '*:*', filterQueries: [ 'taxon_name:*', 'scientificName:*' ] ] || { QueryResponse resp ->
            assert resp.results.size() > 0
            assert resp.results.get(0).getFieldValue('taxon_name') == null
            assert resp.results.get(0).getFieldValue('scientificName') != null
        }
//        'facet query' | [ query: '*:*', addFacetField: ('scientificName') ] || { QueryResponse resp ->
//            assert resp.facetFields.get(0).name == 'scientificName'
//        }
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
