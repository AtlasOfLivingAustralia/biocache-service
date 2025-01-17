package au.org.ala.biocache.util.solr

import au.org.ala.biocache.dao.IndexDAO
import au.org.ala.biocache.util.SolrUtils
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.response.QueryResponse
import org.junit.BeforeClass
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.web.WebAppConfiguration
import spock.lang.Specification
import spock.lang.Unroll

@WebAppConfiguration
@ContextConfiguration(locations = 'classpath:springTest.xml')
@TestPropertySource(locations = "classpath:biocache-test-config.properties")
class FieldMappedSolrClientSpecIT extends Specification {

    static {
        System.setProperty("biocache.config", System.getProperty("user.dir") + "/src/test/resources/biocache-test-config.properties");
    }

    @Autowired
    IndexDAO indexDAO

    def setupSpec() {
        SolrUtils.setupIndex()
        SolrUtils.loadTestData()
    }

    @Unroll
    def 'query: #desc'() {

        when:
        QueryResponse qr = indexDAO.query(query as SolrQuery)
        println qr
        println result

        then:
        qr != null
        with qr, result

        where:
        desc | query || result
        'basic taxon_name'  | [ fields: [ 'taxon_name' ], query: 'taxon_name:*' ] || { QueryResponse resp ->
            assert resp.results.size() > 0
            assert resp.results.get(0).getFieldValue('taxon_name') != null
            assert resp.results.get(0).getFieldValue('scientificName') == null
        }
        'basic taxon_name & scientificName'  | [ fields: [ 'taxon_name', 'scientificName' ], query: 'taxon_name:*' ] || { QueryResponse resp ->
            assert resp.results.size() > 0
            assert resp.results.get(0).getFieldValue('taxon_name') != null
            assert resp.results.get(0).getFieldValue('scientificName') != null
        }
        'combination query' | [ fields: [ 'taxon_name' ], query: 'taxon_name:* AND -(common_name:"test")' ] || { QueryResponse resp ->
            assert resp.results.size() > 0
            assert resp.results.get(0).getFieldValue('taxon_name') != null
            assert resp.results.get(0).getFieldValue('scientificName') == null
        }
        'deprecated field'  | [ fields: [ 'deleted' ], query: 'deleted:*' ] || { QueryResponse resp ->
            assert resp.results.size() == 0
        }
        'deprecated field (string)' | [ fields: [ 'deleted' ], query: 'deleted:""' ] || { QueryResponse resp ->
            assert resp.results.size() == 0
        }
        'deprecated field (number)' | [ fields: [ 'deleted' ], query: 'deleted:100' ] || { QueryResponse resp ->
            assert resp.results.size() == 0
        }
        'deprecated field (range)' | [ fields: [ 'deleted' ], query: 'deleted:[* TO 100]' ] || { QueryResponse resp ->
            assert resp.results.size() == 0
        }
        'deprecated field combination' | [ fields: [ 'deleted' ], query: '*:* deleted:*' ] || { QueryResponse resp ->
            assert resp.results.size() > 0
            assert resp.results.get(0).getFieldValue('delete') == null
        }
        'deprecated field combination' | [ fields: [ 'deleted' ], query: '*:* AND deleted:*' ] || { QueryResponse resp ->
            assert resp.results.size() == 0
        }
        'basic filter query' | [ fields: [ 'taxon_name' ], query: '*:*', filterQueries: [ 'taxon_name:*', 'scientificName:*' ] ] || { QueryResponse resp ->
            assert resp.results.size() > 0
            assert resp.results.get(0).getFieldValue('taxon_name') != null
            assert resp.results.get(0).getFieldValue('scientificName') == null
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
        QueryResponse qr = indexDAO.query(query)

        then:
        qr != null
        qr.facetFields?.size() == 1
        qr.facetFields.get(0).name == 'scientificName'

        when:
        query.facet = false
        query.addFacetField('taxon_name')
        qr = indexDAO.query(query)

        then:
        qr != null
        qr.facetFields?.size() == 1
        qr.facetFields.get(0).name == 'taxon_name'

        when:
        query.facet = false
        query.addFacetField('aust_conservation')
        qr = indexDAO.query(query)

        then:
        qr != null
        qr.facetFields?.size() == 1
        qr.facetFields.get(0).name == 'aust_conservation'
    }

    def 'facet query getFacetField'() {

        setup:
        SolrQuery query = new SolrQuery()
        query.query = '*:*'
        query.fields = [ 'id' ]
        query.addFacetField('scientificName')

        when:
        QueryResponse qr = indexDAO.query(query)

        then:
        qr != null
        qr.facetFields?.size() == 1
        qr.getFacetField('scientificName') != null

        when:
        query.facet = false
        query.addFacetField('taxon_name')
        qr = indexDAO.query(query)

        then:
        qr != null
        qr.facetFields?.size() == 1
        qr.getFacetField('taxon_name') != null

        when:
        query.facet = false
        query.addFacetField('aust_conservation')
        qr = indexDAO.query(query)

        then:
        qr != null
        qr.facetFields?.size() == 1
        qr.getFacetField('aust_conservation') != null
    }
}
