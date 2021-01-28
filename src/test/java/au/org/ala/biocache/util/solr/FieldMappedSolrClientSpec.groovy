package au.org.ala.biocache.util.solr

import com.github.tomakehurst.wiremock.WireMockServer
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.response.QueryResponse
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.web.WebAppConfiguration
import spock.lang.Specification

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.*

@WebAppConfiguration
@ContextConfiguration(locations = 'classpath:springTest.xml')
class FieldMappedSolrClientSpec extends Specification {

    @Autowired
    SolrClient solrClient

    WireMockServer wm = new WireMockServer(options().port(2345));

    def basicFieldMapping() {

        setup:
        SolrQuery query = new SolrQuery()
        query.fields = [ 'scientificName' ]
        query.query = 'scientificName:*'
        query.filterQueries = [ '' ]

        stubFor(get(urlEquals(''))
        )

        when:
        QueryResponse qr = solrClient.query(query)

        then:
        qr != null

        println qr.toString()
    }
}
