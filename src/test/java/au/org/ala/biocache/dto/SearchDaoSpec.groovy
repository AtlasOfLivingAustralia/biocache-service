package au.org.ala.biocache.dto

import au.org.ala.biocache.dao.SearchDAO
import org.apache.solr.client.solrj.SolrClient
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.web.WebAppConfiguration
import spock.lang.Specification

@WebAppConfiguration
@ContextConfiguration(locations = 'classpath:springTest.xml')
class SearchDaoSpec extends Specification {

    @Autowired
    SearchDAO searchDAO

    @Autowired
    SolrClient solrClient

    def queryMapping() {

        println "solrClient: ${solrClient}"

        expect:
        solrClient != null

    }
}
