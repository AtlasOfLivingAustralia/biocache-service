package au.org.ala.biocache.util

import au.org.ala.biocache.service.ListsService
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.io.Resources
import org.apache.solr.client.solrj.util.ClientUtils
import org.springframework.web.client.RestClientException
import spock.lang.Specification

class QueryFormatUtilsSpec extends Specification {

    QueryFormatUtils queryFormatUtils = new QueryFormatUtils()

    def listsService = Stub(ListsService)
    def searchUtils = Stub(SearchUtils)

    def setup() {

        queryFormatUtils.listsService = listsService
        queryFormatUtils.searchUtils = searchUtils
//        queryFormatUtils.searchUtils = new SearchUtils()
//        queryFormatUtils.searchUtils.nameIndexLocation = '/data/lucene/namematching'
    }


    def "test spatial_list: handling"(String currentDisplay, String currentQuery, String resultDisplay, String resultQuery) {
        setup:
        queryFormatUtils.maxBooleanClauses = 12
        listsService.getListItems(_) >> { String id -> getTestListItems(id) }
        listsService.getListInfo(_) >> { String id -> getTestList(id) }
        searchUtils.getTaxonSearch(_) >> { String lsid -> [ "taxon_concept_lsid:${ClientUtils.escapeQueryChars(lsid)}", "taxon_concept_lsid:$lsid"] as String[] }

        when:
        def current = [currentDisplay, currentQuery] as String[]
        queryFormatUtils.formatSpeciesList(current)

        then:
        current[0] == resultDisplay
        current[1] == resultQuery

        where:
        currentDisplay        | currentQuery           || resultDisplay | resultQuery
        'species_list:dr123'  | 'species_list:dr123'   || "<span class='species_list' id='dr123'>Species list</span>" | getResultQuery('dr123')
        'species_list:"dr456"'| 'species_list:"dr456"' || "<span class='species_list' id='dr456'>Test List</span>"    | getResultQuery('dr456')
        'species_list:dr123 species_list:dr123'  | 'species_list:dr123 species_list:dr123'   || "<span class='species_list' id='dr123'>Species list</span> <span class='species_list' id='dr123'>Species list</span>" | "${getResultQuery('dr123')} ${getResultQuery('dr123')}"
        'species_list:dr123 <span>between</span> species_list:dr123'  | 'species_list:dr123 field:between species_list:dr123'   || "<span class='species_list' id='dr123'>Species list</span> <span>between</span> <span class='species_list' id='dr123'>Species list</span>" | "${getResultQuery('dr123')} field:between ${getResultQuery('dr123')}"
        '<span class="hello">Test</span> species_list:"dr456" <span class="bye">Test</span>' | 'field:hello species_list:dr456 field:bye' || '<span class="hello">Test</span> <span class=\'species_list\' id=\'dr456\'>Test List</span> <span class="bye">Test</span>' | "field:hello ${getResultQuery('dr456')} field:bye"
        'not_a_species_list:"dr456" OR (species_list:dr123 AND something:else)' | 'not_a_species_list:"dr456" OR (species_list:dr123 AND something:else)' || "not_a_species_list:\"dr456\" OR (<span class='species_list' id='dr123'>Species list</span> AND something:else)" | "not_a_species_list:\"dr456\" OR (${getResultQuery('dr123')} AND something:else)"
        '-species_list:dr123'  | '-species_list:dr123'   || "-<span class='species_list' id='dr123'>Species list</span>" | "-${getResultQuery('dr123')}"
        'not_a_species_list:dr456' | 'not_a_species_list:dr456' || 'not_a_species_list:dr456' | 'not_a_species_list:dr456'
    }

    def "test spatial_list: error handling"(String currentDisplay, String currentQuery, String resultDisplay, String resultQuery) {
        setup:
        listsService.getListItems(_) >> { String id -> throw new RestClientException("Boom") }
        listsService.getListInfo(_) >> { String id -> throw new RestClientException("Boom") }

        when:
        def current = [currentDisplay, currentQuery] as String[]
        queryFormatUtils.formatSpeciesList(current)

        then:
        current[0] == resultDisplay
        current[1] == resultQuery

        where:
        currentDisplay        | currentQuery           || resultDisplay        | resultQuery
        'species_list:dr123'  | 'species_list:dr123'   || '<span class="species_list failed" id=\'dr123\'>dr123 (FAILED)</span>' | '(NOT *:*)'
        'species_list:dr123 species_list:dr456' | 'species_list:dr123 species_list:dr456'   || '<span class="species_list failed" id=\'dr123\'>dr123 (FAILED)</span> <span class="species_list failed" id=\'dr456\'>dr456 (FAILED)</span>' | '(NOT *:*) (NOT *:*)'
        '<span>before</span> species_list:dr123 <span>between</span> species_list:dr456 <span>after</span>' | 'field:before species_list:dr123 field:between species_list:dr456 field:after'   || '<span>before</span> <span class="species_list failed" id=\'dr123\'>dr123 (FAILED)</span> <span>between</span> <span class="species_list failed" id=\'dr456\'>dr456 (FAILED)</span> <span>after</span>' | 'field:before (NOT *:*) field:between (NOT *:*) field:after'
    }

    private static ObjectMapper om = new ObjectMapper()

    private static String getResultQuery(String uid) {
        Resources.getResource("au/org/ala/biocache/util/${uid}_query.txt").getText('UTF-8')
    }

    private static List<String> getTestListItems(String uid) {
        om.readValue(Resources.getResource("au/org/ala/biocache/util/${uid}_items.json"), ArrayList)
    }

    private static ListsService.SpeciesListSearchDTO getTestList(String uid) {
        om.readValue(Resources.getResource("au/org/ala/biocache/util/${uid}_lists.json"), ListsService.SpeciesListSearchDTO)
    }
}
