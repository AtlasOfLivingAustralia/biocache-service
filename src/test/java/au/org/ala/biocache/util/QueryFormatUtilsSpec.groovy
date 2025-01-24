package au.org.ala.biocache.util

import au.org.ala.biocache.dao.QidCacheDAO
import au.org.ala.biocache.dto.Qid
import au.org.ala.biocache.dto.SpatialSearchRequestDTO
import au.org.ala.biocache.service.AuthService
import au.org.ala.biocache.service.DataQualityService
import au.org.ala.biocache.service.LayersService
import au.org.ala.biocache.service.ListsService
import au.org.ala.biocache.util.solr.FieldMappingUtil
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.io.Resources
import org.apache.solr.client.solrj.util.ClientUtils
import org.springframework.web.client.RestClientException
import spock.lang.Specification

class QueryFormatUtilsSpec extends Specification {

    QueryFormatUtils queryFormatUtils = new QueryFormatUtils()

    def listsService = Stub(ListsService)
    def searchUtils = Stub(SearchUtils)
    def layersService = Stub(LayersService)
    def qidCacheDao = Stub(QidCacheDAO)
    def dataQualityService = Stub(DataQualityService)
    def authService = Stub(AuthService)

    def setup() {
        queryFormatUtils.listsService = listsService
        queryFormatUtils.searchUtils = searchUtils
        queryFormatUtils.layersService = layersService
        queryFormatUtils.qidCacheDao = qidCacheDao
        queryFormatUtils.dataQualityService = dataQualityService
        queryFormatUtils.fieldMappingUtil = Mock(FieldMappingUtil) {
            translateQueryFields(_ as String) >> { String query -> return query }
        }
        queryFormatUtils.authService = authService
    }

    def "test spatial_list: handling"(String currentDisplay, String currentQuery, String resultDisplay, String resultQuery) {
        setup:
        queryFormatUtils.maxBooleanClauses = 12
        listsService.getListItems(_ as String, _ as Boolean) >> { String id, Boolean b -> getTestListItems(id) }
        listsService.getListInfo(_ as String) >> { String id -> getTestList(id) }
        searchUtils.getTaxonSearch(_ as String) >> { String lsid -> [ "taxon_concept_lsid:${ClientUtils.escapeQueryChars(lsid)}", "taxon_concept_lsid:$lsid"] as String[] }

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
        listsService.getListItems(_ as String, _ as Boolean) >> { String id, Boolean _ -> throw new RestClientException("Boom") }
        listsService.getListInfo(_ as String) >> { String id -> throw new RestClientException("Boom") }

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

    def "test formatSearchQuery with empty query"() {
        given:
        SpatialSearchRequestDTO searchParams = new SpatialSearchRequestDTO()
        searchParams.setQ("")
        searchParams.setFq(new String[0])

        when:
        def result = queryFormatUtils.formatSearchQuery(searchParams, false)

        then:
        result[0].isEmpty()
        result[1].isEmpty()
        searchParams.getFormattedQuery() == ""
        searchParams.getDisplayString() == ""
    }

    def "test formatSearchQuery with simple query"() {
        given:
        SpatialSearchRequestDTO searchParams = new SpatialSearchRequestDTO()
        searchParams.setQ("scientificName:Test")
        searchParams.setFq(new String[0])

        when:
        def result = queryFormatUtils.formatSearchQuery(searchParams, false)

        then:
        result[0].isEmpty()
        result[1].isEmpty()
        searchParams.getFormattedQuery() == "scientificName:Test"
        searchParams.getDisplayString() == "scientificName:Test"
    }

    def "test formatSearchQuery with qid"() {
        given:
        SpatialSearchRequestDTO searchParams = new SpatialSearchRequestDTO()
        searchParams.setQ("qid:123")
        searchParams.setFq(new String[0])
        qidCacheDao.get(_) >> new Qid(q: "scientificName:Test", fqs: new String[0])

        when:
        def result = queryFormatUtils.formatSearchQuery(searchParams, false)

        then:
        result[0].isEmpty()
        result[1].isEmpty()
        searchParams.getFormattedQuery() == "scientificName:Test"
        searchParams.getDisplayString() == "scientificName:Test"
    }

    def "test formatSearchQuery with qid fq"() {
        given:
        SpatialSearchRequestDTO searchParams = new SpatialSearchRequestDTO()
        searchParams.setQ("*:*")
        searchParams.setFq(new String[]{"qid:123456"})
        qidCacheDao.get(_) >> new Qid(q: "scientificName:TestTaxon", fqs: new String[0])
        searchParams.setFacet(true)
        when:
        def result = queryFormatUtils.formatSearchQuery(searchParams, false)

        then:
        result[0].size() == 1
        result[1].size() == 1
        searchParams.getFormattedQuery() == "*:*"
        searchParams.getDisplayString() == "[all records]"
        searchParams.getFq() == new String[]{"qid:123456"}
        searchParams.getFormattedFq() == new String[]{"scientificName:TestTaxon"}
    }

    def "test formatSearchQuery with facets"() {
        given:
        SpatialSearchRequestDTO searchParams = new SpatialSearchRequestDTO()
        searchParams.setQ("scientificName:Test")
        searchParams.setFacet(true)
        searchParams.setIncludeUnfilteredFacetValues(false)
        searchParams.setFq(new String[]{"month:1", "year:2020"})
        searchParams.setFacets(new String[]{"month", "year", "eventDate"})

        when:
        def result = queryFormatUtils.formatSearchQuery(searchParams, false)

        then:
        result[0].size() == 2
        result[1].size() == 2
        searchParams.getFormattedQuery() == "scientificName:Test"
        searchParams.getFormattedFq() == new String[]{"month:1", "year:2020"}
        searchParams.getFq() == new String[]{"month:1", "year:2020"}
        searchParams.getDisplayString() == "scientificName:Test"
        searchParams.getFacets() == new String[]{"month", "year", "eventDate"}
        searchParams.getPivotFacets() == new String[]{}
    }

    def "test formatSearchQuery with tagging and excluded facets"() {
        given:
        SpatialSearchRequestDTO searchParams = new SpatialSearchRequestDTO()
        searchParams.setQ("scientificName:Test")
        searchParams.setFacet(true)
        searchParams.setIncludeUnfilteredFacetValues(true)
        searchParams.setFq(new String[]{"month:1", "year:2020"})
        searchParams.setFacets(new String[]{"month", "year", "eventDate"})

        when:
        def result = queryFormatUtils.formatSearchQuery(searchParams, false)

        then:
        result[0].size() == 2
        result[1].size() == 2
        searchParams.getFormattedQuery() == "scientificName:Test"
        searchParams.getDisplayString() == "scientificName:Test"
        searchParams.getFormattedFq() == new String[]{"{!tag=month}month:1", "{!tag=year}year:2020"}
        searchParams.getFq() == new String[]{"month:1", "year:2020"}
        searchParams.getFacets() == new String[]{"eventDate"}
        searchParams.getPivotFacets() == new String[]{"{!ex=month}month", "{!ex=year}year"}
    }

    private static ObjectMapper om = new ObjectMapper()

    private static String getResultQuery(String uid) {
        Resources.getResource("au/org/ala/biocache/util/${uid}_query.txt").getText('UTF-8')
    }

    private static List<ListsService.SpeciesListItemDTO> getTestListItems(String uid) {
        om.readValue(Resources.getResource("au/org/ala/biocache/util/${uid}_items.json"), new TypeReference<List<ListsService.SpeciesListItemDTO>>() {
        })
    }

    private static ListsService.SpeciesListSearchDTO.SpeciesListDTO getTestList(String uid) {
        om.readValue(Resources.getResource("au/org/ala/biocache/util/${uid}_lists.json"), ListsService.SpeciesListSearchDTO.SpeciesListDTO)
    }
}
