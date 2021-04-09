package au.org.ala.biocache.util.solr


import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class FieldMappingUtilSpec extends Specification {

    @Shared
    FieldMappingUtil fieldMappingUtil

    void setup() {

        fieldMappingUtil = new FieldMappingUtil()
        fieldMappingUtil.deprecatedFieldsConfig = '/data/biocache/config/deprecated-fields.json'
        fieldMappingUtil.deprecatedEnumValuesConfig = '/data/biocache/config/deprecated-enum-values.json'

        fieldMappingUtil = builder.newInstance()
    }

    @Unroll
    def 'translateQueryFields: #query'() {

        expect:
        fieldMappingUtil.translateQueryFields(query) == expetedQuery


        where:
        query || expetedQuery
        'taxon_name:*' || 'scientificName:*'
        '-taxon_name:*' || '-scientificName:*'
        'taxon_name:* AND -(common_name:"test")' || 'scientificName:* AND -(vernacularName:"test")'
        'deleted:*' || 'deprecated_deleted:*'
        'assertions:*' || 'assertions:*'
        'assertions:badlyFormedBasisOfRecord' || 'assertions:BASIS_OF_RECORD_INVALID'
        'assertions:(badlyFormedBasisOfRecord coordinatesOutOfRange)' || 'assertions:(BASIS_OF_RECORD_INVALID COORDINATE_OUT_OF_RANGE)'
        'taxon_name:* assertions:badlyFormedBasisOfRecord AND -(common_name:"test")' || 'scientificName:* assertions:BASIS_OF_RECORD_INVALID AND -(vernacularName:"test")'
    }

    def 'translateFieldList'() {

        expect:
        fieldMappingUtil.translateFieldList(fls as String[]) == expectedFls

        where:
        fls || expectedFls
        [ 'test1', 'test2' ] || [ 'test1', 'test2' ]
        [ 'test1,test2', 'test3' ] || [ 'test1,test2', 'test3' ]
        [ 'test1,taxon_name', 'test3' ] || [ 'test1,scientificName', 'test3' ]

    }
}
