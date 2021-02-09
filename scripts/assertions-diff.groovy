@GrabResolver(name='org.gbif', root='http://repository.gbif.org/content/groups/gbif')
@Grapes([
//        @GrabConfig(systemClassLoader=true),
        @Grab(group='org.apache.httpcomponents', module='httpmime', version='4.3.1'),
        @Grab(group = 'org.codehaus.groovy.modules.http-builder', module='http-builder', version='0.7.1'),
        @Grab(group='org.gbif', module='gbif-api', version='0.138'),
//        @Grab(group='au.org.ala', module='livingatlas', version='2.8.0-SNAPSHOT')
//        @Grab(group='au.orgls.ala', module='livingatlas', version='2.8.0-SNAPSHOT', changing=true)
])

import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import groovy.yaml.YamlSlurper

import groovyx.net.http.*

import org.gbif.api.vocabulary.OccurrenceIssue
//import org.ala.pipelines.vocabulary.ALAOccurrenceIssue

//println ALAOccurrenceIssue.values()

YamlSlurper yamlSlurper = new YamlSlurper()

def config = yamlSlurper.parse(new File('./config/field-mapping.yml'))

class AssertionFacet {

    String name
    long count
}

class AssertionMapping {

    AssertionFacet biocacheAssertion
    AssertionFacet pipelinesAssertion

    String gbifOccurrenceIssue
    String alaOccurrenceIssue
}

Collection<AssertionFacet> parseAssertions(String host, String collection) {

    def http = new HTTPBuilder(host)

    Collection assertions = http.request(Method.GET, ContentType.JSON) {

        uri.path = "/solr/${collection}/select"
        uri.query = [
                q            : '*:*',
                limit        : 0,
                facet        : 'on',
                'facet.field': 'assertions',
                'facet.limit': 1000,
                wt           : 'json',
                indent       : 'on'
        ]
        response.success = { resp, json ->

            json.facet_counts.facet_fields.assertions
        }
    }
    Collection<AssertionFacet> assertionFacets = []

    for (int i = 0; i < assertions.size(); i += 2) {

        assertionFacets << new AssertionFacet(name: assertions[i], count: assertions[i+1])
    }

    return assertionFacets
}

def toCamelCase = { it.replaceAll("(_)([A-Za-z0-9])", { Object[] o -> o[2].toUpperCase() }) }
def toSnakeCase = { it.replaceAll( /([A-Z])/, /_$1/ ).toLowerCase().replaceAll( /^_/, '' ) }

Collection<AssertionFacet> biocacheAssertions = parseAssertions(config.biocache.solr.host, config.biocache.solr.collection)
Collection<AssertionFacet> pipelinesAssertions = parseAssertions(config.pipelines.solr.host, config.pipelines.solr.collection)

File assertionMappingJson = new File(config.fields.mappingJson)

Map<String, String> assertionNameMappings = [:]

JsonSlurper slurper = new JsonSlurper()

if (assertionMappingJson.exists()) {
    assertionNameMappings = slurper.parse(assertionMappingJson)
}

println biocacheAssertions
println pipelinesAssertions

Collection<AssertionMapping> assertionMappings = pipelinesAssertions.collect { AssertionFacet assertionFacet ->

    return new AssertionMapping(pipelinesAssertion: assertionFacet)
}

biocacheAssertions.each { AssertionFacet assertionFacet ->

    AssertionMapping assertionMapping = assertionMappings.find { AssertionMapping assertionMapping ->

        if (assertionMapping.biocacheAssertion) { return false }

        String pipelineAssertion = assertionNameMappings[assertionFacet.name]

        if (pipelineAssertion && assertionMapping.pipelinesAssertion.name == pipelineAssertion) {
            return true
        }

        if (assertionMapping.pipelinesAssertion?.name?.equalsIgnoreCase(assertionFacet.name)) {

            return true
        }

        if (assertionMapping.pipelinesAssertion?.name?.equalsIgnoreCase(toSnakeCase(assertionFacet.name))) {

            return true
        }

        return false
    }

    if (assertionMapping) {
        assertionMapping.biocacheAssertion = assertionFacet
    } else {
        assertionMappings << new AssertionMapping(biocacheAssertion: assertionFacet)
    }
}

OccurrenceIssue.values().each { OccurrenceIssue gbifOccurrenceIssue ->

    AssertionMapping assertionMapping = assertionMappings.find { AssertionMapping assertionMapping ->

        if (assertionMapping.gbifOccurrenceIssue) { return false }

        if (gbifOccurrenceIssue.name().equalsIgnoreCase(assertionMapping.biocacheAssertion.name) ||
                gbifOccurrenceIssue.name().equalsIgnoreCase(assertionMapping.pipelinesAssertion.name)) {
            return true
        }

        String gbifOccurrenceIssueName = toCamelCase(gbifOccurrenceIssue.name())

        if (gbifOccurrenceIssueName.equalsIgnoreCase(assertionMapping.biocacheAssertion.name) ||
                gbifOccurrenceIssueName.equalsIgnoreCase(assertionMapping.pipelinesAssertion.name)) {
            return true
        }

        return false
    }

    if (assertionMapping) {
        assertionMapping.gbifOccurrenceIssue = gbifOccurrenceIssue
    } else {
        assertionMappings << new AssertionMapping(gbifOccurrenceIssue: gbifOccurrenceIssue)
    }
}

def assertionComparitor = { AssertionMapping lft, AssertionMapping rgt ->

    String lftFieldName = lft.biocacheAssertion?.name ?: (lft.pipelinesAssertion?.name ?: (lft.gbifOccurrenceIssue ?: lft.alaOccurrenceIssue))
    String rgtFieldName = rgt.biocacheAssertion?.name ?: (rgt.pipelinesAssertion?.name ?: (rgt.gbifOccurrenceIssue ?: rgt.alaOccurrenceIssue))

    if (lftFieldName == rgtFieldName) {
        return 0
    }
    if (!lftFieldName) {
        return -1
    }
    if (!rgtFieldName) {
        return 1
    }

    return lftFieldName.compareToIgnoreCase(rgtFieldName)

}

File assertionsCsv = new File(config.assertions.export)

assertionsCsv.newWriter().withWriter { Writer w ->

    w << "\"Solr V8 (pipelines)\",,\"Solr v6\",,\"GBIF Occurrence Issue\",\"ALA Occurrence Issue\"\n"

    assertionMappings.sort(assertionComparitor).each { AssertionMapping assertionMapping ->

        w << "\"${assertionMapping.pipelinesAssertion?.name ?: ''}\","
        w << "${assertionMapping.pipelinesAssertion?.count ?: ''},"
        w << "\"${assertionMapping.biocacheAssertion?.name ?: ''}\","
        w << "${assertionMapping.biocacheAssertion?.count ?: ''},"
        w << "${assertionMapping.gbifOccurrenceIssue ?: ''},"
        w << '\n'
    }
}

//println ALAOccurrenceIssue.values()


/*
class SolrField {
    String name
    String type
    boolean multi
    long count = 0
    boolean deprecated = false
}

class BiocacheField {

    String name
    String dataType
    Boolean indexed
    Boolean stored
    Boolean multivalue
    Boolean docvalue
    String info
    String downloadName
    String description
    String dwcTerm
    String classs
    String jsonName
    String infoUrl
    String i18nValues
    String downloadDescription

    boolean deprecated = false
}

class DwcField {

    @CsvBindByName(column = 'term_localName', required = true)
    String name

    @CsvBindByName(column = 'term_iri', required = true)
    String termIri

    @CsvBindByName(column = 'status', required = true)
    String status

    @CsvBindByName(column = 'flags')
    String flags
}

class FieldMapping {

    BiocacheField biocacheField
    DwcField dwcField

    SolrField legacySolrField
    SolrField pipelinesSolrField
}



def toCamelCase = { it.replaceAll("(_)([A-Za-z0-9])", { Object[] o -> o[2].toUpperCase() }) }


Map<String, SolrField> parseSolrFields(String host, String collection) {

    Map<String, SolrField> solrFields

    def http = new HTTPBuilder(host)

    http.request(Method.GET, ContentType.JSON) {

        uri.path = "/solr/${collection}/schema"

        response.success = { resp, json ->

            solrFields = json.schema.fields.collectEntries { [(it.name): new SolrField(name: it.name, type: it.type, multi: !!it.multiValued)] }
            json.schema.dynamicFields
        }
    }

    def shards = []

    http.request(Method.GET, ContentType.JSON) {

        uri.path = '/solr/admin/collections'
        uri.query = [action: 'CLUSTERSTATUS', wt: 'json']

        response.success = { resp, json ->

            String collectionAlias = collection

            if (json.cluster.aliases && json.cluster.aliases[collection]) {

                collectionAlias = json.cluster.aliases[collection]
            }

            json.cluster.collections[collectionAlias].shards.each { shard ->

                def firstReplica = shard.value.replicas.entrySet().toArray()[0]

                shards += firstReplica.value.core
            }
        }
    }

    shards.each { shard ->

        http.request(Method.GET, ContentType.JSON) {

            uri.path = "/solr/${shard}/admin/luke"
            uri.query = [wt: 'json']

            response.success = { resp, json ->

                json.fields.each { field, stats ->

                    if (solrFields[field]) {

                        solrFields[field].count += stats.docs ?: 0

                    } else {

                        solrFields[field] = new SolrField(name: field, type: stats.type, multi: stats.schema.contains('M'), count: stats.docs?:0)
                    }
                }
            }
        }
    }

    return solrFields
}


YamlSlurper yamlSlurper = new YamlSlurper()

def config = yamlSlurper.parse(new File('./config/field-mapping.yml'))

// parse the TDWG DwC term vocabulary csv into a map of DwcFields
Map<String, DwcField> dwcFields
config.dwcTerms.toURL().withReader { Reader reader ->

    dwcFields = new CsvToBeanBuilder(reader)
            .withType(DwcField.class)
            .build()
            .parse()
            .findAll { it.status == 'recommended' }
            .collectEntries { [ (it.name): it ]}
}


JsonSlurper slurper = new JsonSlurper()

def biocacheV2Fields = slurper.parse(config.biocache.fields.toURL())

// create Map of all solr fields keyed by field name
Map<String, SolrField> pipelinesSolrFields = parseSolrFields(config.pipelines.solr.host, config.pipelines.solr.collection)
Map<String, SolrField> legacySolrFields = parseSolrFields(config.biocache.solr.host, config.biocache.solr.collection)

File fieldMappingJson = new File(config.mappingJson)

Map<String, String> fieldNameMappings = [:]

if (fieldMappingJson.exists()) {
    fieldNameMappings = slurper.parse(fieldMappingJson)
}

println "${pipelinesSolrFields.size()} : ${legacySolrFields.size()} : ${biocacheV2Fields.size()}, ${fieldNameMappings.size()}"

fieldNameMappings.each { String legacyFieldName, String pipelinesFieldName ->

    if (legacySolrFields[legacyFieldName] && pipelinesSolrFields[pipelinesFieldName]) {


    }
}

// process the biocache fields mapping to the solr schema fields
List<FieldMapping> fieldMappings = biocacheV2Fields.collect { field ->

    BiocacheField biocacheField = new BiocacheField(field)
    FieldMapping fieldMapping = new FieldMapping(biocacheField: biocacheField)

    // the the current biocache field name against the legacy solr field
    fieldMapping.legacySolrField = legacySolrFields[biocacheField.name]
    legacySolrFields.remove(biocacheField.name)

    if (fieldNameMappings.containsKey(biocacheField.name)) {

        String pipelinesFieldName = fieldNameMappings[biocacheField.name]
        if (pipelinesSolrFields[pipelinesFieldName]) {

            fieldMapping.pipelinesSolrField = pipelinesSolrFields[pipelinesFieldName]
            pipelinesSolrFields.remove(pipelinesFieldName)

        } else if (pipelinesFieldName == null) {

            fieldMapping.biocacheField.deprecated = true
            fieldMapping.legacySolrField.deprecated = true

        } else {

            println "no field manual mapping for ${biocacheField.name} -> ${pipelinesFieldName}"
        }

    } else if (!biocacheField.name.startsWith('raw_') && !biocacheField.name.startsWith('_') && pipelinesSolrFields[biocacheField.dwcTerm]) {

        // check the pipelines solr field name matchs DwC term
        fieldMapping.pipelinesSolrField = pipelinesSolrFields[biocacheField.dwcTerm]
        pipelinesSolrFields.remove(biocacheField.dwcTerm)

    } else if (pipelinesSolrFields[biocacheField.name]) {

        // check the pipelines solr field name exact match
        fieldMapping.pipelinesSolrField = pipelinesSolrFields[biocacheField.name]
        pipelinesSolrFields.remove(biocacheField.name)

    } else {

        String camelFieldName = toCamelCase(biocacheField.name)

        if (pipelinesSolrFields[camelFieldName]) {

            // check for camel case version of biocache field name
            fieldMapping.pipelinesSolrField = pipelinesSolrFields[camelFieldName]
            pipelinesSolrFields.remove(camelFieldName)

//        } else if (pipelinesSolrFields[biocacheField.dwcTerm]) {
//
//            // check pipelines solr field name exact match to DWC term
//            fieldMapping.pipelinesSolrField = pipelinesSolrFields[biocacheField.dwcTerm]
//            pipelinesSolrFields.remove(biocacheField.dwcTerm)
        }
    }

    return fieldMapping
}

// add all unmatched legacy solr schema fields to the mapping object
fieldMappings += legacySolrFields.collect { fieldName, solrField ->

    FieldMapping fieldMapping = new FieldMapping(legacySolrField: solrField)

    if (fieldNameMappings.containsKey(fieldName)) {

        String pipelinesFieldName = fieldNameMappings[fieldName]
        if (pipelinesSolrFields[pipelinesFieldName]) {

            fieldMapping.pipelinesSolrField = pipelinesSolrFields[pipelinesFieldName]
            pipelinesSolrFields.remove(pipelinesFieldName)

        } else if (pipelinesFieldName == null) {

            fieldMapping.legacySolrField.deprecated = true

        } else {

            println "no field mapping for ${fieldName} -> ${pipelinesFieldName}"
        }

    } else if (pipelinesSolrFields[fieldName]) {

        // check the pipelines solr exact legacy solr field name
        fieldMapping.pipelinesSolrField = pipelinesSolrFields[fieldName]
        pipelinesSolrFields.remove(fieldName)

    } else if (!fieldName.startsWith('_')) {

        String camelField = toCamelCase(fieldName)

        if (pipelinesSolrFields[camelField]) {

            // check the pipelines solr camel case version of legacy solr field name
            fieldMapping.pipelinesSolrField = pipelinesSolrFields[camelField]
            pipelinesSolrFields.remove(camelField)
        }
    }

//    if (dwcFields[fieldName]) {
//
//        // check the DWC term against exact legacy solr field name
//        fieldMapping.dwcField = dwcFields[fieldName]
//        dwcFields.remove(fieldName)
//
//    } else if (!fieldName.startsWith('_')) {
//
//        String camelField = toCamelCase(fieldName)
//
//        if (dwcFields[camelField]) {
//
//            // check the DWC term against camel case version of legacy solr field name
//            fieldMapping.dwcField = dwcFields[camelField]
//            dwcFields.remove(camelField)
//        }
//    }

    return fieldMapping
}

// try and match unmatched pipelines solr fields
pipelinesSolrFields.each { fieldName, pipelinesSolrField ->

    // perform a case insensitive search for biocache field or camel case version
    FieldMapping unmatchedField = fieldMappings.find { FieldMapping fieldMapping ->

        if (fieldMapping.biocacheField) {
            boolean found = fieldMapping.biocacheField.name.equalsIgnoreCase(fieldName)
            if (!found && !fieldMapping.biocacheField.name.startsWith('_')) {
                found = toCamelCase(fieldMapping.biocacheField.name).equalsIgnoreCase(fieldName)
            }
            if (!found) {

                // match using DwC term (excluding some dynamic fields)
                found = !fieldMapping.biocacheField.name.startsWith('raw_') && !fieldMapping.biocacheField.name.startsWith('_') && fieldName == fieldMapping.biocacheField.dwcTerm
            }
            return found
        }
        return false
    }

    if (unmatchedField && !unmatchedField.pipelinesSolrField) {

        unmatchedField.pipelinesSolrField = pipelinesSolrField

    } else {

        fieldMappings += new FieldMapping(pipelinesSolrField: pipelinesSolrField)
    }
}

def http = new HTTPBuilder(config.pipelines.solr.host)

http.request(Method.GET, ContentType.JSON) {

    uri.path = "/solr/${config.pipelines.solr.collection}/schema"

    response.success = { resp, json ->

        json.schema.dynamicFields.each { dynamicField ->

            String regex = "^${dynamicField.name.replaceAll('\\*', '\\.\\*')}\$"

            fieldMappings.findAll { FieldMapping fieldMapping ->

                fieldMapping.legacySolrField?.name ==~ ~/${regex}/

            }.each { FieldMapping fieldMapping ->

                if (!fieldMapping.pipelinesSolrField) {

                    fieldMapping.pipelinesSolrField = new SolrField(name: dynamicField.name, type: dynamicField.type)
                }
            }
        }
    }
}

/*
// process the dynamic fields searching for unmatched legacy solr fields
biocacheV3Schema.schema.dynamicFields.each { dynamicField ->

    String regex = "^${dynamicField.name.replaceAll('\\*', '\\.\\*')}\$"

    fieldMappings.findAll { FieldMapping fieldMapping ->

        fieldMapping.legacySolrField?.name ==~ ~/${regex}/

    }.each { FieldMapping fieldMapping ->

        if (!fieldMapping.pipelinesSolrField) {

            fieldMapping.pipelinesSolrField = new SolrField(name: dynamicField.name, type: dynamicField.type)
        }
    }
}
*/
/*
// map the dwc terms
fieldMappings.each { FieldMapping fieldMapping ->

    String dwcFieldName = fieldMapping.biocacheField?.name

    DwcField dwcField = dwcFields[dwcFieldName]
    if (dwcField) {

        fieldMapping.dwcField = dwcField
        dwcFields.remove(dwcFieldName)

    } else {

        dwcFieldName = fieldMapping.pipelinesSolrField?.name
        dwcField = dwcFields[dwcFieldName]

        if (dwcField) {

            fieldMapping.dwcField = dwcField
            dwcFields.remove(dwcFieldName)

        } else {

            dwcFieldName = fieldMapping.legacySolrField?.name
            dwcField = dwcFields[dwcFieldName]

            if (dwcField) {

                fieldMapping.dwcField = dwcField
                dwcFields.remove(dwcFieldName)
            }
        }
    }
}

// add the unmatched dwc terms
fieldMappings += dwcFields.collect { fieldName, dwcField -> new FieldMapping(dwcField: dwcField) }


def fieldNameComparitor = { lft, rgt ->

    String lftFieldName = lft.biocacheField?.name ?: (lft.legacySolrField?.name ?: (lft.pipelinesSolrField?.name ?: lft.dwcField?.name))
    String rgtFieldName = rgt.biocacheField?.name ?: (rgt.legacySolrField?.name ?: (rgt.pipelinesSolrField?.name ?: rgt.dwcField?.name))

    if (lftFieldName == rgtFieldName) {
        return 0
    }
    if (!lftFieldName) {
        return -1
    }
    if (!rgtFieldName) {
        return 1
    }

    return lftFieldName.compareToIgnoreCase(rgtFieldName)

}

File fieldMappingCsv = new File(config.mappingCsv)

fieldMappingCsv.newWriter().withWriter { Writer w ->

    w << "\"Solr V8 (pipelines)\",,,,\"Solr v6\",,,,,\"Biocache Index Fields\",,\"DwC Fields\"\n"

    w << "\"field_name\","
    w << "\"field_type\","
    w << "\"multi_value\","
    w << "\"record_count\","

    w << "\"field_name\","
    w << "\"field_type\","
    w << "\"multi_value\","
    w << "\"record_count\","
    w << "\"deprecated\","

    w << "\"dwc_term\","
    w << "\"term_iri\","
    w << "\"flags\","

    w << "\"name\","
    w << "\"jsonName\","
    w << "\"dwcTerm\","
    w << "\"downloadName\","
    w << "\"dataType\","
    w << "\"classs\","
    w << "indexed,"
    w << "docvalue,"
    w << "stored,"
    w << "multivalue,"
    w << "\"i18nValues\","
    w << "\"description\","
    w << "\"downloadDescription\","
    w << "\"info\","
    w << "\"infoUrl\""
    w << '\n'

    fieldMappings.sort(fieldNameComparitor).each { FieldMapping fieldMapping ->

        w << "\"${fieldMapping.pipelinesSolrField?.name ?: ''}\","
        w << "\"${fieldMapping.pipelinesSolrField?.type ?: ''}\","
        w << "${fieldMapping.pipelinesSolrField ? fieldMapping.pipelinesSolrField.multi : ''},"
        w << "${fieldMapping.pipelinesSolrField ? fieldMapping.pipelinesSolrField?.count : ''},"

        w << "\"${fieldMapping.legacySolrField?.name ?: ''}\","
        w << "\"${fieldMapping.legacySolrField?.type ?: ''}\","
        w << "${fieldMapping.legacySolrField ? fieldMapping.legacySolrField.multi : ''},"
        w << "${fieldMapping.legacySolrField ? fieldMapping.legacySolrField?.count : ''},"
        w << "${fieldMapping.legacySolrField ? fieldMapping.legacySolrField.deprecated : ''},"

        w << "\"${fieldMapping.dwcField?.name ?: ''}\","
        w << "\"${fieldMapping.dwcField?.termIri ?: ''}\","
        w << "\"${fieldMapping.dwcField?.flags ?: ''}\","

        w << "\"${fieldMapping.biocacheField?.name ?: ''}\","
        w << "\"${fieldMapping.biocacheField?.jsonName ?: ''}\","
        w << "\"${fieldMapping.biocacheField?.dwcTerm ?: ''}\","
        w << "\"${fieldMapping.biocacheField?.downloadName ?: ''}\","
        w << "\"${fieldMapping.biocacheField?.dataType ?: ''}\","
        w << "\"${fieldMapping.biocacheField?.classs ?: ''}\","
        w << "${fieldMapping.biocacheField?.indexed ?: ''},"
        w << "${fieldMapping.biocacheField?.docvalue ?: ''},"
        w << "${fieldMapping.biocacheField?.stored ?: ''},"
        w << "${fieldMapping.biocacheField?.multivalue ?: ''},"
        w << "\"${fieldMapping.biocacheField?.i18nValues ?: ''}\","
        w << "\"${fieldMapping.biocacheField?.description ?: ''}\","
        w << "\"${fieldMapping.biocacheField?.downloadDescription ?: ''}\","
        w << "\"${fieldMapping.biocacheField?.info ?: ''}\","
        w << "\"${fieldMapping.biocacheField?.infoUrl ?: ''}\""
        w << '\n'
    }
}

Map<String, String> deprecatedFields = [:]

fieldMappings.each { FieldMapping fieldMapping ->
    if (fieldMapping.legacySolrField && fieldMapping.pipelinesSolrField && !fieldMapping.pipelinesSolrField.name.contains('*') && fieldMapping.legacySolrField.name != fieldMapping.pipelinesSolrField.name) {
        deprecatedFields[fieldMapping.legacySolrField.name] = fieldMapping.pipelinesSolrField.name
    } else if (fieldMapping.legacySolrField?.deprecated) {
        deprecatedFields[fieldMapping.legacySolrField.name] = null
    }
}

new File(config.mappingJson).newWriter().withWriter { Writer w ->
    w << JsonOutput.prettyPrint(JsonOutput.toJson(deprecatedFields))
}


 */