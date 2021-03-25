@GrabResolver(name='org.gbif', root='http://repository.gbif.org/content/groups/gbif')
@Grapes([
//        @GrabConfig(systemClassLoader=true),
        @Grab(group='org.apache.httpcomponents', module='httpmime', version='4.3.1'),
        @Grab(group = 'org.codehaus.groovy.modules.http-builder', module='http-builder', version='0.7.1'),
        @Grab(group='org.gbif', module='gbif-api', version='0.138'),
//        @Grab(group='au.org.ala', module='livingatlas', version='2.8.2-SNAPSHOT')
])

import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import groovy.yaml.YamlSlurper

import groovyx.net.http.*

import org.gbif.api.vocabulary.OccurrenceIssue
//import au.org.ala.pipelines.vocabulary.ALAOccurrenceIssue

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

File enumValueMappingJson = new File(config.assertions.mappingJson)

Map<String, String> enumValueMappings = [:]

JsonSlurper slurper = new JsonSlurper()

if (enumValueMappingJson.exists()) {
    enumValueMappings = slurper.parse(enumValueMappingJson)
}

Collection<AssertionMapping> assertionMappings = pipelinesAssertions.collect { AssertionFacet assertionFacet ->

    return new AssertionMapping(pipelinesAssertion: assertionFacet)
}



OccurrenceIssue.values().each { OccurrenceIssue gbifOccurrenceIssue ->

    AssertionMapping assertionMapping = assertionMappings.find { AssertionMapping assertionMapping ->

        if (assertionMapping.gbifOccurrenceIssue) { return false }

        if (gbifOccurrenceIssue.name().equalsIgnoreCase(assertionMapping.pipelinesAssertion?.name)) {
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
/*
ALAOccurrenceIssue.values().each { ALAOccurrenceIssue alaOccurrenceIssue ->

    AssertionMapping assertionMapping = assertionMappings.find { AssertionMapping assertionMapping ->

        if (assertionMapping.alaOccurrenceIssue) { return false }

        if (alaOccurrenceIssue.name().equalsIgnoreCase(assertionMapping.pipelinesAssertion?.name)) {
            return true
        }

        return false
    }

    if (assertionMapping) {
        assertionMapping.gbifOccurrenceIssue = alaOccurrenceIssue
    } else {
        assertionMappings << new AssertionMapping(alaOccurrenceIssue: alaOccurrenceIssue)
    }
}
*/
biocacheAssertions.each { AssertionFacet assertionFacet ->

    String mappedAssertionName = enumValueMappings.assertions[assertionFacet.name]
    println "biocache[${assertionFacet.name}] -> ${mappedAssertionName}"

    AssertionMapping assertionMapping = assertionMappings.find { AssertionMapping assertionMapping ->

        if (assertionMapping.biocacheAssertion) { return false }

        String assertionName = assertionMapping.pipelinesAssertion?.name ?: assertionMapping.gbifOccurrenceIssue ?: assertionMapping.alaOccurrenceIssue

        if (mappedAssertionName && mappedAssertionName == assertionName) {
            println "found manual mapping ${assertionFacet.name} -> ${assertionName}"
            return true
        }

        if (assertionName?.equalsIgnoreCase(assertionFacet.name)) {

            println "found exact mapping ${assertionFacet.name} -> ${assertionName}"
            return true
        }

        if (assertionName?.equalsIgnoreCase(toSnakeCase(assertionFacet.name))) {

            println "found snake case mapping ${assertionFacet.name} -> ${assertionName}"
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

def assertionComparitor = { AssertionMapping lft, AssertionMapping rgt ->

    String lftFieldName = (lft.pipelinesAssertion?.name ?: (lft.gbifOccurrenceIssue ?: lft.alaOccurrenceIssue)) ?: lft.biocacheAssertion?.name
    String rgtFieldName = (rgt.pipelinesAssertion?.name ?: (rgt.gbifOccurrenceIssue ?: rgt.alaOccurrenceIssue)) ?: rgt.biocacheAssertion?.name

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

        w << "\"${assertionMapping.pipelinesAssertion?.name ?: assertionMapping.gbifOccurrenceIssue ?: assertionMapping.alaOccurrenceIssue ?: ''}\","
        w << "${assertionMapping.pipelinesAssertion?.count ?: ''},"
        w << "\"${assertionMapping.biocacheAssertion?.name ?: ''}\","
        w << "${assertionMapping.biocacheAssertion?.count ?: ''},"
        w << "${assertionMapping.gbifOccurrenceIssue ? 'TRUE' : ''},"
        w << "${assertionMapping.alaOccurrenceIssue ? 'TRUE' : ''}"
        w << '\n'
    }
}

//enumValueMappings = [:]

assertionMappings.each {

    String assertionName = it.pipelinesAssertion?.name ?: it.gbifOccurrenceIssue ?: it.alaOccurrenceIssue

    if (assertionName && it.biocacheAssertion && assertionName != it.biocacheAssertion.name) {

        enumValueMappings.assertions[it.biocacheAssertion.name] = assertionName
    }
}

new File(config.assertions.mappingJson).newWriter().withWriter { Writer w ->
    w << JsonOutput.prettyPrint(JsonOutput.toJson(enumValueMappings))
}