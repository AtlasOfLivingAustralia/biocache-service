@GrabResolver(name='org.gbif', root='http://repository.gbif.org/content/groups/gbif')
@Grapes([
//        @GrabConfig(systemClassLoader=true),
        @Grab(group='org.apache.httpcomponents', module='httpmime', version='4.3.1'),
        @Grab(group = 'org.codehaus.groovy.modules.http-builder', module='http-builder', version='0.7.1'),
])

import groovy.json.JsonSlurper

if (args.length != 1) {
    println "usage: update-assertions-wiki <wiki source dir>"
    return
}

String wikiSourceDir = args[0]

File wikiSourceFile = new File(wikiSourceDir)

if (!(wikiSourceFile.exists() && wikiSourceFile.isDirectory())) {
    println "${wikiSourceDir} not a valid directory"
    return
}

def toSnakeCase = { it.replaceAll( /([A-Z])/, /_$1/ ).toUpperCase() }

JsonSlurper slurper = new JsonSlurper()

def assertionCodes = slurper.parse('https://biocache-ws.ala.org.au/ws/assertions/codes?deprecated=true'.toURL())
def biocacheFields = slurper.parse('https://biocache-ws.ala.org.au/ws/index/fields'.toURL())

def deprecates = [:]

assertionCodes.each { assertion ->
    if (assertion.deprecated && assertion.newName) {
        deprecates[assertion.newName] = assertion.name
    }
}

def termNames = [:]

biocacheFields.each { indexField ->

    termNames[indexField.name] = indexField.dwcTerm ? ( indexField.dwcTerm.contains(':') ? indexField.dwcTerm : "dwc:${indexField.dwcTerm}" )  : indexField.name
}

assertionCodes
        .findAll { assertion -> !assertion.deprecated }
        .each { assertion ->

            String occurrenceIssue = assertion.code < 2000 ? "ALAOccurrenceIssue.${assertion.name}" : "OccurrenceIssue.${assertion.name}"

            String deprecatesLink

            String deprecatedAssertion = deprecates[assertion.name]

            if (deprecatedAssertion) {

                String oldAssertionWiki

                if (deprecatedAssertion == assertion.name || toSnakeCase(deprecatedAssertion) == assertion.name) {
                    // don't create a wiki link id the deprecated assertion points to the this assertion
                } else if (new File("${wikiSourceDir}/${deprecatedAssertion}.md").exists()) {
                    oldAssertionWiki = deprecatedAssertion
                } else if (new File("${wikiSourceDir}/${toSnakeCase(deprecatedAssertion)}.md").exists()) {
                    oldAssertionWiki = toSnakeCase(deprecatedAssertion)
                }

                if (deprecatedAssertion) {

                    deprecatesLink = oldAssertionWiki ? "[${deprecatedAssertion}](./${oldAssertionWiki})" : deprecatedAssertion
                }
            }

            File wikiPage = new File("${wikiSourceDir}/${assertion.name}.md")

            if (!wikiPage.exists()) {

                println "No wiki page for assertion: ${assertion.name}"

                wikiPage.withPrintWriter { out ->

                    out.println '# Description'
                    out.println "${assertion.description}"
                    out.println ''

                    out.println '# Related fields'
                    assertion.termsRequiredToTest.each { field ->
                        out.println "* ${termNames[field]}"
                    }
                    out.println ''

                    out.println '# Implications and caveats'
                    out.println "**Severity: ${assertion.category}**"
                    out.println ''

                    out.println '# Data Custodian Recommendations'
                    out.println ''

                    out.println '# Code reference'
                    out.println "[${occurrenceIssue}](https://github.com/search?q=repo%3Agbif%2Fpipelines+in%3Afile+language%3Ajava+${assertion.name}&type=Code)"

                    if (deprecatesLink) {

                        out.println ''
                        out.println '# Replaces'
                        out.println deprecatesLink
                    }
                }

            } else {

                boolean processSection = false

                File newFile = File.createTempFile("test", UUID.randomUUID().toString())

                newFile.withPrintWriter { out ->

                    wikiPage.eachLine { line ->

                        if (line.startsWith('#')) {
                            processSection = false
                        }

                        if (line == '# Related fields') {

                            out.println '# Related fields'
                            assertion.termsRequiredToTest.each { field ->
                                out.println "* ${termNames[field] ?: field}"
                            }
                            out.println ''

                            processSection = true
                        }
                        else if (line == '# Code reference') {

                            out.println '# Code reference'
                            out.println "[${occurrenceIssue}](https://github.com/search?q=repo%3Agbif%2Fpipelines+in%3Afile+language%3Ajava+${assertion.name}&type=Code)"
                            out.println ''
                            processSection = true
                        }
                        else if (line == '# Replaces') {

                            if (deprecatesLink) {

                                out.println '# Replaces'
                                out.println deprecatesLink
                                out.println ''
                                processSection = true
                            }
                        }
                        else if (!processSection) {

                            out.println line
                        }
                    }
                }
                wikiPage.delete()
                newFile.renameTo(wikiPage)
            }
        }