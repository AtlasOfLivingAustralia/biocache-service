// https://mvnrepository.com/artifact/com.opencsv/opencsv
@Grapes(
    @Grab(group = 'com.opencsv', module = 'opencsv', version = '5.3')
)


import com.opencsv.bean.CsvToBeanBuilder
import com.opencsv.bean.CsvBindByName

import groovy.json.JsonSlurper

class SolrField {
    String name
    String type
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

    SolrField solrField
    SolrField pipelinesSolrField
}



def toCamelCase = { it.replaceAll("(_)([A-Za-z0-9])", { Object[] o -> o[2].toUpperCase() }) }

// parse the TDWG DwC term vocabulary csv into a map of DwcFields
Map<String, DwcField> dwcFields
'https://raw.githubusercontent.com/tdwg/dwc/master/vocabulary/term_versions.csv'.toURL().withReader { Reader reader ->

    dwcFields = new CsvToBeanBuilder(reader)
            .withType(DwcField.class)
            .build()
            .parse()
            .findAll { it.status == 'recommended' }
            .collectEntries { [ (it.name): it ]}
}

JsonSlurper slurper = new JsonSlurper()

def biocacheV2Fields = slurper.parse('https://biocache-ws.ala.org.au/ws/index/fields'.toURL())
def biocacheV2Schema = slurper.parse(new File('biocache-prod-solr-schema.json'))
def biocacheV3Schema = slurper.parse(new File('biocache-pipelines-solr-schema.json'))


// create Map of all solr fields keyed by field name
Map<String, SolrField> solrFields = biocacheV2Schema.schema.fields.collectEntries { [(it.name): new SolrField(name: it.name, type: it.type)] }
Map<String, SolrField> pipelinesSolrFields = biocacheV3Schema.schema.fields.collectEntries { [(it.name): new SolrField(name: it.name, type: it.type)] }

// process the biocache fields mapping to the solr schema fields
List<FieldMapping> fieldMappings = biocacheV2Fields.collect { field ->

    FieldMapping fieldMapping = new FieldMapping(biocacheField: new BiocacheField(field))

    fieldMapping.solrField = solrFields[field.name]
    solrFields.remove(field.name)

    return fieldMapping
}

// add all unmatched solr schema fields to the mapping object
fieldMappings += solrFields.collect { fieldName, solrField -> new FieldMapping(solrField: solrField) }

// process the all the current mapping searching for solr / pipelines schema fields
fieldMappings.each { FieldMapping fieldMapping ->

    // perform exact match on biocache field or solr field name
    String pipelinesSolrFieldName = fieldMapping.biocacheField?.name ?: fieldMapping.solrField.name
    SolrField pipelinesSolrField = pipelinesSolrFields[pipelinesSolrFieldName]

    if (pipelinesSolrField) {

        fieldMapping.pipelinesSolrField = pipelinesSolrField
        pipelinesSolrFields.remove(pipelinesSolrFieldName)

        return
    }

    // convert to camel case and perform match
    String camelField = toCamelCase(pipelinesSolrFieldName)

    pipelinesSolrField = pipelinesSolrFields[camelField]

    if (pipelinesSolrField) {

        fieldMapping.pipelinesSolrField = pipelinesSolrField
        pipelinesSolrFields.remove(camelField)

        return
    }
}

pipelinesSolrFields.each { fieldName, pipelinesSolrField ->

    FieldMapping unmatchedField = fieldMappings.find { FieldMapping fieldMapping ->

        fieldMapping.biocacheField && (fieldMapping.biocacheField.name.equalsIgnoreCase(fieldName) || toCamelCase(fieldMapping.biocacheField.name).equalsIgnoreCase(fieldName))
    }

    if (unmatchedField && !unmatchedField.pipelinesSolrField) {

        unmatchedField.pipelinesSolrField = pipelinesSolrFields[fieldName]

    } else {

        fieldMappings += new FieldMapping(pipelinesSolrField: pipelinesSolrField)
    }
}

biocacheV3Schema.schema.dynamicFields.each { dynamicField ->

    String regex = "^${dynamicField.name.replaceAll('\\*', '\\.\\*')}\$"

    fieldMappings.findAll { FieldMapping fieldMapping ->

        fieldMapping.solrField?.name ==~ ~/${regex}/

    }.each { FieldMapping fieldMapping ->

        if (!fieldMapping.pipelinesSolrField) {

            fieldMapping.pipelinesSolrField = new SolrField(name: dynamicField.name, type: dynamicField.type)
        }
    }
}

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

            dwcFieldName = fieldMapping.solrField?.name
            dwcField = dwcFields[dwcFieldName]

            if (dwcField) {

                fieldMapping.dwcField = dwcField
                dwcFields.remove(dwcFieldName)
            }
        }
    }
}

fieldMappings += dwcFields.collect { fieldName, dwcField -> new FieldMapping(dwcField: dwcField) }

println "\"Solr V8 (pipelines)\",,\"Solr v6\",,\"Biocache Index Fields\",,\"DwC Fields\""

print "\"field_name\","
print "\"field_type\","

print "\"field_name\","
print "\"field_type\","

print "\"dwc_term\","
print "\"term_iri\","
print "\"flags\","

print "\"name\","
print "\"jsonName\","
print "\"dwcTerm\","
print "\"downloadName\","
print "\"dataType\","
print "\"classs\","
print "indexed,"
print "docvalue,"
print "stored,"
print "multivalue,"
print "\"i18nValues\","
print "\"description\","
print "\"downloadDescription\","
print "\"info\","
print "\"infoUrl\""
println ''

fieldMappings.sort { lft, rgt ->

    String lftFieldName = lft.biocacheField?.name ?: (lft.solrField?.name ?: (lft.pipelinesSolrField?.name ?: lft.dwcField?.name))
    String rgtFieldName = rgt.biocacheField?.name ?: (rgt.solrField?.name ?: (rgt.pipelinesSolrField?.name ?: rgt.dwcField?.name))

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

}.each { FieldMapping fieldMapping ->


    print "\"${fieldMapping.pipelinesSolrField?.name ?: ''}\","
    print "\"${fieldMapping.pipelinesSolrField?.type ?: ''}\","

    print "\"${fieldMapping.solrField?.name ?: ''}\","
    print "\"${fieldMapping.solrField?.type ?: ''}\","

    print "\"${fieldMapping.dwcField?.name ?: ''}\","
    print "\"${fieldMapping.dwcField?.termIri ?: ''}\","
    print "\"${fieldMapping.dwcField?.flags ?: ''}\","

    print "\"${fieldMapping.biocacheField?.name ?: ''}\","
    print "\"${fieldMapping.biocacheField?.jsonName ?: ''}\","
    print "\"${fieldMapping.biocacheField?.dwcTerm ?: ''}\","
    print "\"${fieldMapping.biocacheField?.downloadName ?: ''}\","
    print "\"${fieldMapping.biocacheField?.dataType ?: ''}\","
    print "\"${fieldMapping.biocacheField?.classs ?: ''}\","
    print "${fieldMapping.biocacheField?.indexed ?: ''},"
    print "${fieldMapping.biocacheField?.docvalue ?: ''},"
    print "${fieldMapping.biocacheField?.stored ?: ''},"
    print "${fieldMapping.biocacheField?.multivalue ?: ''},"
    print "\"${fieldMapping.biocacheField?.i18nValues ?: ''}\","
    print "\"${fieldMapping.biocacheField?.description ?: ''}\","
    print "\"${fieldMapping.biocacheField?.downloadDescription ?: ''}\","
    print "\"${fieldMapping.biocacheField?.info ?: ''}\","
    print "\"${fieldMapping.biocacheField?.infoUrl ?: ''}\""

    println ''
}


/*
biocacheV2Schema.schema.dynamicFields.each {
	
	if (biocacheV3SchemaFields[it.name]) {
		println "\"${it.name}\",\"${it.name}\",${it.type},${biocacheV3SchemaFields[it.name]}"
        biocacheV3SchemaFields.remove(it.name)
	} else {

		 String camelField = it.name.replaceAll( "(_)([A-Za-z0-9])", { Object[] o -> o[2].toUpperCase() } )

		 if (biocacheV3SchemaFields[camelField]) {
         
            println "\"${it.name}\",\"${camelField}\",${it.type},${biocacheV3SchemaFields[camelField]}"
            biocacheV3SchemaFields.remove(camelField)
         
         } else {

         	camelField = camelField.replaceAll('Id', 'ID')
    
            if (biocacheV3SchemaFields[camelField]) {
         
                println "\"${it.name}\",\"${camelField}\",${it.type},${biocacheV3SchemaFields[camelField]}"
                biocacheV3SchemaFields.remove(camelField)
         
             } else {
        
                println "\"${it.name}\",\"\",${it.type},"
             }
        }
	}
}

biocacheV3SchemaFields.each { k, v ->
    println "\"\",\"${k}\",,${v}"
}
*/