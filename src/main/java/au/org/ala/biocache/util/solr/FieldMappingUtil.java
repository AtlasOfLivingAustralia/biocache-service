package au.org.ala.biocache.util.solr;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;


public class FieldMappingUtil {

    private Map<String, String> fieldMappings;

    private Map<String, String> facetMap;
    private Map<String, String> facetRangeMap;

    static String DEPRECATED_PREFIX = "deprecated_";

    protected FieldMappingUtil(Map<String, String> fieldMappings) {

        this.fieldMappings = fieldMappings;
    }

    SolrParams translateSolrParams(SolrParams params) {

        System.out.println("before translation: " + params.toQueryString());

        facetMap = null;
        facetRangeMap = null;

        // TODO: PIPELINES: translate the params performing field mappings
        ModifiableSolrParams translatedSolrParams = ModifiableSolrParams.of(params);
        translatedSolrParams.getParameterNamesIterator().forEachRemaining((String paramName) -> {
            switch (paramName) {
                case "q":
                    translatedSolrParams.set("q", translateQueryFields((Map.Entry<String, String> mapping) -> {
                        System.out.println("query mapping: " + mapping.getKey() + " -> " + mapping.getValue());
                    }, translatedSolrParams.get("q")));
                    break;

                case "fl":
                    translatedSolrParams.set("fl", translateFieldArray((Map.Entry<String, String> mapping) -> {}, translatedSolrParams.getParams("fl")));
                    break;

                case "facet.field":
                    facetMap = new HashMap();
                    translatedSolrParams.set("facet.field", translateFieldArray((Map.Entry<String, String> mapping) -> facetMap.put(mapping.getKey(), mapping.getValue()), translatedSolrParams.getParams("facet.field")));
                    break;
                case "facet.range":
                    facetRangeMap = new HashMap<>();
                    translatedSolrParams.set("facet.range", translateFieldArray((Map.Entry<String, String> mapping) -> facetRangeMap.put(mapping.getKey(), mapping.getValue()), translatedSolrParams.getParams("facet.range")));
                    break;
            }
        });

        System.out.println("after translation: " + params.toQueryString());

        return translatedSolrParams;
    }

    QueryResponse translateQueryResponse(QueryResponse queryResponse) {

        // TODO: PIPELINES: translate the query response performing reverse field mappings
        return new FieldMappedQueryResponse(facetMap, queryResponse);
    }

    public String translateQueryFields(Consumer<Map.Entry<String, String>> translation, String query) {

        if (query == null) {
            return null;
        }

        return this.fieldMappings.entrySet()
                .stream()
                .reduce(query,
                        (String transformedQuery, Map.Entry<String, String> deprecatedField) -> {

                            // transformedQuery.replaceAll("(^|\\s|-|\\()" + deprecatedField.getKey() + ":", "$1" + deprecatedField.getValue() + ":"),

                            // loop through all the regex matcher results manually to allow translation callback
                            Matcher matcher = Pattern.compile("(^|\\s|-|\\()" + deprecatedField.getKey() + ":").matcher(transformedQuery);

                            boolean result = matcher.find();
                            if (result) {

                                StringBuffer sb = new StringBuffer();

                                do {
                                    if (deprecatedField.getValue() == null) {
                                        matcher.appendReplacement(sb, "$1" + DEPRECATED_PREFIX + deprecatedField.getKey() + ":");
                                    } else {
                                        matcher.appendReplacement(sb, "$1" + deprecatedField.getValue() + ":");
                                    }
                                    translation.accept(deprecatedField);
                                    result = matcher.find();
                                } while (result);

                                matcher.appendTail(sb);
                                return sb.toString();
                            }

                            return transformedQuery;
                        },
                        (a, e) -> { throw new IllegalStateException("unable to combine deprecated field replacement, use sequential stream"); });

    }

    public String translateFieldName(Consumer<Map.Entry<String, String>> translation, String fieldName) {

        if (fieldName == null) {
            return null;
        }

        String translatedFieldName = this.fieldMappings.getOrDefault(fieldName, "\0");

        if (translatedFieldName == null || !"\0".equals(translatedFieldName)) {

            translation.accept(Maps.immutableEntry(fieldName, translatedFieldName));
            return translatedFieldName;
        }

        return fieldName;
    }

    public String inverseFieldName(String fieldName) {

        if (fieldName == null) {
            return null;
        }

        return null;
//        String translatedFieldName = this.inverseFieldMappings.get(fieldName);
//
//        return translatedFieldName != null ? translatedFieldName : fieldName;
    }

    public String[] translateFieldList(Consumer<Map.Entry<String, String>> translation, String fls) {

        if (fls == null) {
            return null;
        }

        return translateFieldArray(translation, fls.split(","));
    }

    public String[] translateFieldArray(Consumer<Map.Entry<String, String>> translation, String ...fields) {

        if (fields == null) {
            return null;
        }

        return Arrays.stream(fields)
                .map((String field) -> translateFieldName(translation, field))
                .filter((String field) -> field != null && !field.equals(""))
                .toArray(String[]::new);
    }

    @Component("fieldMappingUtilBuilder")
    public static class Builder {

        private Map<String, String> fieldMappings;
//        private Map<String, String> inverseFieldMappings;

        @Value("${solr.deprecated.fields.config:/data/biocache/config/deprecated-fields.json}")
        void setDeprecatedFieldsConfig(String deprecatedFieldsConfig) throws IOException {

            if (deprecatedFieldsConfig != null && new File(deprecatedFieldsConfig).exists()) {

                ObjectMapper om = new ObjectMapper();
                fieldMappings = om.readValue(new File(deprecatedFieldsConfig), HashMap.class);

//                inverseFieldMappings = fieldMappings.entrySet()
//                        .stream()
//                        .filter((Map.Entry<String, String> fieldMapping) -> fieldMapping.getValue() != null)
//                        .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
            }
        }

        FieldMappingUtil newInstance() {

            return new FieldMappingUtil(fieldMappings);
        }

        public Map<String, String> getFieldMappings() {
            return Collections.unmodifiableMap(fieldMappings);
        }

        public Stream<Map.Entry<String, String>> getFieldMappingStream() {

            return this.fieldMappings.entrySet().stream();
        }
    }
}
