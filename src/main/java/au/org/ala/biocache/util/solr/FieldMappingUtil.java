package au.org.ala.biocache.util.solr;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;
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

    static Consumer<Pair<String, String>> NOOP_TRANSLATION = (Pair<String, String> m) -> {};

    static String DEPRECATED_PREFIX = "deprecated_";
    static Pattern QUERY_TERM_PATTERN = Pattern.compile("(^|\\s|-|\\()(\\w+):");

    static private ThreadLocal<Boolean> disableMapping = new ThreadLocal<>();

    protected FieldMappingUtil(Map<String, String> fieldMappings) {

        this.fieldMappings = fieldMappings;
    }

    static public void disableMapping() {
        disableMapping.set(true);
    }

    static public boolean isMappingDisabled() {

        Boolean disabled = disableMapping.get();
        disableMapping.set(false);

        return disabled == null ? false : disabled;
    }
/*
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
*/
//    QueryResponse translateQueryResponse(QueryResponse queryResponse) {
//
//        // TODO: PIPELINES: translate the query response performing reverse field mappings
//        return new FieldMappedQueryResponse(facetMap, queryResponse);
//    }

    public String translateQueryFields(String query) {

        return translateQueryFields(NOOP_TRANSLATION, query);
    }

    public String translateQueryFields(Consumer<Pair<String, String>> translation, String query) {

        if (query == null) {
            return null;
        }

        Matcher matcher = QUERY_TERM_PATTERN.matcher(query);
        boolean result = matcher.find();

        if (result) {

            StringBuffer sb = new StringBuffer();

            do {

                String queryTerm = matcher.group(2);

                // default not found query terms to "null" character string to distinguish from null (deprecated) field mappings
                String translatedFieldName = this.fieldMappings.getOrDefault(queryTerm, "\0");

                if (translatedFieldName == null) {
                    // query term matched deprecated field
                    matcher.appendReplacement(sb, "$1" + DEPRECATED_PREFIX + queryTerm + ":");
                    translation.accept(Pair.of(queryTerm, null));

                } else if ("\0".equals(translatedFieldName)) {
                    // query term not found in field mappings
                    matcher.appendReplacement(sb, "$1" + queryTerm + ":");
                    Pair.of(queryTerm, queryTerm);

                } else {
                    // query term has translated field name
                    matcher.appendReplacement(sb, "$1" + translatedFieldName + ":");
                    translation.accept(Pair.of(queryTerm, translatedFieldName));
                }

                result = matcher.find();

            } while (result);

            matcher.appendTail(sb);
            return sb.toString();
        }

        return query;

        /* this is the bruit force way of translating query terms
         * loop through all mappings and try each one, this should be slower then the above implementation
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
                                    String requestedField = m.group(1);

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
        */
    }

    public String translateFieldName(String fieldName) {

        return translateFieldName(NOOP_TRANSLATION, fieldName);
    }

    public String translateFieldName(Consumer<Pair<String, String>> translation, String fieldName) {

        if (fieldName == null) {
            return null;
        }

        String translatedFieldName = this.fieldMappings.getOrDefault(fieldName, "\0");

        if (translatedFieldName == null) {

            translation.accept(Pair.of(fieldName, null));
            return DEPRECATED_PREFIX + fieldName;

        } else if (!"\0".equals(translatedFieldName)) {

            translation.accept(Pair.of(fieldName, translatedFieldName));
            return translatedFieldName;
        }

        translation.accept(Pair.of(fieldName, fieldName));
        return fieldName;
    }

    public String[] translateFieldList(String ...fls) {

        return translateFieldList(NOOP_TRANSLATION, fls);
    }

    public String[] translateFieldList(Consumer<Pair<String, String>> translation, String ...fls) {

        if (fls == null) {
            return null;
        }

        return Arrays.stream(fls)
                .map((String fl) -> String.join(",", translateFieldArray(translation, fl.split(","))))
                .toArray(String[]::new);


//        return translateFieldArray(translation, fls.split(","));
    }

    public String[] translateFieldArray(String ...fields) {

        return translateFieldArray(NOOP_TRANSLATION, fields);
    }

    public String[] translateFieldArray(Consumer<Pair<String, String>> translation, String ...fields) {

        if (fields == null) {
            return null;
        }

        return Arrays.stream(fields)
                .filter((String field) -> field != null && !field.equals(""))
                .map((String field) -> translateFieldName(translation, field))
                .toArray(String[]::new);
    }

    @Component("fieldMappingUtilBuilder")
    public static class Builder {

        private Map<String, String> fieldMappings;

        @Value("${solr.deprecated.fields.config:/data/biocache/config/deprecated-fields.json}")
        void setDeprecatedFieldsConfig(String deprecatedFieldsConfig) throws IOException {

            if (deprecatedFieldsConfig != null && new File(deprecatedFieldsConfig).exists()) {

                ObjectMapper om = new ObjectMapper();
                fieldMappings = om.readValue(new File(deprecatedFieldsConfig), HashMap.class);
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
