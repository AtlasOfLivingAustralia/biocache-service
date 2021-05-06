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

@Component("fieldMappingUtil")
public class FieldMappingUtil {

    private Map<String, String> fieldMappings = new Hashtable<>();
    private Map<String, Map<String, String>> enumValueMappings = new Hashtable<>();

    @Value("${solr.pipelines.field.config:/data/biocache/config/pipelines-field-config.json}")
    void setPipelinesFieldConfig(String pipelinesFieldConfig) throws IOException {

        if (pipelinesFieldConfig != null && new File(pipelinesFieldConfig).exists()) {

            ObjectMapper om = new ObjectMapper();
            Map<String, Object> fieldConfig = om.readValue(new File(pipelinesFieldConfig), HashMap.class);

            fieldMappings = (Map<String, String>) fieldConfig.get("fieldNameMapping");
            enumValueMappings = (Map<String, Map<String, String>>) fieldConfig.get("fieldValueMapping");
        }
    }

    static Consumer<Pair<String, String>> NOOP_TRANSLATION = (Pair<String, String> m) -> {};

    static final String DEPRECATED_PREFIX = "deprecated_";
    static final Pattern ENUM_VALUE_PATTERN = Pattern.compile("(\\w+)");
    static final Pattern QUERY_TERM_PATTERN = Pattern.compile("(^|\\s|-|\\()(\\w+):");


    public Stream<Pair<String, String>> getFieldMappingStream() {

        return this.fieldMappings.entrySet().stream().map((Map.Entry<String, String> entry) -> Pair.of(entry.getKey(), entry.getValue()));
    }

    public String translateQueryFields(String query) {

        return translateQueryFields(NOOP_TRANSLATION, query);
    }

    public String translateQueryFields(Consumer<Pair<String, String>> translation, String query) {

        if (query == null) {
            return null;
        }

        if (this.fieldMappings == null) {
            return query;
        }

        Matcher matcher = QUERY_TERM_PATTERN.matcher(query);
        boolean result = matcher.find();

        if (result) {

            StringBuffer sb = new StringBuffer();

            String prevTerm = null;
            int prevEnd = 0;

            // loop through all matched pattern groups
            do {

                String prefix = matcher.group(1);
                String queryTerm = matcher.group(2);

                // default not found query terms to "null" character string to distinguish from null (deprecated) field mappings
                String translatedFieldName = this.fieldMappings.getOrDefault(queryTerm, "\0");

                if (translatedFieldName == null) {
                    // query term matched deprecated field
                    translatedFieldName = DEPRECATED_PREFIX + queryTerm;
                    translation.accept(Pair.of(queryTerm, null));

                } else if ("\0".equals(translatedFieldName)) {
                    // query term not found in field mappings
                    translatedFieldName = queryTerm;

                } else {
                    // query term has translated field name
                    translation.accept(Pair.of(queryTerm, translatedFieldName));
                }

                if (prevTerm != null) {
                    // collect the value between the end of the previous group and the start of the current
                    String value = query.substring(prevEnd, matcher.start());
                    sb.append(translateQueryValue(prevTerm, value));
                }

                // append the translated term with prefix
                sb.append(prefix);
                sb.append(translatedFieldName);
                sb.append(":");

                prevTerm = translatedFieldName;
                prevEnd = matcher.end();

                result = matcher.find();

            } while (result);

            // collect the term value after the last term match 
            String value = query.substring(prevEnd);
            sb.append(translateQueryValue(prevTerm, value));

            return sb.toString();
        }

        return query;
    }

    private String translateQueryValue(String term, String value) {

        if (enumValueMappings == null || term == null) {
            return value;
        }

        Map<String, String> enumValueMapping = enumValueMappings.get(term);
        if (enumValueMapping != null) {

            Matcher matcher = ENUM_VALUE_PATTERN.matcher(value);
            boolean result = matcher.find();

            if (result) {

                StringBuffer sb = new StringBuffer();

                do {

                    String enumValue = matcher.group(1);
                    String translatedEnumValue = enumValueMapping.get(enumValue);

                    if (translatedEnumValue == null) {
                        matcher.appendReplacement(sb, enumValue);
                    } else {
                        matcher.appendReplacement(sb, translatedEnumValue);
                    }

                    result = matcher.find();

                } while (result);

                matcher.appendTail(sb);

                return sb.toString();
            }
        }

        return value;
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
}
