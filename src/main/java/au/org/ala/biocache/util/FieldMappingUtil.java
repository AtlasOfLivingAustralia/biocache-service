package au.org.ala.biocache.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

@Component("fieldMappingUtil")
public class FieldMappingUtil {

    private BiMap<String, String> deprecatedFields;

    @Value("${solr.deprecated.fields.config:/data/biocache/config/deprecated-fields.json}")
    void setDeprecatedFieldsConfig(String deprecatedFieldsConfig) throws IOException {

        if (deprecatedFieldsConfig != null && new File(deprecatedFieldsConfig).exists()) {

            ObjectMapper om = new ObjectMapper();
            deprecatedFields = ImmutableBiMap.copyOf(om.readValue(new File(deprecatedFieldsConfig), Map.class));
        }
    }

    public SolrQuery newSolrQuery() {

        return new FieldMappedSolrQuery(this);
    }

    public QueryResponse wrapQueryResponse(QueryResponse queryResponse) {

        return new FieldMappedQueryResponse(queryResponse, this);
    }

    public String translateQueryFields(String query) {

        if (query == null) {
            return null;
        }

        return this.deprecatedFields.entrySet()
                .stream()
                .reduce(query,
                        (String transformedQuery, Map.Entry<String, String> deprecatedField) ->
                                transformedQuery.replaceAll("(^|\\s|-|\\()" + deprecatedField.getKey() + ":", "$1" + deprecatedField.getValue() + ":"),
                        (a, e) -> { throw new IllegalStateException("unable to combine deprecated field replacement, use sequential stream"); });
    }

    public Stream<Map.Entry<String, String>> asStream() {

        return this.deprecatedFields.entrySet().stream();
    }

    public String translateField(String fieldName) {

        if (fieldName == null) {
            return null;
        }

        String translatedFieldName = this.deprecatedFields.get(fieldName);

        return translatedFieldName != null ? translatedFieldName : fieldName;
    }

    public String[] translateFieldList(String fls) {

        if (fls == null) {
            return null;
        }

        return translateFieldArray(fls.split(","));
    }

    public String[] translateFieldArray(String ...fields) {

        if (fields == null) {
            return null;
        }

        return Arrays.stream(fields)
                .map(this::translateField)
                .toArray(String[]::new);
    }
}
