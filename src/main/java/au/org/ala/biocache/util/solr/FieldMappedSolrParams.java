package au.org.ala.biocache.util.solr;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class FieldMappedSolrParams extends SolrParams {

    /**
     * log4j logger
     */
    private static final Logger logger = Logger.getLogger(FieldMappedSolrParams.class);

    final SolrParams originalParams;
    ModifiableSolrParams translatedSolrParams;

    Map<String, Map<String, String[]>> paramsInverseTranslations = new HashMap();

    FieldMappedSolrParams(FieldMappingUtil fieldMappingUtil, SolrParams solrParams) {

        originalParams = solrParams;

        translateSolrParams(fieldMappingUtil, solrParams);
    }

    private void translateSolrParams(FieldMappingUtil fieldMappingUtil, SolrParams solrParams) {

        logger.debug("before translation: " + solrParams.toQueryString());

        translatedSolrParams = new ModifiableSolrParams();

        // TODO: PIPELINES: translate the params performing field mappings
        solrParams.getParameterNamesIterator().forEachRemaining((String paramName) -> {

            Map<String, String[]> inverseTranslations = new HashMap();

            Consumer<Pair<String, String>> addParamTranslation = (Pair<String, String> mapping) -> {

                String[] old = inverseTranslations.put(mapping.getRight(), new String[]{ mapping.getLeft() });

                if (old != null) {

                    String[] both;
                    both = new String[old.length + 1];
                    System.arraycopy(old, 0, both, 0, old.length);
                    both[old.length] = mapping.getLeft();
                    inverseTranslations.put(mapping.getRight(), both);
                }
            };

            switch (paramName) {
                case "q":
                case "fq":

                    translatedSolrParams.set(paramName,
                            Arrays.stream(solrParams.getParams(paramName))
                                    .map((String param) -> fieldMappingUtil.translateQueryFields(addParamTranslation, param))
                                    .toArray(String[]::new)
                    );

                    break;

                case "fl":

                    translatedSolrParams.set(paramName, fieldMappingUtil.translateFieldList(addParamTranslation, solrParams.getParams(paramName)));
                    break;

                case "sort":

                    String[] translatedParams = Arrays.stream(solrParams.getParams(paramName))
                            .map((String sortFields) ->
                                    Arrays.stream(sortFields.split(","))
                                            .map((String sortField) -> {

                                                // split the sort param (first param is field name, second is sort order)
                                                String[] sortParams = sortField.split("\\s+", 2);

                                                if (sortParams.length < 2) {
                                                    return sortField;
                                                }

                                                String translatedField = fieldMappingUtil.translateFieldName(sortParams[0]);
                                                return translatedField + " " + sortParams[1];
                                            })
                                            .collect(Collectors.joining( "," ))
                            )
                            .toArray(String[]::new);

                    translatedSolrParams.set(paramName, translatedParams);
                    break;

                case "facet.field":
                case "facet.range":

                    translatedSolrParams.set(paramName, fieldMappingUtil.translateFieldArray(addParamTranslation, solrParams.getParams(paramName)));
                    break;

                default:
                    translatedSolrParams.set(paramName, solrParams.getParams(paramName));
            }

            if (inverseTranslations.size() > 0) {
                paramsInverseTranslations.put(paramName, inverseTranslations);
            }
        });

        logger.debug("after translation: " + translatedSolrParams.toQueryString());
    }

    @Override
    public String get(String s) {
        return translatedSolrParams.get(s);
    }

    @Override
    public String[] getParams(String s) {
        return translatedSolrParams.getParams(s);
    }

    @Override
    public Iterator<String> getParameterNamesIterator() {
        return translatedSolrParams.getParameterNamesIterator();
    }
}
