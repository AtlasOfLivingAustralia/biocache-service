package au.org.ala.biocache.web;

import org.apache.commons.lang.StringUtils;

/**
 * Created by mar759 on 22/09/2016.
 */
public class WMSUtils {

    public static String getQ(String cql_filter) {
        String q = cql_filter;
        int p1 = cql_filter.indexOf("qid:");
        if (p1 >= 0) {
            int p2 = cql_filter.indexOf('&', p1 + 1);
            if (p2 < 0) {
                p2 = cql_filter.indexOf(';', p1 + 1);
            }
            if (p2 < 0) {
                p2 = cql_filter.length();
            }
            q = cql_filter.substring(p1, p2);
        }
        return q;
    }

    public static String convertLayersParamToQ(String layers) {
        if (StringUtils.trimToNull(layers) != null) {
            String[] parts = layers.split(",");
            String[] formattedParts = new String[parts.length];
            int i = 0;
            for (String part : parts) {
                if (part.contains(":")) {
                    formattedParts[i] = part.replace('_', ' ').replace(":", ":\"") + "\"";
                } else if (part.startsWith("\"")) {
                    formattedParts[i] = "\"" + part + "\"";
                } else {
                    formattedParts[i] = part;
                }
                i++;
            }
            return StringUtils.join(formattedParts, " OR ");
        } else {
            return null;
        }
    }
}
