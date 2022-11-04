package au.org.ala.biocache.util;

import au.org.ala.biocache.dao.QidCacheDAO;
import au.org.ala.biocache.dto.SpatialSearchRequestDTO;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

@Component("wmsUtils")
public class WMSUtils {

    @Inject
    protected QidCacheDAO qidCacheDAO;

    public String[] getFq(SpatialSearchRequestDTO requestParams) {
        int requestParamsFqLength = requestParams.getFq() != null ? requestParams.getFq().length : 0;

        String[] qidFq = null;
        int qidFqLength = 0;
        String q = requestParams.getQ();
        if (q.startsWith("qid:")) {
            try {
                qidFq = qidCacheDAO.get(q.substring(4)).getFqs();
                if (qidFq != null) {
                    qidFqLength = qidFq.length;
                }
            } catch (Exception e) {
            }
        }

        if (requestParamsFqLength + qidFqLength == 0) {
            return null;
        }

        String[] allFqs = new String[requestParamsFqLength + qidFqLength];

        if (requestParamsFqLength > 0) {
            System.arraycopy(requestParams.getFq(), 0, allFqs, 0, requestParamsFqLength);
        }

        if (qidFqLength > 0) {
            System.arraycopy(qidFq, 0, allFqs, requestParamsFqLength, qidFqLength);
        }

        return allFqs;
    }

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
