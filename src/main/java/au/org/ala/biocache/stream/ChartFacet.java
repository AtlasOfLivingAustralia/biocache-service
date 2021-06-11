package au.org.ala.biocache.stream;

import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dao.SearchDAOImpl;
import au.org.ala.biocache.dto.FieldResultDTO;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.io.Tuple;
import org.springframework.aop.framework.Advised;

import java.util.List;

public class ChartFacet implements ProcessInterface {

    List data;
    boolean xmissing;
    SearchDAO searchDao;
    String facet;

    public ChartFacet(List data, Boolean xmissing, String facet, SearchDAO searchDao) {
        this.data = data;
        this.xmissing = xmissing;
        this.facet = facet;
        this.searchDao = searchDao;
    }

    @Override
    public boolean process(Tuple t) {
        long count = 0;
        String name = null;
        for (Object o : t.getMap().values()) {
            if (o instanceof Long) {
                count = (Long) o;
            } else {
                name = String.valueOf(o);
            }
        }
        if (xmissing || StringUtils.isNotEmpty(name)) {
            try {
//                data.add(new FieldResultDTO(((SearchDAOImpl) ((Advised) searchDao).getTargetSource().getTarget()).getFacetValueDisplayName(facet, name),
//                        facet + "." + count,
//                        count,
//                        ((SearchDAOImpl) ((Advised) searchDao).getTargetSource().getTarget()).getFormattedFqQuery(facet, name)));
            } catch (Exception e) {
                // TODO
            }
        }
        return true;
    }

    @Override
    public boolean flush() {
        return true;
    }
}
