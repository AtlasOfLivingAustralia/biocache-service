package au.org.ala.biocache.stream;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.io.Tuple;

import java.util.concurrent.atomic.AtomicInteger;

public class ScatterplotSearch implements ProcessInterface {

    private final static Logger logger = Logger.getLogger(ScatterplotSearch.class);

    int tmpCount = 0;
    AtomicInteger count;
    double[][] data;
    String x;
    String y;

    public ScatterplotSearch(double[][] data, String x, String y, AtomicInteger count) {
        this.data = data;
        this.x = x;
        this.y = y;
        this.count = count;
    }

    double lasta = 0;
    double lastb = 0;

    public boolean process(Tuple tuple) {
        if (tuple != null && tuple.fields != null && tuple.fields.size() >= 2) {
            try {
                Double a = tuple.getDouble(y);
                Double b = tuple.getDouble(x);
                if (tmpCount == 0 || lasta != a || lastb != b) {
                    data[0][tmpCount] = a;
                    data[1][tmpCount] = b;
                    lasta = a;
                    lastb = b;
                    tmpCount++;
                }
            } catch (Exception e) {
            }
        }
        return true;
    }

    public boolean flush() {
        count.set(tmpCount);

        int size = data[0].length;
        for (int i = tmpCount; i < size; i++) {
            data[0][i] = Double.NaN;
            data[1][i] = Double.NaN;
        }
        return true;
    }
}