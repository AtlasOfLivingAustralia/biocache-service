package au.org.ala.biocache.stream;

import org.apache.solr.client.solrj.io.Tuple;
import org.locationtech.jts.math.Vector2D;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class ScatterplotSearch implements ProcessInterface {

    AtomicInteger count;
    Set<Vector2D> data;
    String x;
    String y;

    public ScatterplotSearch(Set<Vector2D> data, String x, String y, AtomicInteger count) {
        this.data = data;
        this.x = x;
        this.y = y;
        this.count = count;
    }

    public boolean process(Tuple tuple) {
        if (tuple != null && tuple.fields != null && tuple.fields.size() >= 2) {
            try {
                Double a = tuple.getDouble(y);
                Double b = tuple.getDouble(x);
                data.add(Vector2D.create(a, b));
            } catch (Exception e) {
            }
        }

        return true;
    }

    public boolean flush() {
        count.set(data.size());

        return true;
    }
}