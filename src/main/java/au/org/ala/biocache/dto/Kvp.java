package au.org.ala.biocache.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.Comparator;
import java.util.List;

/**
 * Species list key value pairs as a list of keys and a list of values.
 * <p>
 * There will be one of these for each species list item.
 * <p>
 * Species LSID is stored as left right values.
 */
@Schema(name="Species list key value pairs",
        description="Species list key value pairs as a list of keys and a list of values.")
public class Kvp {

    public long lft;
    public long rgt;

    public List<String> keys;
    public List<String> values;

    public static final java.util.Comparator<? super Kvp> KvpComparator = new Comparator<Kvp>() {
        @Override
        public int compare(Kvp o1, Kvp o2) {
            if (o1.lft != o2.lft) {
                return o1.lft < o2.lft ? -1 : 1;
            } else if (o1.rgt != o2.rgt) {
                // larger rgt values first
                return o1.rgt < o2.rgt ? 1 : -1;
            } else {
                return 0;
            }
        }
    };

    public Kvp(long lft, long rgt) {
        this.rgt = rgt;
        this.lft = lft;
    }

    public Kvp(long lft, long rgt, List<String> keys, List<String> values) {
        this.rgt = rgt;
        this.lft = lft;
        this.keys = keys;
        this.values = values;
    }

    public boolean contains(Kvp inner) {
        return this.lft <= inner.lft && this.rgt >= inner.rgt;
    }
}