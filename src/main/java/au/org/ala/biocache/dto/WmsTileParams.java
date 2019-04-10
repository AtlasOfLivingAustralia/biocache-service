package au.org.ala.biocache.dto;

public class WmsTileParams {

    String cache = "default";
    String[] hq;

    public String getCache() {
        return cache;
    }

    public void setCache(String cache) {
        this.cache = cache;
    }

    public String[] getHq() {
        return hq;
    }

    public void setHq(String[] hq) {
        this.hq = hq;
    }
}
