package au.org.ala.biocache.dto;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DownloadStats {

    ConcurrentMap<String, AtomicInteger> uidStats = new ConcurrentHashMap<>();

    Set<String> licences = new HashSet<>();

    public DownloadStats() {
    }

    public ConcurrentMap<String, AtomicInteger> getUidStats() {
        return uidStats;
    }

    public Set<String> getLicences() {
        return licences;
    }

    public void addLicence(String licence) {
        licences.add(licence);
    }
}
