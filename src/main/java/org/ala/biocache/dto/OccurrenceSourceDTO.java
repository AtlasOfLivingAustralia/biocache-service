
package org.ala.biocache.dto;

/**
 * Stores the information about the source of an occurrence
 *
 * @author Natasha
 */
public class OccurrenceSourceDTO {
    private String name;
    private String uid;
    private int count;

    public OccurrenceSourceDTO() {}

    public OccurrenceSourceDTO(String name, String uid, int count) {
        this.name = name;
        this.uid = uid;
        this.count = count;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
