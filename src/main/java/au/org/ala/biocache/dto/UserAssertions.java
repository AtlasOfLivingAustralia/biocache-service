package au.org.ala.biocache.dto;

import java.util.ArrayList;

public class UserAssertions extends ArrayList<QualityAssertion> {

    public UserAssertions() {
    }

    public void deleteUuid(String uuid) {
        for (int i = 0; i < this.size(); i++) {
            if (this.get(i).getUuid().equals(uuid)) {
                this.remove(i);
            }
        }
    }
}