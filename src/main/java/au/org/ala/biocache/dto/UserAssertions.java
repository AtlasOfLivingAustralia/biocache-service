package au.org.ala.biocache.dto;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;

public class UserAssertions extends ArrayList<QualityAssertion> {

    public UserAssertions() {
    }

    public boolean deleteUuid(String uuid) {
        boolean changed = false;
        for (int i = 0; i < this.size(); i++) {
            if (this.get(i).getUuid().equals(uuid)) {
                this.remove(i);
                changed = true;
                break;
            }
        }

        return changed;
    }

    public boolean updateComment(String uuid, String comment) {
        boolean changed = false;
        for (int i = 0; i < this.size(); i++) {
            if (this.get(i).getUuid().equals(uuid) && StringUtils.compare(this.get(i).comment, comment) != 0) {
                this.get(i).comment = comment;
                changed = true;
                break;
            }
        }

        return changed;
    }
}
