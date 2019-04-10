package au.org.ala.biocache.dto;


public class WmsCapabilitiesFilter {
    boolean spatiallyValidOnly = true;
    boolean marineOnly = false;
    boolean terrestrialOnly = false;
    boolean limitToFocus = false;
    boolean useSpeciesGroups = false;

    public boolean isSpatiallyValidOnly() {
        return spatiallyValidOnly;
    }

    public void setSpatiallyValidOnly(boolean spatiallyValidOnly) {
        this.spatiallyValidOnly = spatiallyValidOnly;
    }

    public boolean isMarineOnly() {
        return marineOnly;
    }

    public void setMarineOnly(boolean marineOnly) {
        this.marineOnly = marineOnly;
    }

    public boolean isTerrestrialOnly() {
        return terrestrialOnly;
    }

    public void setTerrestrialOnly(boolean terrestrialOnly) {
        this.terrestrialOnly = terrestrialOnly;
    }

    public boolean isLimitToFocus() {
        return limitToFocus;
    }

    public void setLimitToFocus(boolean limitToFocus) {
        this.limitToFocus = limitToFocus;
    }

    public boolean isUseSpeciesGroups() {
        return useSpeciesGroups;
    }

    public void setUseSpeciesGroups(boolean useSpeciesGroups) {
        this.useSpeciesGroups = useSpeciesGroups;
    }
}
