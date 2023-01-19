package au.org.ala.biocache.dto;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;

/**
 * 
 * A DTO for the parameters used in the breakdown services.
 * 
 * @author Natasha Carter
 */
@Schema(name = "Breakdown parameters")
public class BreakdownRequestParams extends SpatialSearchRequestParams {

    @Parameter(name="rank", description = "The taxonomic rank to use e.g. 'kingdom'")
    protected String rank = null;
    @Parameter(name="name", description = "The name of the root taxon to use e.g. 'Animalia'")
    protected String name = null;
    @Parameter(name="max", description = "The max number of child nodes to return")
    protected Integer max = null;
    @Parameter(name="level")
    protected String level = null;
    
    public String toString(){
        StringBuilder req = new StringBuilder(super.toString());
        if (rank != null) req.append("&rank=").append(rank);
        if (name != null) req.append("&name=").append(name);
        if (max != null) req.append("&max=").append(max);
        if (level != null) req.append("&level=").append(level);
        return req.toString();
    }
    
    /**
     * @return the rank
     */
    public String getRank() {
        return rank;
    }

    /**
     * @param rank the rank to set
     */
    public void setRank(String rank) {
        this.rank = rank;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }   

    /**
     * @return the max
     */
    public Integer getMax() {
        return max;
    }

    /**
     * @param max the max to set
     */
    public void setMax(Integer max) {
        this.max = max;
    }

    /**
     * @return the level
     */
    public String getLevel() {
        return level;
    }

    /**
     * @param level the level to set
     */
    public void setLevel(String level) {
        this.level = level;
    }
}
