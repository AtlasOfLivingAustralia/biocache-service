package org.ala.biocache.dto;

import java.math.BigDecimal;

import org.apache.solr.client.solrj.response.FieldStatsInfo;

/**
 * A DTO for the statistics of a numeric field.  It stores the range information to be used in 
 * "automagic" range based queries.
 * @author Natasha Carter
 *
 */
public class StatsIndexFieldDTO {
    private FieldStatsInfo stats;
    private String dataType;
    private Number start;
    private Number end;
    private Number gap;
    public StatsIndexFieldDTO(){
      
    }
    public StatsIndexFieldDTO(FieldStatsInfo stats, String type){
        this.dataType = type;
        setStats(stats);        
    }
    /**
     * generates the range details to be used by this field.
     * 
     * Standard deviation is used to determine the precision of the start, end and gap.  It is
     * also used to determine the value of the gap.
     * 
     */
    private void generateRange(){
                
        if(stats.getStddev() > 1){
            BigDecimal sd = new BigDecimal(stats.getStddev());
            int sdint = sd.intValue();            
            int length = Integer.toString(sdint).length()-1;
            int mult = (int)Math.pow(10,length);
            int igap = Math.round(sdint/mult)*mult;
            long lstart = Math.round((Double)stats.getMin()/mult)*mult;
            long lend = Math.round((Double)stats.getMax()/mult)*mult;
            if(dataType.equals("int")){               
                gap = new Integer(igap);
                start = new Integer((int)lstart);
                end = new Integer((int)lend);
            }
            else{
                gap = new Double(igap);
                start = new Double(lstart);
                end = new Double(lend);
            }            
        }
        else{
          //TODO may need a better algorithm for this situation... But need to find a use case first.
            gap = new Double(0.5);
            start = (Double)stats.getMin();
            end = (Double)stats.getMax();
        }
        
    }
    public String toString(){
        String value = "stats: " + stats.toString() + "\n" +
                       "gap: " + gap + " start " + start + " end " + end; 
        return value;
    }
    /**
     * @return the stats
     */
    public FieldStatsInfo getStats() {
        return stats;
    }
    /**
     * @param stats the stats to set
     */
    public void setStats(FieldStatsInfo stats) {
        this.stats = stats;
        //now generate the range info
        generateRange();
    }
    /**
     * @return the dataType
     */
    public String getDataType() {
        return dataType;
    }
    /**
     * @param dataType the dataType to set
     */
    public void setDataType(String dataType) {
        this.dataType = dataType;
    }
    /**
     * @return the start
     */
    public Number getStart() {
        return start;
    }
    /**
     * @param start the start to set
     */
    public void setStart(Number start) {
        this.start = start;
    }
    /**
     * @return the end
     */
    public Number getEnd() {
        return end;
    }
    /**
     * @param end the end to set
     */
    public void setEnd(Number end) {
        this.end = end;
    }
    /**
     * @return the gap
     */
    public Number getGap() {
        return gap;
    }
    /**
     * @param gap the gap to set
     */
    public void setGap(Number gap) {
        this.gap = gap;
    }
    
    
}
