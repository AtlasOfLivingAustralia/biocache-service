package au.org.ala.names.model;


import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang3.StringUtils;

/**
 * A model object that represents a Linnaean Classification.
 */
public class LinnaeanRankClassification {
    protected String kingdom;
    protected String kid;
    protected String phylum;
    protected String pid;
    protected String klass;
    protected String cid;
    protected String order;
    protected String oid;
    protected String family;
    protected String fid;
    protected String genus; // Aus
    protected String gid;
    protected String species; // Aus bus
    protected String sid;
    protected String specificEpithet; // bus
    protected String subspecies; // Aus bus cus
    protected String infraspecificEpithet; // cus
    protected String scientificName;
    protected String authorship;
    protected String rank;

    public LinnaeanRankClassification() {
    }

    public LinnaeanRankClassification(String kingdom, String phylum,
                                      String klass, String order, String family, String genus,
                                      String species, String specificEpithet, String subspecies, String infraspecificEpithet,
                                      String scientificName) {
        this.kingdom = kingdom;
        this.phylum = phylum;
        this.klass = klass;
        this.order = order;
        this.family = family;
        this.genus = genus;
        this.species = species;
        this.specificEpithet = specificEpithet;
        this.subspecies = subspecies;
        this.infraspecificEpithet = infraspecificEpithet;
        this.scientificName = scientificName;
    }
    public LinnaeanRankClassification(LinnaeanRankClassification cl){
        this.kingdom = cl.kingdom;
        this.kid = cl.kid;
        this.phylum = cl.phylum;
        this.pid = cl.pid;
        this.klass = cl.klass;
        this.kid = cl.cid;
        this.order = cl.order;
        this.oid = cl.oid;
        this.family = cl.family;
        this.fid = cl.fid;
        this.genus = cl.genus;
        this.gid = cl.gid;
        this.species = cl.species;
        this.sid = cl.sid;
        this.specificEpithet = cl.specificEpithet;
        this.subspecies = cl.subspecies;
        this.infraspecificEpithet = cl.infraspecificEpithet;
        this.authorship = cl.authorship;
        this.scientificName = cl.scientificName;
        this.rank = cl.rank;
    }

    /**
     * @param kingdom
     * @param phylum
     * @param klass
     * @param order
     * @param family
     * @param genus
     * @param scientificName
     */
    public LinnaeanRankClassification(String kingdom, String phylum, String klass, String order, String family, String genus, String scientificName) {
        setKingdom(kingdom);
        setPhylum(phylum);
        setKlass(klass);
        setOrder(order);
        setFamily(family);
        setGenus(genus);
        this.scientificName = scientificName;
    }

    /**
     * The constructor was added for convenience.
     *
     * @param kingdom
     * @param genus
     */
    public LinnaeanRankClassification(String kingdom, String genus) {
        this(kingdom, null, null, null, null, genus, null);
    }

    /**
     * @return Returns the family.
     */
    public String getFamily() {
        return family;
    }

    /**
     * @param family The family to set.
     */
    public void setFamily(String family) {
        this.family = family;
    }

    /**
     * @return Returns the genus.
     */
    public String getGenus() {
        return genus;
    }

    /**
     * @param genus The genus to set.
     */
    public void setGenus(String genus) {
        this.genus = genus;
    }

    /**
     * @return Returns the kingdom.
     */
    public String getKingdom() {
        return kingdom;
    }

    /**
     * @param kingdom The kingdom to set.
     */
    public void setKingdom(String kingdom) {
        this.kingdom = kingdom;
    }

    /**
     * @return Returns the klass.
     */
    public String getKlass() {
        return klass;
    }

    /**
     * @param klass The klass to set.
     */
    public void setKlass(String klass) {
        this.klass = klass;
    }

    /**
     * @return Returns the order.
     */
    public String getOrder() {
        return order;
    }

    /**
     * @param order The order to set.
     */
    public void setOrder(String order) {
        this.order = order;
    }

    /**
     * @return Returns the phylum.
     */
    public String getPhylum() {
        return phylum;
    }

    /**
     * @param phylum The phylum to set.
     */
    public void setPhylum(String phylum) {
        this.phylum = phylum;
    }

    /**
     * @return Returns the scientificName.
     */
    public String getScientificName() {
        return scientificName;
    }

    /**
     * @param scientificName The scientificName to set.
     */
    public void setScientificName(String scientificName) {
        this.scientificName = scientificName;
    }

    public String getSpecies() {
        return species;
    }

    public void setSpecies(String species) {
        this.species = species;
    }

    /**
     * @return the specificEpithet
     */
    public String getSpecificEpithet() {
        return specificEpithet;
    }

    /**
     * @param specificEpithet the specificEpithet to set
     */
    public void setSpecificEpithet(String specificEpithet) {
        this.specificEpithet = specificEpithet;
    }

    /**
     * @return the infraspecificEpithet
     */
    public String getInfraspecificEpithet() {
        return infraspecificEpithet;
    }

    /**
     * @param infraspecificEpithet the infraspecificEpithet to set
     */
    public void setInfraspecificEpithet(String infraspecificEpithet) {
        this.infraspecificEpithet = infraspecificEpithet;
    }

    /**
     * @return the subspecies
     */
    public String getSubspecies() {
        return subspecies;
    }

    /**
     * @param subspecies the subspecies to set
     */
    public void setSubspecies(String subspecies) {
        this.subspecies = subspecies;
    }

    /**
     * @return the identification for the kingdom, either a CB ID or GUID
     */
    public String getKid() {
        return kid;
    }

    public void setKid(String kid) {
        this.kid = kid;
    }

    /**
     * @return the identification for the phylum, either a CB ID or GUID
     */
    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    /**
     * @return the identification for the class, either a CB ID or GUID
     */
    public String getCid() {
        return cid;
    }

    public void setCid(String cid) {
        this.cid = cid;
    }

    /**
     * @return the identification for the order, either a CB ID or GUID
     */
    public String getOid() {
        return oid;
    }

    public void setOid(String oid) {
        this.oid = oid;
    }

    public String getFid() {
        return fid;
    }

    public void setFid(String fid) {
        this.fid = fid;
    }

    public String getGid() {
        return gid;
    }

    public void setGid(String gid) {
        this.gid = gid;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public String getAuthorship() {
        return authorship;
    }

    public void setAuthorship(String authorship) {
        this.authorship = authorship;
    }

    public String getRank() {
        return rank;
    }

    public void setRank(String rank) {
        this.rank = rank;
    }

    /**
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return new ToStringBuilder(this)
                .append("kingdom", this.kingdom)
                .append("phylum", this.phylum)
                .append("klass", this.klass)
                .append("order", this.order)
                .append("family", this.family)
                .append("genus", this.genus)
                .append("species", this.species)
                .append("specificEpithet", this.specificEpithet)
                .append("subspecies", this.subspecies)
                .append("infraspecificEpithet", this.infraspecificEpithet)
                .append("scientificName", this.scientificName)
                .toString();
    }

    public String toCSV(char sep) {
        StringBuilder sb = new StringBuilder();
        sb.append("\"").append(authorship).append("\"").append(sep)
                .append(this.kingdom).append(sep).append(this.phylum).append(sep)
                .append(this.klass).append(sep).append(this.order).append(sep)
                .append(this.family).append(sep).append(genus);
        return sb.toString();
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object object) {
        if (!(object instanceof LinnaeanRankClassification)) {
            return false;
        }
        LinnaeanRankClassification rhs = (LinnaeanRankClassification) object;

        return new EqualsBuilder().append(
                this.scientificName, rhs.scientificName).append(this.phylum,
                rhs.phylum).append(this.kingdom, rhs.kingdom).append(
                this.family, rhs.family).append(this.order, rhs.order).append(
                this.klass, rhs.klass).append(this.genus, rhs.genus).append(this.species, rhs.species)
                .append(this.specificEpithet, rhs.specificEpithet).append(this.subspecies, rhs.subspecies)
                .append(this.infraspecificEpithet, rhs.infraspecificEpithet).isEquals();
    }

    /**
     * checks to see if the non-null values of this classification are identical to
     * the supplied classification
     * <p/>
     * This is used to determine whether classification match. Thus we are not using the scientific name.
     * <p/>
     * Use this method to determine whether or not a search result (lrc) matches the supplied classification.
     *
     * @param lrc
     * @return
     */
    public boolean hasIdenticalClassification(LinnaeanRankClassification lrc, RankType matchLevel) {
        if (kingdom != null && matchLevel.getId() >= RankType.KINGDOM.getId()) {
            if (!kingdom.equalsIgnoreCase(lrc.kingdom))
                return false;
        }
        if (phylum != null && matchLevel.getId() >= RankType.PHYLUM.getId()) {
            if (!phylum.equalsIgnoreCase(lrc.phylum))
                return false;
        }
        if (klass != null && matchLevel.getId() >= RankType.CLASS.getId()) {
            if (!klass.equalsIgnoreCase(lrc.klass))
                return false;
        }
        if (order != null && matchLevel.getId() >= RankType.ORDER.getId()) {
            if (!order.equalsIgnoreCase(lrc.order))
                return false;
        }
        if (family != null && matchLevel.getId() >= RankType.FAMILY.getId()) {
            if (!family.equalsIgnoreCase(lrc.family))
                return false;
        }
        if (genus != null && matchLevel.getId() >= RankType.GENUS.getId()) {
            if (!genus.equalsIgnoreCase(lrc.genus))
                return false;
        }
        if (species != null && matchLevel.getId() >= RankType.SPECIES.getId()) {
            if (!species.equalsIgnoreCase(lrc.species))
                return false;
        }
        if (subspecies != null && matchLevel.getId() >= RankType.SUBSPECIES.getId()) {
            if (!subspecies.equalsIgnoreCase(lrc.subspecies))
                return false;
        }
            /*if(specificEpithet != null){
                if(!specificEpithet.equalsIgnoreCase(lrc.specificEpithet))
                    return false;
            }
            if(infraspecificEpithet != null){
                if(!infraspecificEpithet.equalsIgnoreCase(lrc.infraspecificEpithet))
                    return false;
            }*/

        return true;
    }

    /**
     * Returns the additional string that needs to be included in a search
     *
     * @param optional Indicates whether the the terms should be optional
     * @return
     */
    public String getLuceneSearchString(boolean optional) {
        String prefix = optional ? " " : " +";
        StringBuilder sb = new StringBuilder();
        if (StringUtils.isNotEmpty(kingdom))
            sb.append(prefix).append(RankType.KINGDOM.getRank()).append(":\"").append(kingdom).append("\"");
        if (StringUtils.isNotEmpty(phylum))
            sb.append(prefix).append(RankType.PHYLUM.getRank()).append(":\"").append(phylum).append("\"");
        if (StringUtils.isNotEmpty(klass))
            sb.append(prefix).append(RankType.CLASS.getRank()).append(":\"").append(klass).append("\"");
        if (StringUtils.isNotEmpty(order))
            sb.append(prefix).append(RankType.ORDER.getRank()).append(":\"").append(order).append("\"");
        if (StringUtils.isNotEmpty(family))
            sb.append(prefix).append(RankType.FAMILY.getRank()).append(":\"").append(family).append("\"");
        if (StringUtils.isNotEmpty(genus))
            sb.append(prefix).append(RankType.GENUS.getRank()).append(":\"").append(genus).append("\"");
        if (StringUtils.isNotEmpty(species))
            sb.append(prefix).append(RankType.SPECIES.getRank()).append(":\"").append(species).append("\"");
        //authorship is always optional due to inconsistencies in the name format etc...
        if (authorship != null)
            sb.append(" ").append(NameIndexField.AUTHOR.toString()).append(":\"").append(authorship).append("\"~");
        return sb.toString();
    }



    public static void main(String[] args) {
        LinnaeanRankClassification a = new LinnaeanRankClassification(null, null, null, null, null, null, "AuS bus");
        LinnaeanRankClassification b = new LinnaeanRankClassification(null, null, null, null, null, null, new String("Aus bus"));
        System.out.println("a=b: " + a.equals(b));
        System.out.println("a.hc=b.hc: " + (a.hashCode() == b.hashCode()));
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return new HashCodeBuilder(1497136033, 448920019).append(this.scientificName).append(
                this.phylum).append(this.kingdom).append(this.family).append(
                this.order).append(this.klass).append(this.genus).append(this.species).
                append(this.specificEpithet).append(this.subspecies).append(this.infraspecificEpithet).toHashCode();
    }


}
