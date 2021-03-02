package au.org.ala.biocache.util;

import au.org.ala.biocache.dao.SearchDAOImpl;
import au.org.ala.biocache.dto.QualityAssertion;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.geotools.referencing.CRS;
import org.springframework.util.StreamUtils;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static au.org.ala.biocache.dto.AssertionCodes.DECIMAL_LAT_LONG_CALCULATED_FROM_EASTING_NORTHING;
import static au.org.ala.biocache.dto.AssertionCodes.DECIMAL_LAT_LONG_CALCULATION_FROM_EASTING_NORTHING_FAILED;
import static au.org.ala.biocache.dto.AssertionStatus.PASSED;

/**
 * Utilities for parsing UK Ordnance survey British and Irish grid references.
 * <p>
 * Merged from biocache-store.
 */
public class GridUtil {

    /**
     * log4j logger
     */
    private static final Logger logger = Logger.getLogger(SearchDAOImpl.class);

    static Cache<String, GISPoint> lru =
            CacheBuilder.newBuilder()
                    .maximumSize(100000)
                    .build(
                            new CacheLoader<String, GISPoint>() {
                                @Override
                                public GISPoint load(String s) throws Exception {
                                    return null;
                                }
                            });

    // deal with the 2k OS grid ref separately
    Pattern osGridRefNoEastingNorthing = Pattern.compile("([A-Z]{2})");
    Pattern osGridRefRegex1Number = Pattern.compile("([A-Z]{2})\\s*([0-9]+)$");
    Pattern osGridRef2kRegex = Pattern.compile("([A-Z]{2})\\s*([0-9]+)\\s*([0-9]+)\\s*([A-Z]{1})");
    Pattern osGridRefRegex = Pattern.compile("([A-Z]{2})\\s*([0-9]+)\\s*([0-9]+)$");
    Pattern osGridRefWithQuadRegex =
            Pattern.compile("([A-Z]{2})\\s*([0-9]+)\\s*([0-9]+)\\s*([NW|NE|SW|SE]{2})$");

    // deal with the 2k OS grid ref separately
    static char[] irishGridletterscodes =
            new char[]{
                    'A', 'B', 'C', 'D', 'F', 'G', 'H', 'J', 'L', 'M', 'N', 'O', 'Q', 'R', 'S', 'T', 'V', 'W',
                    'X', 'Y'
            };
    static String irishGridlettersFlattened = new String(irishGridletterscodes);
    static Pattern irishGridRefNoEastingNorthing =
            Pattern.compile("(I?[" + irishGridlettersFlattened + "]{1})");
    static Pattern irishGridRefRegex1Number = Pattern.compile("(I?[A-Z]{1})\\s*([0-9]+)$");
    static Pattern irishGridRef2kRegex =
            Pattern.compile("(I?[A-Z]{1})\\s*([0-9]+)\\s*([0-9]+)\\s*([A-Z]{1})");
    static Pattern irishGridRefRegex = Pattern.compile("(I?[A-Z]{1})\\s*([0-9]+)\\s*([0-9]+)$");
    static Pattern irishGridRefWithQuadRegex =
            Pattern.compile("(I?[A-Z]{1})\\s*([0-9]+)\\s*([0-9]+)\\s*([NW|NE|SW|SE]{2})$");
    static char[] tetradLetters =
            new char[]{
                    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S',
                    'T', 'U', 'V', 'W', 'X', 'Y', 'Z'
            };

    // CRS
    public static String IRISH_CRS = "EPSG:29902";
    public static String OSGB_CRS = "EPSG:27700";

    private static Map<String, String> crsEpsgCodesMap = null;

    public static Map<String, String> getCrsEpsgCodesMap() {
        if (crsEpsgCodesMap == null) {
            try {
                Map<String, String> map = new ConcurrentHashMap();
                for (String line :
                        StreamUtils.copyToString(
                                GridUtil.class.getResourceAsStream("/crsEpsgCodes.txt"), StandardCharsets.UTF_8)
                                .split("\n")) {
                    String[] values = line.split("=");
                    map.put(values[0], values[1]);
                }
                crsEpsgCodesMap = map;
            } catch (Exception e) {
                logger.error("failed to read resource /crsEpsgCodes.txt", e);
            }
        }
        return crsEpsgCodesMap;
    }

    private static Map<String, String> zoneEpsgCodesMap = null;

    public static Map<String, String> getZoneEpsgCodesMap() {
        if (zoneEpsgCodesMap == null) {
            try {
                Map<String, String> map = new ConcurrentHashMap();
                for (String line :
                        StreamUtils.copyToString(
                                GridUtil.class.getResourceAsStream("/zoneEpsgCodes.txt"),
                                StandardCharsets.UTF_8)
                                .split("\n")) {
                    String[] values = line.split("=");
                    map.put(values[0], values[1]);
                }
                zoneEpsgCodesMap = map;
            } catch (Exception e) {
                logger.error("failed to read resource /zoneEpsgCodes.txt", e);
            }
        }
        return zoneEpsgCodesMap;
    }

    /**
     * Derive a value from the grid reference accuracy for coordinateUncertaintyInMeters.
     *
     * @param noOfNumericalDigits
     * @param noOfSecondaryAlphaChars
     * @return
     */
    static Integer getCoordinateUncertaintyFromGridRef(
            Integer noOfNumericalDigits, Integer noOfSecondaryAlphaChars) {
        Integer accuracy = null;
        switch (noOfNumericalDigits) {
            case 10:
                accuracy = 1;
                break;
            case 8:
                accuracy = 10;
                break;
            case 6:
                accuracy = 100;
                break;
            case 4:
                accuracy = 1000;
                break;
            case 2:
                accuracy = 10000;
                break;
            case 0:
                accuracy = 100000;
                break;
            default:
                accuracy = null;
                break;
        }

        if (noOfSecondaryAlphaChars == 2) {
            return accuracy / 2;
        } else if (noOfSecondaryAlphaChars == 1) {
            return accuracy / 5;
        } else {
            return accuracy;
        }
    }

    /**
     * Reduce the resolution of the supplied grid reference.
     *
     * @param gridReference
     * @param uncertaintyString
     * @return
     */
    static String convertReferenceToResolution(String gridReference, String uncertaintyString) {

        String ref = null;

        try {
            Map<String, String> gridRefs = getGridRefAsResolutions(gridReference);
            Integer uncertainty = Integer.parseInt(uncertaintyString);

            String[] gridRefSeq =
                    new String[]{
                            gridRefs.getOrDefault("grid_ref_100000", ""),
                            gridRefs.getOrDefault("grid_ref_10000", ""),
                            gridRefs.getOrDefault("grid_ref_2000", ""),
                            gridRefs.getOrDefault("grid_ref_1000", ""),
                            gridRefs.getOrDefault("grid_ref_100", "")
                    };

            if (uncertainty > 10000) {
                ref = getBestValue(gridRefSeq, 0);
            } else if (uncertainty <= 10000 && uncertainty > 2000) {
                ref = getBestValue(gridRefSeq, 1);
            } else if (uncertainty <= 2000 && uncertainty > 1000) {
                ref = getBestValue(gridRefSeq, 2);
            } else if (uncertainty <= 1000 && uncertainty > 100) {
                ref = getBestValue(gridRefSeq, 3);
            } else if (uncertainty < 100) {
                ref = getBestValue(gridRefSeq, 4);
            }
        } catch (Exception e) {
            logger.error(
                    "Problem converting grid reference "
                            + gridReference
                            + " to lower resolution of "
                            + uncertaintyString,
                    e);
        }

        return ref;
    }

    static String getBestValue(String[] values, Integer preferredIndex) {

        Integer counter = preferredIndex;
        while (counter >= 0) {
            if (StringUtils.isNotEmpty(values[counter])) {
                return values[counter];
            }
            counter = counter - 1;
        }
        return "";
    }

    private static String padWithZeros(String ref, Integer pad) {
        while (ref.length() < pad) {
            ref = "0" + ref;
        }
        return ref;
    }

    /**
     * Takes a grid reference and returns a map of grid references at different resolutions. Map will
     * look like: grid_ref_100000 -> "NO" grid_ref_10000 -> "NO11" grid_ref_1000 -> "NO1212"
     * grid_ref_100 -> "NO123123"
     *
     * @param gridRef
     * @return
     */
    public static Map<String, String> getGridRefAsResolutions(String gridRef) {

        Map<String, String> map = new HashMap<>();

        GridRef gr = gridReferenceToEastingNorthing(gridRef);

        int gridSize = gr.getCoordinateUncertainty();
        map.put("grid_ref_100000", gr.gridLetters);

        if (gridRef.length() > 2) {

            String eastingAsStr = padWithZeros(String.valueOf(gr.getEasting() % 100000), 5);
            String northingAsStr = padWithZeros(String.valueOf(gr.getNorthing() % 100000), 5);

            // add grid references for 10km, and 1km
            if (eastingAsStr.length() >= 2 && northingAsStr.length() >= 2) {
                map.put(
                        "grid_ref_10000",
                        gr.gridLetters + eastingAsStr.substring(0, 1) + northingAsStr.substring(0, 1));
            }
            if (eastingAsStr.length() >= 3 && northingAsStr.length() >= 3) {
                int eastingWithin10km = Integer.parseInt(eastingAsStr.substring(1, 2));
                int northingWithin10km = Integer.parseInt(northingAsStr.substring(1, 2));
                int tetrad = tetradLetters[(eastingWithin10km / 2) * 5 + (northingWithin10km / 2)];

                if (gridSize != -1 && gridSize <= 2000) {
                    map.put(
                            "grid_ref_2000",
                            gr.gridLetters
                                    + eastingAsStr.substring(0, 1)
                                    + northingAsStr.substring(0, 1)
                                    + tetrad);
                }
                if (gridSize != -1 && gridSize <= 1000) {
                    map.put(
                            "grid_ref_1000",
                            gr.gridLetters + eastingAsStr.substring(0, 2) + northingAsStr.substring(0, 2));
                }
            }

            if (gridSize != -1 && gridSize <= 100 && eastingAsStr.length() > 3) {
                map.put(
                        "grid_ref_100",
                        gr.gridLetters + eastingAsStr.substring(0, 3) + northingAsStr.substring(0, 3));
            }
        }

        return map;
    }

    /**
     * Takes a grid reference (british or irish) and returns easting, northing, datum and precision.
     */
    public static GridRef gridReferenceToEastingNorthing(String gridRef) {
        GridRef result = osGridReferenceToEastingNorthing(gridRef);
        if (result != null) {
            return result;
        } else {
            return irishGridReferenceToEastingNorthing(gridRef);
        }
    }

    /**
     * Convert an ordnance survey grid reference to northing, easting and
     * coordinateUncertaintyInMeters. This is a port of this javascript code:
     *
     * <p>http://www.movable-type.co.uk/scripts/latlong-gridref.html
     *
     * <p>with additional extensions to handle 2km grid references e.g. NM39A
     *
     * @param gridRef
     * @return easting, northing, coordinate uncertainty in meters, minEasting, minNorthing,
     * maxEasting, maxNorthing, coordinate system
     */
    static GridRef irishGridReferenceToEastingNorthing(String gridRef) {

        // validate & parse format
        String gridletters;
        String easting;
        String northing;
        String twoKRef = "";
        String quadRef = "";
        Integer coordinateUncertainty;

        Matcher matcher1 = irishGridRefRegex1Number.matcher(gridRef);
        Matcher matcher2 = irishGridRefRegex.matcher(gridRef);
        Matcher matcher3 = irishGridRef2kRegex.matcher(gridRef);
        Matcher matcher4 = irishGridRefWithQuadRegex.matcher(gridRef);
        Matcher matcher5 = irishGridRefNoEastingNorthing.matcher(gridRef);

        if (matcher1.matches()) {
            gridletters = matcher1.group(1);
            String gridDigits = matcher1.group(2);
            easting = gridDigits.substring(0, gridDigits.length() / 2);
            northing = gridDigits.substring(gridDigits.length() / 2);
            coordinateUncertainty = getCoordinateUncertaintyFromGridRef(gridDigits.length(), 0);
        } else if (matcher2.matches()) {
            gridletters = matcher2.group(1);
            easting = matcher2.group(2);
            northing = matcher2.group(3);
            twoKRef = matcher2.group(4);
            coordinateUncertainty = getCoordinateUncertaintyFromGridRef(easting.length() * 2, 0);
        } else if (matcher3.matches()) {
            gridletters = matcher3.group(1);
            easting = matcher3.group(2);
            northing = matcher3.group(3);
            twoKRef = matcher3.group(4);
            coordinateUncertainty = getCoordinateUncertaintyFromGridRef(easting.length() * 2, 1);
        } else if (matcher4.matches()) {
            gridletters = matcher4.group(1);
            easting = matcher4.group(2);
            northing = matcher4.group(3);
            quadRef = matcher4.group(4);
            coordinateUncertainty = getCoordinateUncertaintyFromGridRef(easting.length() * 2, 2);
        } else if (matcher5.matches()) {
            gridletters = matcher5.group(1);
            easting = "0";
            northing = "0";
            coordinateUncertainty = getCoordinateUncertaintyFromGridRef(0, 0);
        } else {
            return null;
        }

        char singleGridLetter = gridletters.charAt(0);
        if (gridletters.length() == 2) singleGridLetter = gridletters.charAt(1);

        int gridIdx = String.valueOf(irishGridletterscodes).indexOf(singleGridLetter);

        // convert grid letters into 100km-square indexes from false origin (grid square SV):
        int e100km = (gridIdx % 4);
        int n100km = (4 - (gridIdx / 4));

        String easting10digit = (easting + "00000").substring(0, 5);
        String northing10digit = (northing + "00000").substring(0, 5);

        int e = Integer.parseInt(e100km + easting10digit);
        int n = Integer.parseInt(n100km + northing10digit);

        /** C & P from below * */

        // handle the non standard grid parts
        if (StringUtils.isNotEmpty(twoKRef)) {

            int cellSize = 0;
            if (easting.length() == 1) cellSize = 2000;
            else if (easting.length() == 2) cellSize = 200;
            else if (easting.length() == 3) cellSize = 20;
            else if (easting.length() == 4) cellSize = 2;

            // Dealing with 5 character grid references = 2km grids
            // http://www.kmbrc.org.uk/recording/help/gridrefhelp.php?page=6
            if (Character.codePointAt(twoKRef, 0) <= 'N') {
                e = e + (((Character.codePointAt(twoKRef, 0) - 65) / 5) * cellSize);
                n = n + (((Character.codePointAt(twoKRef, 0) - 65) % 5) * cellSize);
            } else if (Character.codePointAt(twoKRef, 0) >= 'P') {
                e = e + (((Character.codePointAt(twoKRef, 0) - 66) / 5) * cellSize);
                n = n + (((Character.codePointAt(twoKRef, 0) - 66) % 5) * cellSize);
            } else {
                return null;
            }
        } else if (StringUtils.isNotEmpty(quadRef)) {

            int cellSize = 0;
            if (easting.length() == 1) cellSize = 5000;
            else if (easting.length() == 2) cellSize = 500;
            else if (easting.length() == 3) cellSize = 50;
            else if (easting.length() == 4) cellSize = 5;

            if (cellSize > 0) {
                if ("NW".equals(twoKRef)) {
                    e = e + (cellSize / 2);
                    n = n + (cellSize + cellSize / 2);
                } else if ("NE".equals(twoKRef)) {
                    e = e + (cellSize + cellSize / 2);
                    n = n + (cellSize + cellSize / 2);
                } else if ("SW".equals(twoKRef)) {
                    e = e + (cellSize / 2);
                    n = n + (cellSize / 2);
                } else if ("SE".equals(twoKRef)) {
                    e = e + (cellSize + cellSize / 2);
                    n = n + (cellSize / 2);
                } else {
                    return null;
                }
            }
        }

        /** end of C & P ** */
        int coordinateUncertaintyOrZero = coordinateUncertainty == null ? 0 : coordinateUncertainty;

        return new GridRef(
                gridletters,
                e,
                n,
                coordinateUncertaintyOrZero,
                e,
                n,
                e + coordinateUncertaintyOrZero,
                n + coordinateUncertaintyOrZero,
                IRISH_CRS);
    }

    /**
     * Convert an ordnance survey grid reference to northing, easting and
     * coordinateUncertaintyInMeters. This is a port of this javascript code:
     *
     * <p>http://www.movable-type.co.uk/scripts/latlong-gridref.html
     *
     * <p>with additional extensions to handle 2km grid references e.g. NM39A
     *
     * @param gridRef
     * @return easting, northing, coordinate uncertainty in meters, minEasting, minNorthing,
     * maxEasting, maxNorthing
     */
    static GridRef osGridReferenceToEastingNorthing(String gridRef) {

        // deal with the 2k OS grid ref separately
        Pattern osGridRefNoEastingNorthing = Pattern.compile("([A-Z]{2})");
        Pattern osGridRefRegex1Number = Pattern.compile("([A-Z]{2})\\s*([0-9]+)$");
        Pattern osGridRef2kRegex = Pattern.compile("([A-Z]{2})\\s*([0-9]+)\\s*([0-9]+)\\s*([A-Z]{1})");
        Pattern osGridRefRegex = Pattern.compile("([A-Z]{2})\\s*([0-9]+)\\s*([0-9]+)$");
        Pattern osGridRefWithQuadRegex =
                Pattern.compile("([A-Z]{2})\\s*([0-9]+)\\s*([0-9]+)\\s*([NW|NE|SW|SE]{2})$");

        // validate & parse format
        String gridletters;
        String easting;
        String northing;
        String twoKRef = null;
        String quadRef = null;
        Integer coordinateUncertainty;

        Matcher matcher1 = osGridRefRegex1Number.matcher(gridRef);
        Matcher matcher2 = osGridRefRegex.matcher(gridRef);
        Matcher matcher3 = osGridRef2kRegex.matcher(gridRef);
        Matcher matcher4 = osGridRefWithQuadRegex.matcher(gridRef);
        Matcher matcher5 = osGridRefNoEastingNorthing.matcher(gridRef);
        if (matcher1.matches()) {
            String gridDigits = matcher1.group(2);

            coordinateUncertainty = getCoordinateUncertaintyFromGridRef(gridDigits.length(), 0);
            gridletters = matcher1.group(1);
            easting = gridDigits.substring(0, gridDigits.length() / 2);
            northing = gridDigits.substring(gridDigits.length() / 2);
        } else if (matcher2.matches()) {
            gridletters = matcher2.group(1);
            easting = matcher2.group(2);
            northing = matcher2.group(3);
            coordinateUncertainty = getCoordinateUncertaintyFromGridRef(easting.length() * 2, 0);
        } else if (matcher3.matches()) {
            gridletters = matcher3.group(1);
            easting = matcher3.group(2);
            northing = matcher3.group(3);
            twoKRef = matcher3.group(4);
            coordinateUncertainty = getCoordinateUncertaintyFromGridRef(easting.length() * 2, 1);
        } else if (matcher4.matches()) {
            gridletters = matcher4.group(1);
            easting = matcher4.group(2);
            northing = matcher4.group(3);
            quadRef = matcher4.group(4);
            coordinateUncertainty = getCoordinateUncertaintyFromGridRef(easting.length() * 2, 2);
        } else if (matcher5.matches()) {
            gridletters = matcher5.group(1);
            easting = "0";
            northing = "0";
            coordinateUncertainty = getCoordinateUncertaintyFromGridRef(0, 0);
        } else {
            return null;
        }

        // get numeric values of letter references, mapping A->0, B->1, C->2, etc:
        int l1;
        int value1 = Character.codePointAt(gridletters, 0) - Character.codePointAt("A", 0);
        if (value1 > 7) {
            l1 = value1 - 1;
        } else {
            l1 = value1;
        }

        int l2;
        int value2 = Character.codePointAt(gridletters, 1) - Character.codePointAt("A", 0);
        if (value2 > 7) {
            l2 = value2 - 1;
        } else {
            l2 = value2;
        }

        // convert grid letters into 100km-square indexes from false origin (grid square SV):
        int e100km = (int) (((l1 - 2) % 5) * 5 + (l2 % 5));
        int n100km = (int) ((19 - Math.floor(l1 / 5) * 5) - Math.floor(l2 / 5));

        // validation
        if (e100km < 0 || e100km > 6 || n100km < 0 || n100km > 12) {
            return null;
        }
        if (easting == null || northing == null) {
            return null;
        }
        if (easting.length() != northing.length()) {
            return null;
        }

        // standardise to 10-digit refs (metres)
        String easting10digit = (easting + "00000").substring(0, 5);
        String northing10digit = (northing + "00000").substring(0, 5);

        int e = Integer.parseInt(e100km + easting10digit);
        int n = Integer.parseInt(n100km + northing10digit);

        // handle the non standard grid parts
        if (StringUtils.isNotEmpty(twoKRef)) {

            int cellSize = 0;
            if (easting.length() == 1) cellSize = 2000;
            else if (easting.length() == 2) cellSize = 200;
            else if (easting.length() == 3) cellSize = 20;
            else if (easting.length() == 4) cellSize = 2;

            // Dealing with 5 character grid references = 2km grids
            // http://www.kmbrc.org.uk/recording/help/gridrefhelp.php?page=6
            if (Character.codePointAt(twoKRef, 0) <= 'N') {
                e = e + (((Character.codePointAt(twoKRef, 0) - 65) / 5) * cellSize);
                n = n + (((Character.codePointAt(twoKRef, 0) - 65) % 5) * cellSize);
            } else if (Character.codePointAt(twoKRef, 0) >= 'P') {
                e = e + (((Character.codePointAt(twoKRef, 0) - 66) / 5) * cellSize);
                n = n + (((Character.codePointAt(twoKRef, 0) - 66) % 5) * cellSize);
            } else {
                return null;
            }

        } else if (StringUtils.isNotEmpty(quadRef)) {

            int cellSize = 0;
            if (easting.length() == 1) cellSize = 5000;
            else if (easting.length() == 2) cellSize = 500;
            else if (easting.length() == 3) cellSize = 50;
            else if (easting.length() == 4) cellSize = 5;

            if (cellSize > 0) {
                if ("NW".equals(quadRef)) {

                    e = e + (cellSize / 2);
                    n = n + (cellSize + cellSize / 2);
                } else if ("NE".equals(quadRef)) {
                    e = e + (cellSize + cellSize / 2);
                    n = n + (cellSize + cellSize / 2);
                } else if ("SW".equals(quadRef)) {
                    e = e + (cellSize / 2);
                    n = n + (cellSize / 2);
                } else if ("SE".equals(quadRef)) {
                    e = e + (cellSize + cellSize / 2);
                    n = n + (cellSize / 2);
                } else {
                    return null;
                }
            }
        }

        int coordinateUncertaintyOrZero = coordinateUncertainty == null ? 0 : coordinateUncertainty;

        return new GridRef(
                gridletters,
                e,
                n,
                coordinateUncertainty,
                e,
                n,
                e + coordinateUncertaintyOrZero,
                n + coordinateUncertaintyOrZero,
                OSGB_CRS);
    }

    /**
     * Process supplied grid references. This currently only recognises UK OS grid references but
     * could be extended to support other systems.
     *
     * @param gridReference
     */
    public static GISPoint processGridReference(String gridReference) {

        GISPoint cachedObject = lru.getIfPresent(gridReference);
        if (cachedObject != null) return cachedObject;

        GISPoint result = null;

        GridRef gr = GridUtil.gridReferenceToEastingNorthing(gridReference);

        // move coordinates to the centroid of the grid
        double reposition = 0;
        if (gr.getCoordinateUncertainty() == null && gr.getCoordinateUncertainty() > 0) {
            reposition = gr.getCoordinateUncertainty() / 2;
        }

        double[] coords =
                GISUtil.reprojectCoordinatesToWGS84(
                        gr.getEasting() + reposition, gr.getNorthing() + reposition, gr.getDatum(), 5);

        // reproject min/max lat/lng
        double[][] bbox =
                new double[][]{
                        GISUtil.reprojectCoordinatesToWGS84(
                                gr.getMinEasting().doubleValue(), gr.getMinNorthing().doubleValue(), gr.datum, 5),
                        GISUtil.reprojectCoordinatesToWGS84(
                                gr.getMaxEasting().doubleValue(), gr.getMaxNorthing().doubleValue(), gr.datum, 5)
                };

        if (coords != null) {
            String uncertaintyToUse = null;
            if (gr.getCoordinateUncertainty() != null) {
                gr.getCoordinateUncertainty().toString();
            }
            result =
                    new GISPoint(
                            coords[0],
                            coords[1],
                            GISUtil.WGS84_EPSG_Code,
                            uncertaintyToUse,
                            gr.getEasting().toString(),
                            gr.getNorthing().toString(),
                            bbox[0][0],
                            bbox[0][1],
                            bbox[1][0],
                            bbox[1][1]);
        } else {
            result = null;
        }

        lru.put(gridReference, result);

        return result;
    }

    /**
     * Get the EPSG code associated with a coordinate reference system string e.g. "WGS84" or "AGD66".
     *
     * @param crs The coordinate reference system string.
     * @return The EPSG code associated with the CRS, or None if no matching code could be found. If
     * the supplied string is already a valid EPSG code, it will simply be returned.
     */
    String lookupEpsgCode(String crs) {
        if (StringUtils.startsWithIgnoreCase(crs, "EPSG:")) {
            // Do a lookup with the EPSG code to ensure that it is valid
            try {
                CRS.decode(crs.toUpperCase());
                // lookup was successful so just return the EPSG code
                return crs.toUpperCase();
            } catch (Exception e) {
            }
        }
        return getCrsEpsgCodesMap().getOrDefault(crs.toUpperCase(), null);
    }

    /**
     * Converts a easting northing to a decimal latitude/longitude.
     *
     * @param verbatimSRS
     * @param easting
     * @param northing
     * @param zone
     * @param assertions
     * @return 3-tuple reprojectedLatitude, reprojectedLongitude, WGS84_EPSG_Code
     */
    GISPoint processNorthingEastingZone(
            String verbatimSRS,
            String easting,
            String northing,
            String zone,
            List<QualityAssertion> assertions) {

        // Need a datum and a zone to get an epsg code for transforming easting/northing values
        String epsgCodeKey;
        if (verbatimSRS != null) {
            epsgCodeKey = verbatimSRS.toUpperCase() + "|" + zone;
        } else {
            // Assume GDA94 / MGA zone
            epsgCodeKey = "GDA94|" + zone;
        }

        if (getZoneEpsgCodesMap().containsKey(epsgCodeKey)) {
            String crsEpsgCode = getZoneEpsgCodesMap().get(epsgCodeKey);
            Double eastingAsDouble = null;

            Double northingAsDouble = null;

            try {
                eastingAsDouble = Double.parseDouble(easting);
                northingAsDouble = Double.parseDouble(northing);
            } catch (Exception e) {
            }

            if (eastingAsDouble != null && northingAsDouble != null) {
                // Always round to 5 decimal places as easting/northing values are in metres and 0.00001
                // degree is approximately equal to 1m.
                double[] reprojectedCoords =
                        GISUtil.reprojectCoordinatesToWGS84(eastingAsDouble, northingAsDouble, crsEpsgCode, 5);
                if (reprojectedCoords == null) {
                    assertions.add(
                            new QualityAssertion(
                                    DECIMAL_LAT_LONG_CALCULATION_FROM_EASTING_NORTHING_FAILED,
                                    "Transformation of verbatim easting and northing to WGS84 failed"));
                    return null;
                } else {
                    // lat and long from easting and northing did NOT fail:
                    assertions.add(
                            new QualityAssertion(
                                    DECIMAL_LAT_LONG_CALCULATION_FROM_EASTING_NORTHING_FAILED, PASSED));
                    assertions.add(
                            new QualityAssertion(
                                    DECIMAL_LAT_LONG_CALCULATED_FROM_EASTING_NORTHING,
                                    "Decimal latitude and longitude were calculated using easting, northing and zone."));
                    double reprojectedLatitude = reprojectedCoords[0];
                    double reprojectedLongitude = reprojectedCoords[1];

                    return new GISPoint(
                            reprojectedLatitude, reprojectedLongitude, GISUtil.WGS84_EPSG_Code, null);
                }
            } else {
                return null;
            }
        } else {
            if (verbatimSRS == null) {
                assertions.add(
                        new QualityAssertion(
                                DECIMAL_LAT_LONG_CALCULATION_FROM_EASTING_NORTHING_FAILED,
                                "Unrecognized zone GDA94 / MGA zone " + zone));
            } else {
                assertions.add(
                        new QualityAssertion(
                                DECIMAL_LAT_LONG_CALCULATION_FROM_EASTING_NORTHING_FAILED,
                                "Unrecognized zone " + verbatimSRS + " / zone " + zone));
            }
            return null;
        }
    }
}
