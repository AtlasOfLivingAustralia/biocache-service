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
    Pattern osGridRef50kRegex =Pattern.compile("([A-Z]{2})\\s*([NW|NE|SW|SE]{2})");
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
    static Pattern irishGridRef50kRegex = Pattern.compile("([A-Z]{1})\\s*([NW|NE|SW|SE]{2})$");
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
    static Integer getGridSizeFromGridRef(
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
                            gridRefs.getOrDefault("grid_ref_50000", ""),
                            gridRefs.getOrDefault("grid_ref_10000", ""),
                            gridRefs.getOrDefault("grid_ref_2000", ""),
                            gridRefs.getOrDefault("grid_ref_1000", ""),
                            gridRefs.getOrDefault("grid_ref_100", "")
                    };

            if (uncertainty > 10000) {
                ref = getBestValue(gridRefSeq, 0);
            } else if (uncertainty <= 50000 && uncertainty > 10000) {
                getBestValue(gridRefSeq, 1);
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

        int gridSize = gr.getGridSize();
        map.put("grid_ref_100000", gr.gridLetters);

        if (gridRef.length() > 2) {

            String eastingAsStr = padWithZeros(String.valueOf(gr.getEasting() % 100000), 5);
            String northingAsStr = padWithZeros(String.valueOf(gr.getNorthing() % 100000), 5);

            //add grid reference for 50km
            if (eastingAsStr.length() >= 2 && northingAsStr.length() >= 2) {
                var quad = "";
                if (Integer.parseInt(eastingAsStr.substring(0, 1)) < 5) { //W
                    if (Integer.parseInt(northingAsStr.substring(0, 1)) < 5) { //S
                        quad = "SW";
                    } else { //N
                        quad = "NW";
                    }
                } else { //E
                    if (Integer.parseInt(northingAsStr.substring(0, 1)) < 5) { //S
                        quad = "SE";
                    } else { //N
                        quad = "NE";
                    }
                }
                map.put("grid_ref_50000", gr.getGridLetters() + quad);
            }

            if (gridSize < 50000) {
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
        Integer gridSize;

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
            gridSize = getGridSizeFromGridRef(gridDigits.length(), 0);
        } else if (matcher2.matches()) {
            gridletters = matcher2.group(1);
            easting = matcher2.group(2);
            northing = matcher2.group(3);
            twoKRef = matcher2.group(4);
            gridSize = getGridSizeFromGridRef(easting.length() * 2, 0);
        } else if (matcher3.matches()) {
            gridletters = matcher3.group(1);
            easting = matcher3.group(2);
            northing = matcher3.group(3);
            twoKRef = matcher3.group(4);
            gridSize = getGridSizeFromGridRef(easting.length() * 2, 1);
        } else if (matcher4.matches()) {
            gridletters = matcher4.group(1);
            easting = matcher4.group(2);
            northing = matcher4.group(3);
            quadRef = matcher4.group(4);
            gridSize = getGridSizeFromGridRef(easting.length() * 2, 2);
        } else if (matcher5.matches()) {
            gridletters = matcher5.group(1);
            easting = "0";
            northing = "0";
            gridSize = getGridSizeFromGridRef(0, 0);
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

            if (gridSize == 50000) { //50km grids only
                cellSize = 50000;
            }

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
        int gridSizeOrZero = gridSize == null ? 0 : gridSize;

        return new GridRef(
                gridletters,
                e,
                n,
                gridSizeOrZero,
                e,
                n,
                e + gridSizeOrZero,
                n + gridSizeOrZero,
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
        Pattern osGridRef50kRegex = Pattern.compile("([A-Z]{2})\\s*([NW|NE|SW|SE]{2})$");
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
        Integer gridSize;

        Matcher matcher1 = osGridRefRegex1Number.matcher(gridRef);
        Matcher matcher2 = osGridRefRegex.matcher(gridRef);
        Matcher matcher3 = osGridRef2kRegex.matcher(gridRef);
        Matcher matcher4 = osGridRefWithQuadRegex.matcher(gridRef);
        Matcher matcher5 = osGridRefNoEastingNorthing.matcher(gridRef);
        if (matcher1.matches()) {
            String gridDigits = matcher1.group(2);

            gridSize = getGridSizeFromGridRef(gridDigits.length(), 0);
            gridletters = matcher1.group(1);
            easting = gridDigits.substring(0, gridDigits.length() / 2);
            northing = gridDigits.substring(gridDigits.length() / 2);
        } else if (matcher2.matches()) {
            gridletters = matcher2.group(1);
            easting = matcher2.group(2);
            northing = matcher2.group(3);
            gridSize = getGridSizeFromGridRef(easting.length() * 2, 0);
        } else if (matcher3.matches()) {
            gridletters = matcher3.group(1);
            easting = matcher3.group(2);
            northing = matcher3.group(3);
            twoKRef = matcher3.group(4);
            gridSize = getGridSizeFromGridRef(easting.length() * 2, 1);
        } else if (matcher4.matches()) {
            gridletters = matcher4.group(1);
            easting = matcher4.group(2);
            northing = matcher4.group(3);
            quadRef = matcher4.group(4);
            gridSize = getGridSizeFromGridRef(easting.length() * 2, 2);
        } else if (matcher5.matches()) {
            gridletters = matcher5.group(1);
            easting = "0";
            northing = "0";
            gridSize = getGridSizeFromGridRef(0, 0);
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

            if (gridSize == 50000) { //50km grids only
                cellSize = 50000;
            }

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

        int gridSizeOrZero = gridSize == null ? 0 : gridSize;

        return new GridRef(
                gridletters,
                e,
                n,
                gridSize,
                e,
                n,
                e + gridSizeOrZero,
                n + gridSizeOrZero,
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
        if (gr.getGridSize() == null && gr.getGridSize() > 0) {
            reposition = gr.getGridSize() / 2;
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
            if (gr.getGridSize() != null) {
                gr.getGridSize().toString();
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
     *///TODO_BIOCACHE_STORE was used by biocache store
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
    //Below is original code from the GridUtil class that was used by biocache store. TODO_BIOCACHE_STORE
//
//    //proportion of grid size a point is allowed to vary (in x or y dimensions) from the true centre and still be considered central (to deal with rounding errors)
//    public static double CENTROID_FRACTION = 0.1;
//
//
//    Integer getGridSizeInMeters(String gridRef) { //TODO_BIOCACHE_STORE was used by biocache store
//        GridRef grid = gridReferenceToEastingNorthing(gridRef);
//        if (grid == null) {
//            logger.info("Invalid grid reference: " + gridRef);
//            return null;
//        }
//        return grid.getGridSize();
//    }
//
//
//
//    boolean isCentroid(Double decimalLongitude, Double decimalLatitude, String gridRef) {
//        GridRef gr = gridReferenceToEastingNorthing(gridRef);
//        if (gr == null){
//            return false;
//        }
//
//        double reposition = (gr.getGridSize()!=null  && !gr.getGridSize().equals("")) ?
//                (gr.getGridSize()) / 2 : 0;
//
//
//        double[] coordsCentroid = GISUtil.reprojectCoordinatesToWGS84(gr.getEasting() + reposition, gr.getNorthing() + reposition, gr.getDatum(), 5);
//        double[] coordsCorner = GISUtil.reprojectCoordinatesToWGS84((double)gr.getEasting(), (double)gr.getNorthing(), gr.getDatum(), 5);
//        double gridCentroidLatitude = coordsCentroid[0];
//        double devLatitude = Math.abs(decimalLatitude - coordsCentroid[0]);
//        double devLongitude = Math.abs(decimalLongitude - coordsCentroid[1]);
//        double gridSizeLatitude = Math.abs(coordsCentroid[0] - coordsCorner[0]) * 2.0;
//        double gridSizeLongitude = Math.abs(coordsCentroid[1] - coordsCorner[1]) * 2.0;
//        if ((devLatitude > CENTROID_FRACTION * gridSizeLatitude) ||
//                (devLongitude > CENTROID_FRACTION * gridSizeLongitude)) {
//            return false;
//        } else {
//            return true;
//        }
//
//
//
//    }
//
//    public String getGridAsTextWithAnnotation(String gridReference) {
//        String text = "";
//        if (gridReference != null && !gridReference.isEmpty()) {
//            // Assuming GridUtil.osGridReferenceToEastingNorthing returns an Optional
//            GridRef result = GridUtil.osGridReferenceToEastingNorthing(gridReference);
//            if(result != null) {
//                text = "OSGB Grid Reference " + gridReference;
//            } else {
//                text = "OSI Grid Reference " + gridReference;
//            }
//        }
//        return text;
//    }
//
//    // get grid WKT from a grid reference
//    String getGridWKT(String gridReference) {
//        if (gridReference == null || gridReference.isEmpty()) {
//            gridReference = "";
//        }
//        var poly_grid = "";
//        if (gridReference != "") {
//            GridRef gr = GridUtil.gridReferenceToEastingNorthing(gridReference);
//            if (gr == null){
//                logger.info("Invalid grid reference: " + gridReference);
//                return null;
//            }
//
//
//            double[][] bbox = new double[2][2];
//            bbox[0] = GISUtil.reprojectCoordinatesToWGS84((double)gr.getMinEasting(), (double)gr.getMinNorthing(), gr.getDatum(), 5);
//            bbox[1] = GISUtil.reprojectCoordinatesToWGS84((double)gr.getMaxEasting(), (double)gr.getMaxNorthing(), gr.getDatum(), 5);
//            double minLatitude = bbox[0][0];
//            double minLongitude = bbox[0][1];
//            double maxLatitude = bbox[1][0];
//            double maxLongitude = bbox[1][1];
//
//            //for WKT, need to give points in lon-lat order, not lat-lon
//            poly_grid = "POLYGON((" + minLongitude + " " + minLatitude + "," +
//                    minLongitude + " " + maxLatitude + "," +
//                    maxLongitude + " " + maxLatitude + "," +
//                    maxLongitude + " " + minLatitude + "," +
//                    minLongitude + " " + minLatitude + "))";
//
//        }
//
//        return poly_grid;
//    }
//
//    /**
//     * Convert a WGS84 lat/lon coordinate to either OSGB (ordnance survey GB) or Irish OS grid reference using coordinateUncertaintyInMeters to define grid cell size
//     *
//     * Note: does not handle 2000m uncertainty
//     *
//     *  http://www.carabus.co.uk/ll_ngr.html
//     *
//     * @param lat latitude
//     * @param lon longitude
//     * @param coordinateUncertaintyInMeters
//     * @param geodeticDatum geodeticDatum (if empty assume WGS84)
//     * @return gridRef
//     */
//    String  latLonToOsGrid(double lat, double lon, double coordinateUncertaintyInMeters, String geodeticDatum, String gridType, int knownGridSize) {
//        String datum = lookupEpsgCode(geodeticDatum);
//
//        double N = 0.0;
//        double E = 0.0;
//
//        if (!(datum.equals("") || datum.equals("EPSG:27700") || datum.equals("EPSG:4326"))) {
//            return null;
//        } else {
//            if (datum.equals("EPSG:27700")) {
//                //in OSGB36
////                val northingsEastings = GridRefGISUtil.coordinatesOSGB36toNorthingEasting(lat, lon, 4)
////                val (northings, eastings) = northingsEastings.get
////                N = northings.toDouble
////                E = eastings.toDouble
//                double[] northingsEastings = GridRefGISUtil.coordinatesOSGB36toNorthingEasting(lat, lon, 4);
//                N = northingsEastings[0];
//                E = northingsEastings[1];
//            } else { //assume WGS84
//                double[] reprojectedNorthingsEastings;
//
//                switch (gridType) {
//                    case "OSGB":
//                        reprojectedNorthingsEastings = GridRefGISUtil.reprojectCoordinatesWGS84ToOSGB36(lat, lon, 4);
//                        break;
//                    case "Irish":
//                        reprojectedNorthingsEastings = GridRefGISUtil.reprojectCoordinatesWGS84ToOSNI(lat, lon, 4);
//                        break;
//                    default:
//                        reprojectedNorthingsEastings = GridRefGISUtil.reprojectCoordinatesWGS84ToOSGB36(lat, lon, 4);
//                }
//
//                N = reprojectedNorthingsEastings[0];
//                E = reprojectedNorthingsEastings[1];
//            }
//        }
//
//        int gridSize = (knownGridSize >= 0) ?
//                gridSizeRoundedUp(knownGridSize)
//                :
//                calculateGridSize(coordinateUncertaintyInMeters);
//
//
//
//        int digits = calculateNumOfGridRefDigits(gridSize);
//
//        if (gridSize == 2000) {
//            //FIXME: sort out getOSGridFromNorthingEasting to handle 2km, 50km grids properly
//            String onekmGrid = getOSGridFromNorthingEasting(Math.round(N), Math.round(E), 4, gridType);
//            //now convert 1km grid to containing 2km grid
//            if (onekmGrid != null) {
//                return convertReferenceToResolution(onekmGrid, "2000");
//            } else {
//                return null;
//            }
//        } else if (gridSize == 50000) {
//            //FIXME: sort out getOSGridFromNorthingEasting to handle 2km, 50km grids properly
//            String tenkmGrid = getOSGridFromNorthingEasting(Math.round(N), Math.round(E), 2, gridType);
//            //now convert 10km grid to containing 50km grid
//            if (tenkmGrid != null) {
//                return convertReferenceToResolution(tenkmGrid, "50000");
//            } else {
//                return null;
//            }
//        } else {
//            return getOSGridFromNorthingEasting(Math.round(N), Math.round(E), digits, gridType);
//        }
//
//    }
//
//    private static int calculateGridSize(double coordinateUncertaintyInMeters) {
//        // Implement based on Scala logic
//        return gridSizeRoundedUp(coordinateUncertaintyInMeters * Math.sqrt(2.0) - 0.001);
//    }
//
//    private static int calculateNumOfGridRefDigits(int gridSize) {
//        switch (gridSize) {
//            case 1:
//                return 10;
//            case 10:
//                return 8;
//            case 100:
//                return 6;
//            case 1000:
//                return 4;
//            case 2000:
//                return 3;
//            case 10000:
//                return 2;
//            default:
//                return 0;
//        }
//    }
//
//    private static int gridSizeRoundedUp(double gridSize) {
//        int[] limits = new int[]{1, 10, 100, 1000, 2000, 10000, 100000};
//        double gridSizeWhole = Math.ceil(gridSize);
//
//        for (int limit : limits) {
//            if (gridSizeWhole <= limit) {
//                return limit;
//            }
//        }
//
//        return 100000;
//    }
//
//    /**
//     *
//     * @param n : north
//     * @param e : east
//     * @param digits : digits
//     * @param gridType : gridType (Irish or OSGB)
//     * @return
//     */
//    public static String getOSGridFromNorthingEasting(double n, double e, int digits, String gridType) {
//        if ((digits % 2 != 0) || digits > 16) {
//            return null;
//        } else {
//            double e100k = Math.floor(e / 100000);
//            double n100k = Math.floor(n / 100000);
//
//            if (e100k < 0 || e100k > 6 || n100k < 0 || n100k > 12) {
//                return null;
//            }
//
//            // translate those into numeric equivalents of the grid letters// translate those into numeric equivalents of the grid letters
//            double l1 = (19 - n100k) - (19 - n100k) % 5 + Math.floor((e100k + 10) / 5);
//            double l2 = (19 - n100k) * 5 % 25 + e100k % 5;
//
//            if (l1 > 7) l1 += 1;
//            if (l2 > 7) l2 += 1;
//
//            char letter1 = (char) ('A' + l1);
//            char letter2 = (char) ('A' + l2);
//            String letterPair = "" + letter1 + letter2;
//            String letterPairLastOnly = "" + letter2;
//
//            double eMod = Math.floor((e % 100000) / Math.pow(10, 5 - digits / 2));
//            double nMod = Math.floor((n % 100000) / Math.pow(10, 5 - digits / 2));
//
//            String eModStr = padWithZeros(String.valueOf((int)eMod), digits / 2);
//            String nModStr = padWithZeros(String.valueOf((int)nMod), digits / 2);
//
//            switch (gridType) {
//                case "Irish":
//                    return letterPairLastOnly + eModStr + nModStr;
//                case "OSGB":
//                default:
//                    return letterPair + eModStr + nModStr;
//            }
//        }
//    }
//
//    private static String padWithZeros(String str, int length) {
//        while (str.length() < length) {
//            str = "0" + str;
//        }
//        return str;
//    }
//
//
//
//    /**
//     * Helper function to calculate coordinate uncertainty for a given grid size
//     *
//     * @param gridSize
//     * @return
//     */
//    String gridToCoordinateUncertaintyString(Integer gridSize ) {
//        return String.format("%.1f", gridSize / Math.sqrt(2.0));
//    }
}
