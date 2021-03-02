package au.org.ala.biocache.util;

import org.apache.commons.math3.util.Precision;
import org.geotools.geometry.GeneralDirectPosition;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.referencing.operation.DefaultCoordinateOperationFactory;
import org.opengis.geometry.DirectPosition;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.CoordinateOperation;

/**
 * GIS Utilities.
 * <p>
 * Merged from biocache-store.
 */
public class GISUtil {

    public static String WGS84_EPSG_Code = "EPSG:4326";

    /**
     * Re-projects coordinates into WGS 84
     *
     * @param coordinate1            first coordinate. If source value is easting/northing, then this should be the easting value.
     *                               Otherwise it should be the latitude
     * @param coordinate2            first coordinate. If source value is easting/northing, then this should be the northing value.
     *                               Otherwise it should be the longitude
     * @param sourceCrsEpsgCode      epsg code for the source CRS, e.g. EPSG:4202 for AGD66
     * @param decimalPlacesToRoundTo number of decimal places to round the reprojected coordinates to
     * @return Reprojected coordinates (latitude, longitude), or None if the operation failed.
     */
    static double[] reprojectCoordinatesToWGS84(Double coordinate1, Double coordinate2, String sourceCrsEpsgCode,
                                                Integer decimalPlacesToRoundTo) {
        try {
            DefaultGeographicCRS wgs84CRS = DefaultGeographicCRS.WGS84;
            CoordinateReferenceSystem sourceCRS = CRS.decode(sourceCrsEpsgCode);
            CoordinateOperation transformOp = new DefaultCoordinateOperationFactory().createOperation(sourceCRS, wgs84CRS);
            GeneralDirectPosition directPosition = new GeneralDirectPosition(coordinate1, coordinate2);
            DirectPosition wgs84LatLong = transformOp.getMathTransform().transform(directPosition, null);

            //NOTE - returned coordinates are longitude, latitude, despite the fact that if
            //converting latitude and longitude values, they must be supplied as latitude, longitude.
            //No idea why this is the case.
            Double longitude = wgs84LatLong.getOrdinate(0);
            Double latitude = wgs84LatLong.getOrdinate(1);

            Double roundedLongitude = Precision.round(longitude, decimalPlacesToRoundTo);
            Double roundedLatitude = Precision.round(latitude, decimalPlacesToRoundTo);

            return new double[]{roundedLatitude, roundedLongitude};
        } catch (Exception e) {
            return null;
        }
    }


}
