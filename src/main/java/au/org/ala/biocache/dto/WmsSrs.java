package au.org.ala.biocache.dto;


import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.DefaultCoordinateOperationFactory;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.CoordinateOperation;

public class WmsSrs {
    String srs = "EPSG:3857";

    private CoordinateOperation transformTo4326;
    private CoordinateOperation transformFrom4326;

    public String getSrs() {
        return srs;
    }

    public void setSrs(String srs) throws Exception {
        if ("EPSG:900913".equalsIgnoreCase(srs)) {
            this.srs = "EPSG:3857";
        } else {
            this.srs = srs;
        }

        CRSAuthorityFactory factory = CRS.getAuthorityFactory(true);
        CoordinateReferenceSystem sourceCRS = factory.createCoordinateReferenceSystem(this.srs);
        CoordinateReferenceSystem targetCRS = factory.createCoordinateReferenceSystem("EPSG:4326");
        transformTo4326 = new DefaultCoordinateOperationFactory().createOperation(sourceCRS, targetCRS);
        transformFrom4326 = new DefaultCoordinateOperationFactory().createOperation(targetCRS, sourceCRS);
    }

    public CoordinateOperation getTransformTo4326() {
        return transformTo4326;
    }

    public CoordinateOperation getTransformFrom4326() {
        return transformFrom4326;
    }
}
