package au.org.ala.biocache.util;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.util.FileCopyUtils;

import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.io.InputStreamReader;

@RunWith(MockitoJUnitRunner.class)
public class SpatialUtilsTest {

    @Test
    public void testSimplfyWktDefaultParameters() throws Exception {
        final String wkt = readTextWkt();
        final int[] maxPoints = new int[] {20000, 10000, 5000, 1000};
        final int minimumPoints = 100;

        for (int i = 0; i < maxPoints.length; i++) {
            String simplifiedWkt = SpatialUtils.simplifyWkt(wkt, maxPoints[i]);
            WKTReader r = new WKTReader();
            Geometry resultGeom = r.read(simplifiedWkt);
            int numPoints = resultGeom.getNumPoints();
            assertTrue("numPoints=" + numPoints + " was greater than " + maxPoints[i], numPoints <  maxPoints[i]);
            assertTrue("numPoints=" + numPoints + " was less than " + minimumPoints, numPoints > minimumPoints);
        }
    }

    @Test
    public void testSimplfyWktCustomParameters() throws Exception {
        final String wkt = readTextWkt();
        final int[] maxPoints = new int[] {20000, 10000, 5000, 1000};
        final int minimumPoints = 20;

        for (int nextMaxPoints = 0; nextMaxPoints < maxPoints.length; nextMaxPoints++) {
            String simplifiedWkt = SpatialUtils.simplifyWkt(wkt, maxPoints[nextMaxPoints]);
            WKTReader r = new WKTReader();
            Geometry resultGeom = r.read(simplifiedWkt);
            int numPoints = resultGeom.getNumPoints();
            assertTrue("numPoints=" + numPoints + " was greater than " + maxPoints[nextMaxPoints], numPoints <  maxPoints[nextMaxPoints] * 2);
            assertTrue("numPoints=" + numPoints + " was less than " + minimumPoints, numPoints > minimumPoints);
        }
    }

    private String readTextWkt() throws Exception {
        InputStream wktIn = getClass().getResourceAsStream("simplify-wkt-input.wkt");
        InputStreamReader reader = new InputStreamReader(wktIn, "UTF-8");
        return FileCopyUtils.copyToString(reader);
    }
}
