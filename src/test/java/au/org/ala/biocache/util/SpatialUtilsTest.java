package au.org.ala.biocache.util;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import org.junit.Test;
import org.junit.runner.RunWith;
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
        final double[] factors = new double[] {2, 1.5, 1.2};
        final double[] initialPrecisions = new double[] {0.0001, 0.001, 0.01};
        final double[] maxPrecisions = new double[] {0.1, 1.0, 10.0};
        final int minimumPoints = 100;

        for (int nextMaxPrecision = 0; nextMaxPrecision < maxPrecisions.length; nextMaxPrecision++) {
            for (int nextInitialPrecision = 0; nextInitialPrecision < initialPrecisions.length; nextInitialPrecision++) {
                for (int nextFactor = 0; nextFactor < factors.length; nextFactor++) {
                    for (int nextMaxPoints = 0; nextMaxPoints < maxPoints.length; nextMaxPoints++) {
                        String simplifiedWkt = SpatialUtils.simplifyWkt(wkt, maxPoints[nextMaxPoints], factors[nextFactor], initialPrecisions[nextInitialPrecision], maxPrecisions[nextMaxPrecision]);
                        WKTReader r = new WKTReader();
                        Geometry resultGeom = r.read(simplifiedWkt);
                        int numPoints = resultGeom.getNumPoints();
                        assertTrue("numPoints=" + numPoints + " was greater than " + maxPoints[nextMaxPoints] + " factor=" + factors[nextFactor] + " initialPrecision=" + initialPrecisions[nextInitialPrecision] + " maxPrecision=" + maxPrecisions[nextMaxPrecision], numPoints <  maxPoints[nextMaxPoints]);
                        assertTrue("numPoints=" + numPoints + " was less than " + minimumPoints + " factor=" + factors[nextFactor] + " initialPrecision=" + initialPrecisions[nextInitialPrecision] + " maxPrecision=" + maxPrecisions[nextMaxPrecision], numPoints > minimumPoints);
                    }
                }
            }
        }
    }

    private String readTextWkt() throws Exception {
        InputStream wktIn = getClass().getResourceAsStream("simplify-wkt-input.wkt");
        InputStreamReader reader = new InputStreamReader(wktIn, "UTF-8");
        return FileCopyUtils.copyToString(reader);
    }
}
