package au.org.ala.biocache.util;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.util.FileCopyUtils;

import java.io.InputStream;
import java.io.InputStreamReader;


@RunWith(MockitoJUnitRunner.class)
public class SpatialUtilsTest {

    @Test
    public void testSimplfyWkt() throws Exception {
        String wkt = readTextWkt();
        int[] maxPoints = new int[] {20000, 10000, 5000, 1000};

        for (int i=0; i<maxPoints.length; i++) {
            String simplifiedWkt = SpatialUtils.simplifyWkt(wkt, maxPoints[i]);
            WKTReader r = new WKTReader();
            Geometry resultGeom = r.read(simplifiedWkt);
            int numPoints = resultGeom.getNumPoints();
            assert(numPoints <  maxPoints[i]);
            assert(numPoints > 100);
        }
    }

    private String readTextWkt() throws Exception {
        InputStream wktIn = getClass().getResourceAsStream("simplify-wkt-input.wkt");
        InputStreamReader reader = new InputStreamReader(wktIn, "UTF-8");
        return FileCopyUtils.copyToString(reader);
    }
}
