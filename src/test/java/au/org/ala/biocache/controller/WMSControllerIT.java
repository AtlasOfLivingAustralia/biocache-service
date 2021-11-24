package au.org.ala.biocache.controller;

import au.org.ala.biocache.util.SolrUtils;
import au.org.ala.biocache.web.WMSController;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:springTest.xml"})
@WebAppConfiguration
public class WMSControllerIT {
    static {
        System.setProperty("biocache.config", System.getProperty("user.dir") + "/src/test/resources/biocache-test-config.properties");
    }

    @Autowired
    WMSController wmsController;

    @Autowired
    WebApplicationContext wac;
    MockMvc mockMvc;

    static File targetDirectory = new File("./target/test-wms");

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        SolrUtils.setupIndex();

        // empty ./target/test-wms/
        if (targetDirectory.exists()) FileUtils.forceDelete(targetDirectory);
        targetDirectory.mkdirs();
    }

    @Before
    public void setup() {
        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.wac).build();
    }

    @Test
    public void testWMS() throws Exception {
        String acceptType = "image/png";
        String url = "/mapping/wms/reflect";

        try {
            // smaller blue circles
            MvcResult mvcResult1 = this.mockMvc.perform(get(url).header("Accept", acceptType)
                    .param("BBOX", "0,-90,180,0")
                    .param("SRS", "EPSG:4326")
                    //.param("ENV", "olor%3A0000ff%3Bname%3Acircle%3Bsize%3A4%3Bopacity%3A0.8%3Bsel%3A*:*"))
                    .param("ENV", "color%3A0000ff%3Bname%3Acircle%3Bsize%3A4")
                    .param("OUTLINECOLOUR", "0xff0000")
                    .param("OUTLINE", "false"))
                    .andExpect(status().isOk()).andReturn();
            Map<Integer, Integer> colourCounts1 = imageColourCount(mvcResult1.getResponse().getContentAsByteArray());
            Files.write(new File(targetDirectory, "testWMS.1.png").toPath(), mvcResult1.getResponse().getContentAsByteArray());

            // larger green circles
            MvcResult mvcResult2 = this.mockMvc.perform(get(url).header("Accept", acceptType)
                    .param("BBOX", "0,-90,180,0")
                    .param("SRS", "EPSG:4326")
                    .param("ENV", "color%3A00ff00%3Bname%3Acircle%3Bsize%3A8")
                    .param("OUTLINECOLOUR", "0xff0000")
                    .param("OUTLINE", "false"))
                    .andExpect(status().isOk()).andReturn();
            Map<Integer, Integer> colourCounts2 = imageColourCount(mvcResult2.getResponse().getContentAsByteArray());
            Files.write(new File(targetDirectory, "testWMS.2.png").toPath(), mvcResult2.getResponse().getContentAsByteArray());

            // larger green circles with outline
            MvcResult mvcResult3 = this.mockMvc.perform(get(url).header("Accept", acceptType)
                    .param("BBOX", "0,-90,180,0")
                    .param("SRS", "EPSG:4326")
                    .param("ENV", "color%3A00ff00%3Bname%3Acircle%3Bsize%3A8")
                    .param("OUTLINE", "true")
                    .param("OUTLINECOLOUR", "0xff0000"))
                    .andExpect(status().isOk()).andReturn();
            Map<Integer, Integer> colourCounts3 = imageColourCount(mvcResult3.getResponse().getContentAsByteArray());
            Files.write(new File(targetDirectory, "testWMS.3.png").toPath(), mvcResult3.getResponse().getContentAsByteArray());


            // first request has blue dots and no green dots
            assert (colourCounts1.getOrDefault(0xff0000ff, 0) > 100);
            assert (colourCounts1.getOrDefault(0xff00ff00, 0) == 0);
            //first request has transparent background
            assert (colourCounts1.getOrDefault(0x00000000, 0) > 100);
            // first request no red boarders
            assert (colourCounts1.getOrDefault(0xff000000, 0) == 0);
            assert (borderCount(0x00ff0000, 0x000000ff, colourCounts1) == 0);

            // second request has green dots and no blue dots
            assert (colourCounts2.getOrDefault(0xff00ff00, 0) > 100);
            assert (colourCounts2.getOrDefault(0xff0000ff, 0) == 0);
            // second request has transparent background
            assert (colourCounts2.getOrDefault(0x00000000, 0) > 100);
            // second request no red boarders
            assert (borderCount(0x00ff0000, 0x0000ff00, colourCounts2) == 0);

            // larger dots in the second request
            assert (colourCounts1.getOrDefault(0xff0000ff, 0) < colourCounts2.getOrDefault(0xff00ff00, 0));

            // third request has green dots and no blue dots
            assert (colourCounts3.getOrDefault(0xff00ff00, 0) > 100);
            assert (colourCounts3.getOrDefault(0xff0000ff, 0) == 0);
            // third request has transparent background
            assert (colourCounts3.getOrDefault(0x00000000, 0) > 100);
            // third request has red boarders
            assert (borderCount(0x00ff0000, 0x0000ff00, colourCounts3) > 0);

        } catch (Exception e) {
            // exception not expected
            assert (false);
        }
    }

    @Test
    public void testWMSSel() throws Exception {
        String acceptType = "image/png";
        String url = "/mapping/wms/reflect";

        try {
            // with selection on all points, blue
            MvcResult mvcResult1 = this.mockMvc.perform(get(url).header("Accept", acceptType)
                    .param("BBOX", "0,-90,180,0")
                    .param("SRS", "EPSG:4326")
                    .param("ENV", "color%3A0000ff%3Bname%3Acircle%3Bsize%3A4%3Bopacity%3A0.8%3Bsel%3A*:*")
                    .param("OUTLINE", "false"))
                    .andExpect(status().isOk()).andReturn();
            Map<Integer, Integer> colourCounts1 = imageColourCount(mvcResult1.getResponse().getContentAsByteArray());
            Files.write(new File(targetDirectory, "testWMSSel.1.png").toPath(), mvcResult1.getResponse().getContentAsByteArray());

            // without selection, green
            MvcResult mvcResult2 = this.mockMvc.perform(get(url).header("Accept", acceptType)
                    .param("BBOX", "0,-90,180,0")
                    .param("SRS", "EPSG:4326")
                    .param("ENV", "color%3A00ff00%3Bname%3Acircle%3Bsize%3A4")
                    .param("OUTLINE", "false"))
                    .andExpect(status().isOk()).andReturn();
            Map<Integer, Integer> colourCounts2 = imageColourCount(mvcResult2.getResponse().getContentAsByteArray());
            Files.write(new File(targetDirectory, "testWMSSel.2.png").toPath(), mvcResult2.getResponse().getContentAsByteArray());

            // first request has blue dots and no green dots
            assert (colourCounts1.getOrDefault(0xff0000ff, 0) > 100);
            assert (colourCounts1.getOrDefault(0xff00ff00, 0) == 0);
            //first request has transparent background
            assert (colourCounts1.getOrDefault(0x00000000, 0) > 100);
            // first request has a red boarder around points (test only works because of some high density red areas)
            assert (borderCount(0x00ff0000, 0x000000ff, colourCounts1) > 0);


            // second request has green dots and no blue dots
            assert (colourCounts2.getOrDefault(0xff00ff00, 0) > 100);
            assert (colourCounts2.getOrDefault(0xff0000ff, 0) == 0);
            // second request has transparent background
            assert (colourCounts2.getOrDefault(0x00000000, 0) > 100);
            // second request has no red border
            assert (borderCount(0x00ff0000, 0x0000ff00, colourCounts2) == 0);

        } catch (Exception e) {
            e.printStackTrace();
            // exception not expected
            assert (false);
        }
    }

    // Not suitable when border is black.
    static int borderCount(int borderColourNoAlpha, int pointColourNoAlpha, Map<Integer, Integer> counts) {
        // third request has black boarders
        AtomicInteger borderCount = new AtomicInteger();
        counts.forEach((k, v) -> {
            // remove blended point colour from drawn border
            if (((k.intValue() & (~pointColourNoAlpha)) & borderColourNoAlpha) != 0x00000000)
                borderCount.addAndGet(v);
        });

        return borderCount.get();
    }

    static Map<Integer, Integer> imageColourCount(byte[] bytes) throws Exception {
        InputStream is = new ByteArrayInputStream(bytes);

        BufferedImage bi = ImageIO.read(is);

        HashMap<Integer, Integer> counts = new HashMap();

        for (int y = 0; y < bi.getHeight(); y++) {
            for (int x = 0; x < bi.getWidth(); x++) {
                int colour = bi.getRGB(x, y);
                Integer count = counts.getOrDefault(colour, 0);
                counts.put(colour, ++count);
            }
        }

        return counts;
    }
}
