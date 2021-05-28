package au.org.ala.biocache.dto;

import au.org.ala.biocache.dao.IndexDAO;
import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.service.DownloadService;
import au.org.ala.biocache.service.SpeciesImageService;
import au.org.ala.biocache.util.QueryFormatUtils;
import au.org.ala.biocache.util.SolrUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.InvalidPropertyException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link DownloadRequestParams}
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:springTest.xml"})
@WebAppConfiguration
public class DownloadRequestParamsIT {

    @Autowired
    protected DownloadService downloadService;

    @Autowired
    protected SpeciesImageService speciesImageService;

    @Autowired
    protected IndexDAO indexDAO;

    @Autowired
    protected SearchDAO searchDAO;

    @Autowired
    protected QueryFormatUtils queryFormatUtils;

    static {
        System.setProperty("biocache.config", System.getProperty("user.dir") + "/src/test/resources/biocache-test-config.properties");
    }

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        SolrUtils.setupIndex();
    }

    /**
     * Test for {@link DownloadRequestParams#setFileType(String)}.
     */
    @Test
    public final void testSetFileType() throws Exception {

        DownloadRequestParams downloadRequestParams = new DownloadRequestParams();
        downloadRequestParams.setFileType("csv");
        assertTrue("csv".equals(downloadRequestParams.getFileType()));

        downloadRequestParams = new DownloadRequestParams();
        downloadRequestParams.setFileType("tsv");
        assertTrue("tsv".equals(downloadRequestParams.getFileType()));

        // permit fileType=shp
        downloadService.setDownloadShpEnabled(true);
        downloadRequestParams = new DownloadRequestParams();
        downloadRequestParams.setFileType("shp");
        assertTrue("shp".equals(downloadRequestParams.getFileType()));

        // forbid fileType=shp and expect an exception when setting
        downloadService.setDownloadShpEnabled(false);
        downloadRequestParams = new DownloadRequestParams();
        boolean exception = false;
        try {
            downloadRequestParams.setFileType("shp");
        } catch (InvalidPropertyException e) {
            exception = true;
        }
        assertTrue(exception);
        assertFalse("shp".equals(downloadRequestParams.getFileType()));
    }

    /**
     * Test for {@link DownloadRequestParams#setEmailTemplate(String)}.
     */
    @Test
    public final void testSetEmailTemplate() throws Exception {

        DownloadRequestParams downloadRequestParams = new DownloadRequestParams();
        assertTrue("default".equals(downloadRequestParams.getEmailTemplate()));

        downloadRequestParams = new DownloadRequestParams();
        downloadRequestParams.setEmailTemplate("doi");
        assertTrue("doi".equals(downloadRequestParams.getEmailTemplate()));

        downloadRequestParams = new DownloadRequestParams();
        downloadRequestParams.setEmailTemplate("csdm");
        assertTrue("csdm".equals(downloadRequestParams.getEmailTemplate()));

        downloadRequestParams = new DownloadRequestParams();
        downloadRequestParams.setEmailTemplate("uuidaof");
        assertFalse("uuidaof".equals(downloadRequestParams.getEmailTemplate()));
        assertTrue("default".equals(downloadRequestParams.getEmailTemplate()));
    }
}
