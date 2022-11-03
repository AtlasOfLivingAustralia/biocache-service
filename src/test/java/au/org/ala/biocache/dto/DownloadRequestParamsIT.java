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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link DownloadRequestDTO}
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
     * Test for {@link DownloadRequestDTO#setFileType(String)}.
     */
    @Test
    public final void testSetFileType() throws Exception {

        DownloadRequestDTO downloadRequestDTO = new DownloadRequestDTO();
        downloadRequestDTO.setFileType("csv");
        assertTrue("csv".equals(downloadRequestDTO.getFileType()));

        downloadRequestDTO = new DownloadRequestDTO();
        downloadRequestDTO.setFileType("tsv");
        assertTrue("tsv".equals(downloadRequestDTO.getFileType()));
    }

    /**
     * Test for {@link DownloadRequestDTO#setEmailTemplate(String)}.
     */
    @Test
    public final void testSetEmailTemplate() throws Exception {

        DownloadRequestDTO downloadRequestDTO = new DownloadRequestDTO();
        assertTrue("default".equals(downloadRequestDTO.getEmailTemplate()));

        downloadRequestDTO = new DownloadRequestDTO();
        downloadRequestDTO.setEmailTemplate("doi");
        assertTrue("doi".equals(downloadRequestDTO.getEmailTemplate()));

        downloadRequestDTO = new DownloadRequestDTO();
        downloadRequestDTO.setEmailTemplate("csdm");
        assertTrue("csdm".equals(downloadRequestDTO.getEmailTemplate()));

        downloadRequestDTO = new DownloadRequestDTO();
        downloadRequestDTO.setEmailTemplate("uuidaof");
        assertFalse("uuidaof".equals(downloadRequestDTO.getEmailTemplate()));
        assertTrue("default".equals(downloadRequestDTO.getEmailTemplate()));
    }
}
