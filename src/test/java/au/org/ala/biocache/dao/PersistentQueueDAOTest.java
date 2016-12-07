package au.org.ala.biocache.dao;

import au.org.ala.biocache.dto.DownloadDetailsDTO;
import au.org.ala.biocache.dto.DownloadDetailsDTO.DownloadType;
import au.org.ala.biocache.dto.DownloadRequestParams;
import au.org.ala.biocache.dto.FacetThemes;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.nio.file.Files;
import java.nio.file.Paths;

public class PersistentQueueDAOTest {

    protected final JsonPersistentQueueDAOImpl queueDAO = new JsonPersistentQueueDAOImpl();
    
    @Before
    public void setUp() throws Exception{
        System.out.println("BEFORE...");

        //init FacetThemes
        new FacetThemes();

        FileUtils.deleteQuietly(new java.io.File("/data/cache/downloads"));
        Files.createDirectories(Paths.get("/", "data", "cache", "downloads"));
        queueDAO.init();
    }

    private DownloadRequestParams getParams(String query){
        DownloadRequestParams d = new DownloadRequestParams();
        d.setQ(query);
        d.setFile("Testing");
        d.setEmail("natasha.carter@csiro.au");
        return d;
    }
    
    @Test
    public void testQueue(){
        System.out.println("test add");
        DownloadDetailsDTO dd = new DownloadDetailsDTO(getParams("test1"), "127.0.0.1", DownloadType.FACET);
        
        queueDAO.addDownloadToQueue(dd);
        assertEquals(1,queueDAO.getTotalDownloads());
        DownloadDetailsDTO dd2 = new DownloadDetailsDTO(getParams("test2"), "127.0.0.1", DownloadType.FACET);
        
        queueDAO.addDownloadToQueue(dd2);
        assertEquals(2,queueDAO.getTotalDownloads());
        //now test that they are persisted
        queueDAO.refreshFromPersistent();
        assertEquals(2,queueDAO.getTotalDownloads());

        //now remove
        queueDAO.removeDownloadFromQueue(queueDAO.getNextDownload());
        assertEquals(1,queueDAO.getTotalDownloads());
        //now test that the removal has been persisted
        queueDAO.refreshFromPersistent();
        assertEquals(1,queueDAO.getTotalDownloads());
    }
}