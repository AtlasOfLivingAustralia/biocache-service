package au.org.ala.biocache.dao;

import au.org.ala.biocache.dto.DownloadDetailsDTO;
import au.org.ala.biocache.dto.DownloadDetailsDTO.DownloadType;
import au.org.ala.biocache.dto.DownloadRequestParams;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PersistentQueueDAOTest {

    protected static final JsonPersistentQueueDAOImpl queueDAO = new JsonPersistentQueueDAOImpl();
    
    @Before
    public void setup(){
        System.out.println("BEFORE...");
        FileUtils.deleteQuietly(new java.io.File("/data/cache/downloads"));
        queueDAO.init();
    }

    private DownloadRequestParams getParams(String query){
        DownloadRequestParams d = new DownloadRequestParams();
        d.setQ(query);
        d.setFile("Testing");
        d.setEmail("natasha.carter@csiro.au");
        return d;
    }
    
    private void addQueue(String title){
        DownloadDetailsDTO dd = new DownloadDetailsDTO(getParams(title), "127.0.0.1", DownloadType.FACET);        
        queueDAO.addDownloadToQueue(dd);
    }
    
    @Test
    public void testAdd(){
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
    }
    
    @Test
    public void testRemove(){
        //set up some test data so that the remove operation can be tested correctly
        addQueue("test1");
        addQueue("test2");
        DownloadDetailsDTO dd = queueDAO.getNextDownload();
        assertEquals("?q=test1", dd.getDownloadParams());
        //all thedownloads should still be on the queue
        assertEquals(2,queueDAO.getTotalDownloads());
        //now remove
        queueDAO.removeDownloadFromQueue(dd);
        assertEquals(1,queueDAO.getTotalDownloads());
        //now test that the removal has been persisted
        queueDAO.refreshFromPersistent();
        assertEquals(1,queueDAO.getTotalDownloads());
    }
}