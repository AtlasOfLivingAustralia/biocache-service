/**
 * 
 */
package au.org.ala.biocache.service;

import static org.junit.Assert.*;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.MessageSourceResolvable;
import org.springframework.context.NoSuchMessageException;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;

import au.org.ala.biocache.dao.JsonPersistentQueueDAOImpl;
import au.org.ala.biocache.dao.PersistentQueueDAO;
import au.org.ala.biocache.dao.SearchDAOImpl;
import au.org.ala.biocache.dto.DownloadDetailsDTO;
import au.org.ala.biocache.dto.FacetThemes;
import au.org.ala.biocache.util.thread.DownloadCreator;

/**
 * Test for {@link DownloadService}
 * 
 * @author Peter Ansell
 */
public class DownloadServiceTest {

    @Rule
    public Timeout timeout = new Timeout(30, TimeUnit.SECONDS);
    
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();
    
    private Path testCacheDir;
    private Path testDownloadDir;
    
    private PersistentQueueDAO persistentQueueDAO;

    private DownloadService testService;

    @Before
    public void setUp() throws Exception {
        testCacheDir = tempDir.newFolder("downloadcontrolthreadtest-cache").toPath();
        testDownloadDir = tempDir.newFolder("downloadcontrolthreadtest-destination").toPath();        
        persistentQueueDAO = new JsonPersistentQueueDAOImpl() {
            @Override
            public void init() {
                cacheDirectory = testCacheDir.toAbsolutePath().toString();
                biocacheDownloadDir = testDownloadDir.toAbsolutePath().toString();
                super.init();
            }
        };
        persistentQueueDAO.init();
        
        // Every application needs to explicitly initialise static fields in FacetThemes by calling its constructor
        new FacetThemes();
        
        SearchDAOImpl searchDAO = new SearchDAOImpl();
        
        testService = new DownloadService() {
            protected DownloadCreator getNewDownloadCreator() {
                return new DownloadCreator() {
                    @Override
                    public Callable<DownloadDetailsDTO> createCallable(final DownloadDetailsDTO nextDownload, final long executionDelay) {
                        return new Callable<DownloadDetailsDTO>() {
                            @Override
                            public DownloadDetailsDTO call() throws Exception {
                                Thread.sleep(executionDelay);
                                return nextDownload;
                            }
                        };
                    }
                };
            }
        };
        testService.persistentQueueDAO = persistentQueueDAO;
        testService.searchDAO = searchDAO;
    }

    @After
    public void tearDown() throws Exception {
        persistentQueueDAO.shutdown();
    }

    /**
     * Test method for {@link au.org.ala.biocache.service.DownloadService#afterInitialisation()}.
     */
    @Test
    public final void testAfterInitialisation() {
        final CountDownLatch latch = new CountDownLatch(1);
        Thread testThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    latch.await();
                    testService.afterInitialisation();
                } catch (InterruptedException e) {
                    fail("Interruption occurred: " + e.getMessage());
                }
            }
        });
        testThread.start();
        testService.init();
        
        latch.countDown();
        testService.afterInitialisation();
    }

    /**
     * Test method for {@link au.org.ala.biocache.service.DownloadService#init()}.
     */
    @Test
    public final void testInit() {
        testService.init();
    }

    /**
     * Test method for {@link au.org.ala.biocache.service.DownloadService#getNewDownloadCreator()}.
     */
    @Test
    public final void testGetNewDownloadCreator() {
        // TODO: This method is overriden to avoid referencing classes that are not yet setup for testing
        // Just verifies that the method continues to exist
        testService.getNewDownloadCreator();
    }

    /**
     * Test method for {@link au.org.ala.biocache.service.DownloadService#onApplicationEvent(org.springframework.context.event.ContextClosedEvent)}.
     */
    @Test
    public final void testOnApplicationEvent() {
        // Check that this method completes reliably
        testService.onApplicationEvent(new ContextClosedEvent(new GenericApplicationContext()));
    }

    /**
     * Test method for {@link au.org.ala.biocache.service.DownloadService#registerDownload(au.org.ala.biocache.dto.DownloadRequestParams, java.lang.String, au.org.ala.biocache.dto.DownloadDetailsDTO.DownloadType)}.
     */
    @Ignore("TODO: Implement me")
    @Test
    public final void testRegisterDownload() {
        fail("Not yet implemented"); // TODO
    }

    /**
     * Test method for {@link au.org.ala.biocache.service.DownloadService#unregisterDownload(au.org.ala.biocache.dto.DownloadDetailsDTO)}.
     */
    @Ignore("TODO: Implement me")
    @Test
    public final void testUnregisterDownload() {
        fail("Not yet implemented"); // TODO
    }

    /**
     * Test method for {@link au.org.ala.biocache.service.DownloadService#getCurrentDownloads()}.
     */
    @Ignore("TODO: Implement me")
    @Test
    public final void testGetCurrentDownloads() {
        fail("Not yet implemented"); // TODO
    }

    /**
     * Test method for {@link au.org.ala.biocache.service.DownloadService#writeQueryToStream(au.org.ala.biocache.dto.DownloadDetailsDTO, au.org.ala.biocache.dto.DownloadRequestParams, java.lang.String, java.io.OutputStream, boolean, boolean, boolean, boolean)}.
     */
    @Ignore("TODO: Implement me")
    @Test
    public final void testWriteQueryToStreamDownloadDetailsDTODownloadRequestParamsStringOutputStreamBooleanBooleanBooleanBoolean() {
        fail("Not yet implemented"); // TODO
    }

    /**
     * Test method for {@link au.org.ala.biocache.service.DownloadService#writeQueryToStream(au.org.ala.biocache.dto.DownloadRequestParams, javax.servlet.http.HttpServletResponse, java.lang.String, javax.servlet.ServletOutputStream, boolean, boolean, boolean)}.
     */
    @Ignore("TODO: Implement me")
    @Test
    public final void testWriteQueryToStreamDownloadRequestParamsHttpServletResponseStringServletOutputStreamBooleanBooleanBoolean() {
        fail("Not yet implemented"); // TODO
    }

    /**
     * Test method for {@link au.org.ala.biocache.service.DownloadService#getCitations(java.util.concurrent.ConcurrentMap, java.io.OutputStream, char, char, java.util.List)}.
     */
    @Ignore("TODO: Implement me")
    @Test
    public final void testGetCitations() {
        fail("Not yet implemented"); // TODO
    }

    /**
     * Test method for {@link au.org.ala.biocache.service.DownloadService#getHeadings(java.util.concurrent.ConcurrentMap, java.io.OutputStream, au.org.ala.biocache.dto.DownloadRequestParams, java.lang.String[])}.
     */
    @Ignore("TODO: Implement me")
    @Test
    public final void testGetHeadings() {
        fail("Not yet implemented"); // TODO
    }

}
