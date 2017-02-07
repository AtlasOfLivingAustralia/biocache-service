/**************************************************************************
 *  Copyright (C) 2016 Atlas of Living Australia
 *  All Rights Reserved.
 *
 *  The contents of this file are subject to the Mozilla Public
 *  License Version 1.1 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://www.mozilla.org/MPL/
 *
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 ***************************************************************************/
package au.org.ala.biocache.util.thread;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

import au.org.ala.biocache.dto.DownloadDetailsDTO;

/**
 * An interface representing a BiFunction to create Callable<DownloadDetailsDTO> objects that can be processed by the DownloadService.
 * 
 * @author Peter Ansell
 */
public interface DownloadCreator {
    /**
     * Create a {@link Callable} from the given DownloadDetailsDTO that can be executed by an ExecutorService.
     * @param nextDownload The details of the download
     * @param executionDelay The number of milliseconds that this download should sleep for after being executed to debug and work around database connectivity issues
     * @param capacitySemaphore A Semaphore that is used to smoothly allocate downloads across DownloadControlThreads. {@link Semaphore#release()} must be called when the resulting Callable is finished downloading.
     * @param executorService The executorService that will be used to process subsets of the results in parallel
     * @return A Callable that can execute the download
     */
    Callable<DownloadDetailsDTO> createCallable(DownloadDetailsDTO nextDownload, long executionDelay, Semaphore capacitySemaphore, ExecutorService executorService);
}
