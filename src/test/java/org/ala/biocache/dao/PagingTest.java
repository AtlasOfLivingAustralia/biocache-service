package org.ala.biocache.dao;

import junit.framework.TestCase;
import au.org.ala.biocache.FullRecord;
import au.org.ala.biocache.OccurrenceConsumer;
import au.org.ala.biocache.Store;
import au.org.ala.biocache.Versions;
import org.junit.Ignore;

@Ignore
public class PagingTest extends TestCase {

	/**
	 * A junit test that test paging functionality.
	 */
	public void testPaging(){
		
		Store.pageOverAll(Versions.RAW(), new OccurrenceConsumer(){
            int counter = 0;

			public boolean consume(FullRecord fullrecord) {
				//System.out.println("GUID: "+ fullrecord.getO().getUuid());
                counter++;
                if(counter>10){
                    return false;
                }
                return true;
			}
		},null, 10);
	}

	public void testDownload(){
		
		String[] uuids = new String[]{
				"0000b9e7-65b4-4335-b012-60cdb13a91fb",
				"0000eb51-ea32-4693-a0ce-f9dbf025d212",
				"0001b51b-32d7-48a8-9f67-3563cba731f3"};
		
		System.out.println("Raw values");
		Store.writeToStream(System.out, "\t", "\n", uuids,  new String[]{"uuid","scientificName", "eventDate"}, new String[] {});
		
		System.out.println("Processed values");
		Store.writeToStream(System.out, "\t", "\n", uuids,  new String[]{"uuid","scientificName", "eventDate"}, new String[] {});
	}
}
