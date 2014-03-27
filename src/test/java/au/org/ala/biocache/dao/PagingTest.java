/**************************************************************************
 *  Copyright (C) 2011 Atlas of Living Australia
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
package au.org.ala.biocache.dao;

import junit.framework.TestCase;
import au.org.ala.biocache.model.FullRecord;
import au.org.ala.biocache.OccurrenceConsumer;
import au.org.ala.biocache.Store;
import au.org.ala.biocache.model.Versions;
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
