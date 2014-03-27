/**************************************************************************
 *  Copyright (C) 2010 Atlas of Living Australia
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
package au.org.ala.biocache.dto;

public class DataProviderCountDTO {

    protected String id;
    protected String name;
    protected long count = 0;
    
    public DataProviderCountDTO() {}
    
    public DataProviderCountDTO(String id, String name, long count) {
            super();
            this.id = id;
            this.name = name;
            this.count = count;
    }
    /**
     * @return the count
     */
    public long getCount() {
            return count;
    }
    /**
     * @param count the count to set
     */
    public void setCount(long count) {
            this.count = count;
    }
    /**
     * @return the id
     */
    public String getId() {
            return id;
    }
    /**
     * @param id the id to set
     */
    public void setId(String id) {
            this.id = id;
    }
    /**
     * @return the name
     */
    public String getName() {
            return name;
    }
    /**
     * @param name the name to set
     */
    public void setName(String name) {
            this.name = name;
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
            return "DataProviderCountDTO [count=" + count + ", id=" + id
                            + ", name=" + name + "]";
    }
}