/**************************************************************************
 *  Copyright (C) 2013 Atlas of Living Australia
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
package au.org.ala.biocache.util;

import au.org.ala.biocache.dto.ContactDTO;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Utilities for dealing with contacts and user roles.
 */
@Component("contactUtils")
public class ContactUtils {

    private final static Logger logger = LoggerFactory.getLogger(ContactUtils.class);

    @Value("${registry.url:\"http://collections.ala.org.au\"}")
    protected String registryUrl;
    @Value("${contacts.url:\"http://collections.ala.org.au/ws/collection\"}")
    protected String collectionContactsUrl;

    @Inject
    private org.springframework.web.client.RestOperations restTemplate;

    /**
     * Retrieve a list of contacts for this UID.
     *
     * @param collectionUid
     * @return list of contacts
     */
    public List<ContactDTO> getContactsForUID(String collectionUid){

        if(collectionUid ==null){
            return new ArrayList<ContactDTO>();
        }

        List<ContactDTO> contactDTOs = new ArrayList<ContactDTO>();

        final String jsonUri = collectionContactsUrl + "/" + collectionUid + "/contacts.json";

        List<Map<String, Object>> contacts = restTemplate.getForObject(jsonUri, List.class);
        logger.debug("number of contacts = " + contacts.size());

        for (Map<String, Object> contact : contacts) {
            Map<String, String> details = (Map<String, String>) contact.get("contact");
            String email = details.get("email");

            String title = details.get("title");
            String firstName = details.get("firstName");
            String lastName = details.get("lastName");
            String phone = details.get("phone");

            logger.debug("email = " + email);

            ContactDTO c = new ContactDTO();
            c.setEmail(email);

            StringBuffer sb = new StringBuffer();
            if(StringUtils.isNotEmpty(title)){
                sb.append(title);
                sb.append(" ");
            }
            sb.append(firstName);
            if(sb.length()>0){
                sb.append(" ");
            }
            sb.append(lastName);
            c.setDisplayName(sb.toString());
            c.setPhone(phone);
            c.setRole((String) contact.get("role"));

            contactDTOs.add(c);
        }

        return contactDTOs;
    }

    public boolean isCollectionsAdmin(String collectionUid, String userId){
        final String jsonUri = collectionContactsUrl + "/" + collectionUid + "/contacts.json";
        logger.debug("Requesting: " + jsonUri);
        List<Map<String, Object>> contacts = restTemplate.getForObject(jsonUri, List.class);
        logger.debug("number of contacts = " + contacts.size());

        for (Map<String, Object> contact : contacts) {
            Map<String, String> details = (Map<String, String>) contact.get("contact");
            String email = details.get("email");
            logger.debug("email = " + email);
            if (userId.equalsIgnoreCase(email)) {
                logger.info("Logged in user has collection admin rights: " + email);
                return true;
            }
        }
        return false;
    }

    /**
     * Return contact details for this user if they are a collection manager etc
     *
     * @param email
     * @param uid
     * @return
     */
    public ContactDTO getContactForEmailAndUid(String email, String uid){
        if(email == null){
            return null;
        }
        List<ContactDTO> contacts = getContactsForUID(uid);
        for(ContactDTO contact : contacts){
            if(email.toLowerCase().equals(contact.getEmail().toLowerCase())){
                return contact;
            }
        }
        return null;
    }

    public void setRegistryUrl(String registryUrl) {
        this.registryUrl = registryUrl;
    }

    public void setCollectionContactsUrl(String collectionContactsUrl) {
        this.collectionContactsUrl = collectionContactsUrl;
    }

    public void setRestTemplate(RestOperations restTemplate) {
        this.restTemplate = restTemplate;
    }
}