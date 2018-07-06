package au.org.ala.biocache.util;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class DwCTerms {

    private static final Logger logger = Logger.getLogger(DwCTerms.class);
    private static DwCTerms instance = null;
    Map<String, DwcTermDetails> collectedTerms = null;

    protected DwCTerms() {}

    public static DwCTerms getInstance() {
        if(instance == null) {
            instance = new DwCTerms();
        }
        return instance;
    }

    public DwcTermDetails getDwCTermDetails(String simpleName){

        if(this.collectedTerms == null) {
            try {
                XmlPullParserFactory factory = XmlPullParserFactory.newInstance();
                factory.setNamespaceAware(true);
                XmlPullParser xpp = factory.newPullParser();
                InputStream input = DwCTerms.class.getResourceAsStream("/dwcterms.rdf");
                xpp.setInput(input, "UTF-8");
                this.collectedTerms = processDocument(xpp);
            } catch (Exception e){
                logger.error("Unable to parse local copy of dwcterms.rdf", e);
            }
        }

        return collectedTerms.get(simpleName);
    }

    private static Map<String, DwcTermDetails>  processDocument(XmlPullParser xpp)
            throws XmlPullParserException, IOException {

        Map<String, DwcTermDetails> collectedTerms = new HashMap<String, DwcTermDetails>();

        int eventType = xpp.getEventType();

        String currentElement = null;
        DwcTermDetails dwCTermDetails = null;

        do {
            if(eventType == xpp.START_TAG) {
                String name = xpp.getName();

                if("Description".equalsIgnoreCase(name)){
                    if(dwCTermDetails != null){
                        collectedTerms.put(dwCTermDetails.term, dwCTermDetails);
                    }
                    //new term
                    dwCTermDetails = new DwcTermDetails();
                    dwCTermDetails.uri = xpp.getAttributeValue(0);
                    if(dwCTermDetails.uri.lastIndexOf("/") > 0){
                        dwCTermDetails.term = dwCTermDetails.uri.substring(dwCTermDetails.uri.lastIndexOf("/") + 1);
                    } else {
                        dwCTermDetails.term = dwCTermDetails.uri;
                    }
                }

                currentElement = name;

            } else if(eventType == xpp.TEXT) {
                if("label".equalsIgnoreCase(currentElement)){
                    String value = StringUtils.trimToNull(xpp.getText());
                    if(value !=null){
                        dwCTermDetails.label = value;
                    }
                }
                if("comment".equalsIgnoreCase(currentElement)){
                    String value = StringUtils.trimToNull(xpp.getText());
                    if(value !=null){
                        dwCTermDetails.comment = value;
                    }
                }
            }
            eventType = xpp.next();
        } while (eventType != xpp.END_DOCUMENT);

        return collectedTerms;
    }

}
