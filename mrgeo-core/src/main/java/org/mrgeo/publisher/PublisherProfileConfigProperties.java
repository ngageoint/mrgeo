package org.mrgeo.publisher;

import org.mrgeo.core.MrGeoProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by ericwood on 8/17/16.
 */
public class PublisherProfileConfigProperties {

    public static final String MRGEO_PUBLISHER_PROPERTY_PREFIX = "mrgeo.publisher";

    /**
     * Gets the publisher profile properties and groups them by profile name
     */
    static Map<String, Properties> getPropertiesByProfileName() {
        // Get all Publisher properties for each profile.  Put them in a map by profile name
        Map<String, Properties> mapPublisherProps = new HashMap<>();
        Properties mrgeoProps = MrGeoProperties.getInstance();
//        Properties mrgeoProps = new Properties();
//        mrgeoProps.putAll(MrGeoProperties.getInstance());
        // TODO may want to add system properties as well, so a MRGEO setting gould be overriden at runtime.
        for (Map.Entry<Object, Object> prop: mrgeoProps.entrySet()) {
            String propName = prop.getKey().toString();
            String[] propParts = propName.split(MRGEO_PUBLISHER_PROPERTY_PREFIX);
            // If the string is not found then the array will have 1 element.  If the propname starts with the string
            // the size of the first element will be 0.
            if (propParts.length == 1 || propParts[0].length() != 0) continue;
            // Split on first occurence of "."
            propParts = propParts[1].substring(1).split("\\.", 2);
            String profileName = propParts[0];
            Properties profileProps = mapPublisherProps.get(profileName);
            if (profileProps == null) {
                // Clone the MrGeo Props instance so we preserve its behavior, (i.e. encrypted properties support), but
                // can have different properties
                profileProps = (Properties) mrgeoProps.clone();
                profileProps.clear();
                mapPublisherProps.put(profileName, profileProps);
            }
            profileProps.put(propParts[1], prop.getValue());
        }
        return mapPublisherProps;
    }

}
