package org.mrgeo.core;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;

import java.util.Properties;

import static org.junit.Assert.*;
import static org.mrgeo.core.MrGeoProperties.MRGEO_ENCRYPTION_MASTER_PASSWORD_PROPERTY;

/**
 * Created by ericwood on 8/22/16.
 */
public class MrGeoPropertiesTest {

    private String ENCRYPTED_TEST_PROPERTY = "org.mrgeo.core.MrGeoPropertiesTest.testEncryptedProperty";
    private String EXPECTED_DECRYPTED_PROPERTY_VALUE = "org.mrgeo.core.MrGeoPropertiesTest.testEncryptedPropertyValue";
    private String ENCRYPTED_PROPETRY_VALUE =
            "ENC(BxWLj6cHbhZxRSu0sPvzJXaGNWiGyzYraZWqF1ODrS6KzNX3b5L8luDcQKgdhEvud4CKOUkUBlvimZ5MdMMbCNMgVjE8yEaw)";
    private String UNENCRYPTED_TEST_PROPERTY = "org.mrgeo.core.MrGeoPropertiesTest.testUnencryptedProperty";
    private String EXPECTED_UNENCRYPTED_PROPERTY_VALUE = "org.mrgeo.core.MrGeoPropertiesTest.testUnencryptedPropertyValue";
    private String TEST_MASTER_PASSWORD = "testMasterPassword";

    private Properties mrGeoProperties = null;


    @Before
    public void setUp() throws Exception {
        // Force the properties to null so they get reinitialized
        MrGeoProperties.properties = null;
    }

    @After
    public void tearDown() throws Exception {
        mrGeoProperties.clear();
        System.clearProperty(MRGEO_ENCRYPTION_MASTER_PASSWORD_PROPERTY);
    }

    @Test
    @Category(UnitTest.class)
    public void testGettingEncryptedPropertyWhenMasterPasswordIsSet() {
        setUpPropererties(true);
        assertEquals(EXPECTED_DECRYPTED_PROPERTY_VALUE, mrGeoProperties.getProperty(ENCRYPTED_TEST_PROPERTY));
    }

    @Test
    @Category(UnitTest.class)
    public void testGettingUnencryptedPropertyWhenMasterPasswordIsSet() {
        setUpPropererties(true);
        assertEquals(EXPECTED_UNENCRYPTED_PROPERTY_VALUE, mrGeoProperties.getProperty(UNENCRYPTED_TEST_PROPERTY));
    }

    @Test
    @Category(UnitTest.class)
    public void testGettingUnencryptedPropertyWhenMasterPasswordIsNotSet() {
        setUpPropererties(false);
        assertEquals(EXPECTED_UNENCRYPTED_PROPERTY_VALUE, mrGeoProperties.getProperty(UNENCRYPTED_TEST_PROPERTY));
    }

    private void setUpPropererties(boolean useMasterPassword) {
        if (useMasterPassword) {
            // Set a master password
            System.setProperty(MRGEO_ENCRYPTION_MASTER_PASSWORD_PROPERTY, TEST_MASTER_PASSWORD);
        }
        // Get the properties.  Becasue a reference to the properties is returned, we can insert properties to test
        mrGeoProperties = MrGeoProperties.getInstance();
        // Add the encrypted and unencrypted properties
        mrGeoProperties.setProperty(ENCRYPTED_TEST_PROPERTY, ENCRYPTED_PROPETRY_VALUE);
        mrGeoProperties.setProperty(UNENCRYPTED_TEST_PROPERTY, EXPECTED_UNENCRYPTED_PROPERTY_VALUE);
    }

}