package org.mrgeo.core;

import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;

import java.util.Properties;

import static org.junit.Assert.*;
import static org.mrgeo.core.MrGeoProperties.MRGEO_ENCRYPTION_MASTER_PASSWORD_PROPERTY;

public class MrGeoPropertiesTest {

private String ENCRYPTED_TEST_PROPERTY = "org.mrgeo.core.MrGeoPropertiesTest.testEncryptedProperty";
private String UNENCRYPTED_TEST_PROPERTY = "org.mrgeo.core.MrGeoPropertiesTest.testUnencryptedProperty";
private String TEST_MASTER_PASSWORD = "testMasterPassword";

private String decryptedValue = "MrGeo is awesome!";
private String encryptedValue;




@Before
public void setUp() throws Exception {
  MrGeoProperties.clearProperties();

  StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
  encryptor.setPassword(TEST_MASTER_PASSWORD);
  encryptedValue = "ENC(" + encryptor.encrypt(decryptedValue) + ")";

}

@After
public void tearDown() throws Exception {
  System.clearProperty(MRGEO_ENCRYPTION_MASTER_PASSWORD_PROPERTY);
}


@Test
@Category(UnitTest.class)
public void testGettingUnencryptedPropertyWhenMasterPasswordIsSet() {
  setUpPropererties(true);
  assertEquals(decryptedValue, MrGeoProperties.getInstance().getProperty(ENCRYPTED_TEST_PROPERTY));
}

@Test
@Category(UnitTest.class)
public void testGettingUnencryptedPropertyWhenMasterPasswordIsNotSet() {
  setUpPropererties(false);
  assertEquals(encryptedValue, MrGeoProperties.getInstance().getProperty(ENCRYPTED_TEST_PROPERTY));
}


private void setUpPropererties(boolean useMasterPassword) {
  if (useMasterPassword) {
    // Set a master password
    System.setProperty(MRGEO_ENCRYPTION_MASTER_PASSWORD_PROPERTY, TEST_MASTER_PASSWORD);
  }
  // Add the encrypted and unencrypted properties
  MrGeoProperties.getInstance().setProperty(ENCRYPTED_TEST_PROPERTY, encryptedValue);
}

}