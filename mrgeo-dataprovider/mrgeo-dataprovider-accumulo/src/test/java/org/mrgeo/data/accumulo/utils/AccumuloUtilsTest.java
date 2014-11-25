package org.mrgeo.data.accumulo.utils;

import junit.framework.Assert;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;

public class AccumuloUtilsTest {

	@BeforeClass
	public static void init() throws Exception{} // end init

	@Before
	public void setup(){} // end setup
	
	@After
	public void teardown(){}
	
	@Test
	@Category(UnitTest.class)
	public void testValidateProtectionLevel(){
		
		String pl = "(A&B)";
		Assert.assertTrue(AccumuloUtils.validateProtectionLevel(pl));
		String pl2 = "A(C&D";
		Assert.assertFalse(AccumuloUtils.validateProtectionLevel(pl2));
		String pl3 = "";
		Assert.assertTrue(AccumuloUtils.validateProtectionLevel(pl3));
		
	}  // end testValidateProtectionLevel
	
} // end AccumuloUtilsTest
