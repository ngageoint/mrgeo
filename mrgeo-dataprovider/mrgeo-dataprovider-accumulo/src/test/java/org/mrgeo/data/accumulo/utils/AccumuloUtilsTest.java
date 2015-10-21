/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.data.accumulo.utils;

import junit.framework.Assert;
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
