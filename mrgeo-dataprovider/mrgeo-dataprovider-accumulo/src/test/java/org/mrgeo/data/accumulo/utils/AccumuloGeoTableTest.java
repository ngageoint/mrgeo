/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.data.accumulo.utils;

import junit.framework.Assert;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;


@SuppressWarnings("all") // Test code, not included in production
public class AccumuloGeoTableTest
{
	
	  protected String u = "root";
	  protected String pw = "secret";
	  protected String inst = "accumulo";
	  protected String zoo = "localhost:2181";
	  protected String authsStr = "A,B,C,D,E,F,G,U";

	@BeforeClass
	public static void init() throws Exception{} // end init

	@Before
	public void setup(){} // end setup
	
	@After
	public void teardown(){}
	
	//@Ignore
	  @Test
	  @Category(UnitTest.class)
	  public void testGetGeoTables() throws Exception{
		  ZooKeeperInstance zkinst = new ZooKeeperInstance(inst, zoo);
		  PasswordToken pwTok = new PasswordToken(pw.getBytes());
		  Connector conn = zkinst.getConnector(u, pwTok);
	    Assert.assertNotNull(conn);

	    PasswordToken token = new PasswordToken(pw.getBytes());
	    
	    //Authorizations auths = new Authorizations(authsStr.split(","));
	    Authorizations auths = new Authorizations("A,B,C,D,ROLE_USER,U".split(","));
	    System.out.println(auths.toString());
	    Hashtable<String, String> ht = AccumuloUtils.getGeoTables(null, token, auths, conn);
	    for(String k : ht.keySet()){
	    	System.out.println(k + " => " + ht.get(k));
	    }
	    
	    
	  } // end testConnectorAll
	  
	  
	  @Test
	  @Category(UnitTest.class)
	  public void testGetTile() throws Exception{
		  
		  ZooKeeperInstance zkinst = new ZooKeeperInstance(inst, zoo);
		  PasswordToken pwTok = new PasswordToken(pw.getBytes());
		  Connector conn = zkinst.getConnector(u, pwTok);
		  Assert.assertNotNull(conn);

		  PasswordToken token = new PasswordToken(pw.getBytes());
	    
		  Authorizations auths = new Authorizations(authsStr.split(","));
		  long start = 0;
		  long end = Long.MAX_VALUE;
		  Key sKey = AccumuloUtils.toKey(start);
		  Key eKey = AccumuloUtils.toKey(end);
		  Range r = new Range(sKey, eKey);
		  Scanner s = conn.createScanner("paris4", auths);
		  s.fetchColumnFamily(new Text(Integer.toString(10)));
		  s.setRange(r);
		  
		  Iterator<Entry<Key, Value>> it = s.iterator();
		  while(it.hasNext()){
			  Entry<Key, Value> ent = it.next();
			  if(ent == null){
				  return;
			  }
			  System.out.println("current key   = " + AccumuloUtils.toLong(ent.getKey().getRow()));
			  System.out.println("current value = " + ent.getValue().getSize());
		  }
		  
	  } // end testGetTile
	  
  
} // end AccumuloGeoTableTest
