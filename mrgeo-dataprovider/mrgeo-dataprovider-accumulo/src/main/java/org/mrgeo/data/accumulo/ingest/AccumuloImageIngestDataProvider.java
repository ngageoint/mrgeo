/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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

package org.mrgeo.data.accumulo.ingest;

import com.sun.org.apache.xml.internal.security.exceptions.Base64DecodingException;
import com.sun.org.apache.xml.internal.security.utils.Base64;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperationsImpl;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.commons.lang.NotImplementedException;
import org.mrgeo.data.accumulo.utils.AccumuloConnector;
import org.mrgeo.data.accumulo.utils.MrGeoAccumuloConstants;
import org.mrgeo.data.ingest.ImageIngestDataProvider;
import org.mrgeo.data.ingest.ImageIngestRawInputFormatProvider;
import org.mrgeo.data.ingest.ImageIngestTiledInputFormatProvider;
import org.mrgeo.data.ingest.ImageIngestWriterContext;
import org.mrgeo.data.tile.MrsTileWriter;

import java.awt.image.Raster;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;


public class AccumuloImageIngestDataProvider extends ImageIngestDataProvider
{
  // The following should store the original resource name and any Accumulo
  // property settings required to access that resource. Internally, this
  // data provider should make use of the resolved resource name. It should
  // never use this property directly, but instead call 
  private String resolvedResourceName;

  public AccumuloImageIngestDataProvider(String resourceName)
  {
    super();
    // TODO: Determine if the resourceName is resolved or not. If it is, then call
    // setResourceName. If not, then we'll need to resolve it on demand - see
    // getResolvedResourceName().
    boolean nameIsResolved = false;
    if (nameIsResolved)
    {
      resolvedResourceName = resourceName;
    }
    else
    {
      setResourceName(resourceName);
    }
  }

  /**
   * If the resourceName is null, then this method should extract/compute it
   * from the resolvedResourceName and return it.
   */
  @Override
  public String getResourceName()
  {
    String result = super.getResourceName();
    if (result == null)
    {
      // TODO: Compute the resource name from the resolved name, and set it.
      result = resolvedResourceName;
      setResourceName(resolvedResourceName);
    }
    return result;
  }

  /**
   * If the resolvedResourceName is null, then it computes it based on the
   * resourceName and the Accumulo configuration properties. The
   * resolvedResourceName consists of the resourceName and the properties encoded
   * together into a single String which is only used internally to this
   * plugin.
   * 
   * @return
   */
  public String getResolvedName()
  {
    if (resolvedResourceName == null)
    {
      // TODO: Compute the resolved resource name and assign it.
      resolvedResourceName = getResourceName();
    }
    return resolvedResourceName;
  }

  @Override
  public InputStream openImage() throws IOException
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ImageIngestTiledInputFormatProvider getTiledInputFormat()
  {
    // TODO Auto-generated method stub
    //return new AccumuloImageIngestTiledInputFormatProvider();
    return null;
  }

  
  @Override
  public MrsTileWriter<Raster> getMrsTileWriter(ImageIngestWriterContext context)
      throws IOException
  {
    // TODO Auto-generated method stub
    return null;
  }

  
  @Override
  public ImageIngestRawInputFormatProvider getRawInputFormat()
  {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * 
   * @param name
   * @return
   * @throws IOException
   */
  public static boolean canOpen(final String name) throws IOException {

    if(name.equals("!METADATA") || name.equals("trace")){
      return false;
    }
    

    ArrayList<String> tables = listGeoTables();
    return tables.contains(name);
    
    
  } // end canOpen
  
  
  public static boolean canWrite(final String name){
    try{
      return canOpen(name);
    } catch(IOException ioe){
      
    } finally {
      return false;
    }
  } // end canWrite
  
  
  public static ArrayList<String> listGeoTables() throws IOException{
    ArrayList<String> retList = new ArrayList<String>();

    
    Properties props = AccumuloConnector.getAccumuloProperties();
    if(props == null){
      throw new IOException("Problem connecting to Accumulo.");      
    }

    Authorizations auths = new Authorizations();
    if(props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_AUTHS) != null){
      auths = new Authorizations(props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_AUTHS).split(","));
    }
    
    byte[] pw;
    String enc = props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PWENCODED64, "false");
    try{
      if(Boolean.parseBoolean(enc)){
        pw = Base64.decode(props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD));
      } else {
        pw = props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD).getBytes();
      }
    } catch(Base64DecodingException b64){
      throw new IOException(b64.getMessage());
    }
    
    TCredentials tcr = new TCredentials();
    tcr.setInstanceId(props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_INSTANCE));
    tcr.setPrincipal(props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_USER));
    tcr.setToken(pw);
    
    Connector conn = AccumuloConnector.getConnector(props);
    // get the list of tables

    TableOperationsImpl toi = new TableOperationsImpl(conn.getInstance(), tcr);
    Set<String> list = toi.list();


    for(String l : list){
      
      if(l.equals("!METADATA") || l.equals("trace")){
        continue;
      }
      
      
      //System.out.println("Looking at table: " + l);
      Scanner scann = null;
      try{
        scann = conn.createScanner(l, auths);
        String m1 = "METADATA";
        String m2 = m1 + 1;
        Range r = new Range("METADATA", m2);
          
        scann.setRange(r);
          
        Iterator<Entry<Key, Value>> it = scann.iterator();
        while(it.hasNext()){
          
          Entry<Key, Value> e = it.next();
          //System.out.println("\tKey:   " + e.getKey().toString());
          //System.out.println("\tValue: " + e.getValue().toString());
          String row = e.getKey().getRow().toString();
          String cf = e.getKey().getColumnFamily().toString();
          String cq = e.getKey().getColumnQualifier().toString();
          String v = e.getValue().toString();
  
          if(row.equals(MrGeoAccumuloConstants.MRGEO_ACC_METADATA) &&
              cf.equals(MrGeoAccumuloConstants.MRGEO_ACC_CQALL) && 
              cq.equals(MrGeoAccumuloConstants.MRGEO_ACC_CQALL)){
            retList.add(l);
          }
          
        } // end while
            
        scann.clearColumns();
      } catch(TableNotFoundException tnfe){
        tnfe.printStackTrace();
      } catch(Exception e){
        e.printStackTrace();
      }

    }
    
    return retList;
  } // end 
  
  

  @Override
  public void delete() throws IOException
  {
    // TODO Auto-generated method stub
    throw new NotImplementedException("Delete not implemented.");
  }


  public static void delete(String name) throws IOException{
    // do nothing
  }

  @Override
  public void move(String toResource) throws IOException
  {
    throw new NotImplementedException("We do not move tables in Accumulo!");
    
  }
  
  public static boolean exists(String name) throws IOException{
    ArrayList<String> list = listGeoTables();
    return list.contains(name);
  }

  
  
} // end AccumuloImageIngestDataProvider
