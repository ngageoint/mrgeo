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

package org.mrgeo.data.accumulo.metadata;

import junit.framework.Assert;
import org.apache.accumulo.core.client.Connector;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.data.accumulo.AccumuloDefs;
import org.mrgeo.data.accumulo.image.AccumuloMrsImageDataProvider;
import org.mrgeo.data.accumulo.utils.AccumuloConnector;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.image.MrsPyramidMetadata.Classification;

import java.io.*;

@Ignore
public class AccumuloMrsImagePyramidMetadataReaderTest
{

  private static String junk = "junkadp";
  private static String badTable = "badTable";

  private File file;
  
  private AccumuloMrsImageDataProvider provider;
  private AccumuloMrsImagePyramidMetadataWriter writer;
  private AccumuloMrsImagePyramidMetadataReader reader;
  private MrsPyramidMetadata metadata;
  private String originalFileStr;
  private String originalMeta;
  
  private static Connector conn = null;
  
  @BeforeClass
  public static void init() throws Exception
  {
    //conn = AccumuloConnector.getConnector(AccumuloDefs.INSTANCE, AccumuloDefs.ZOOKEEPERS, AccumuloDefs.USER, AccumuloDefs.PASSWORD);
    conn = AccumuloConnector.getConnector();

    //conn = AccumuloConnector.getMockConnector(AccumuloDefs.INSTANCE, AccumuloDefs.USER, AccumuloDefs.PASSWORDBLANK);
    
    conn.tableOperations().create(junk);
    
  } // end init

  
  @Before
  public void setup() throws IOException
  {
    
    metadata = new MrsPyramidMetadata();
    
    provider = new AccumuloMrsImageDataProvider(junk);

    String fstr = AccumuloDefs.CWD + AccumuloDefs.INPUTDIR + AccumuloDefs.INPUTMETADATADIR + AccumuloDefs.INPUTMETADATAFILE;
    file = new File(fstr);

    writer = (AccumuloMrsImagePyramidMetadataWriter) provider.getMetadataWriter();
    writer.setConnector(conn);

    reader = (AccumuloMrsImagePyramidMetadataReader) provider.getMetadataReader();
    reader.setConnector(conn);
    
    FileInputStream fis = new FileInputStream(file);
    long length = file.length();
    byte[] b = new byte[(int)length];    
    fis.read(b);
    
    originalFileStr = new String(b);
    
    fis.close();

    ByteArrayInputStream bis = new ByteArrayInputStream(b);
    metadata = MrsPyramidMetadata.load(bis);
    bis.close();
    
    writer.write(metadata);
    
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    metadata.save(bos);
    originalMeta = new String(bos.toByteArray());
    bos.close();
    
  } // end setup

  @After
  public void teardown(){
    // get rid of the test table
//    try{
//      conn.tableOperations().delete(junk);
//    } catch(TableNotFoundException tnfe){
//      
//    } catch(AccumuloSecurityException ase){
//      
//    } catch(AccumuloException ae){
//      
//    }
  } // end teardown

  @Ignore
  @Test
  @Category(UnitTest.class)
  public void testRead() throws IOException
  {
    MrsPyramidMetadata meta = reader.read();
    
    Assert.assertEquals("Classification incorrect", Classification.Continuous, meta.getClassification());
    Assert.assertEquals("Max zoom incorrect", 10, meta.getMaxZoomLevel());
    Assert.assertEquals("Tile size incorrect", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT, meta.getTilesize());
  }

  @Ignore
  @Test(expected=IOException.class)
  @Category(UnitTest.class)
  public void testBadTable() throws IOException
  {
    provider = new AccumuloMrsImageDataProvider(badTable);
    reader = new AccumuloMrsImagePyramidMetadataReader(provider, null);
    reader.setConnector(conn);

    reader.read();
    
  }

  @Ignore
  @Test(expected=IOException.class)
  @Category(UnitTest.class)
  public void testReadNoProvider() throws IOException
  {
    reader = new AccumuloMrsImagePyramidMetadataReader(null, null);

    reader.read();
  }


  @Ignore
  @Test
  @Category(UnitTest.class)
  public void testReload() throws IOException
  {
    MrsPyramidMetadata meta = reader.read();
    
    final ByteArrayOutputStream orig = new ByteArrayOutputStream();
    meta.save(orig);
    
    String originalMetadata = orig.toString();

    // set a couple values in meta.
    meta.setClassification(Classification.Categorical);
    meta.setMaxZoomLevel(100);
    meta.setTilesize(100);
    
    // check the values changed
    Assert.assertEquals("Classification wasn't overwritten correctly", Classification.Categorical, meta.getClassification());
    Assert.assertEquals("Max zoom wasn't overwritten correctly", 100, meta.getMaxZoomLevel());
    Assert.assertEquals("Tile size wasn't overwritten correctly", 100, meta.getTilesize());

    // reload
    reader.reload();
    
    // check the values have reverted
    Assert.assertEquals("Classification wasn't reloaded correctly", Classification.Continuous, meta.getClassification());
    Assert.assertEquals("Max zoom wasn't reloaded correctly", 10, meta.getMaxZoomLevel());
    Assert.assertEquals("Tile size wasn't reloaded correctly", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT, meta.getTilesize());
    
    final ByteArrayOutputStream os = new ByteArrayOutputStream();
    meta.save(os);

    // check the entire JSON
    Assert.assertEquals("JSON dumps are different!", originalMetadata, os.toString());
  }

  
  
} // end AccumuloMrsImagePyramidMetadataReaderTest
