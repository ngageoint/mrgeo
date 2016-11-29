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

package org.mrgeo.data.accumulo.metadata;

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
@SuppressWarnings("all") // Test code, not included in production
public class AccumuloMrsPyramidMetadataWriterTest
{

  private static String junk = "junk";

  private File file;
  
  private AccumuloMrsImageDataProvider provider;
  private AccumuloMrsPyramidMetadataWriter writer;
  private AccumuloMrsPyramidMetadataReader reader;
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

    writer = (AccumuloMrsPyramidMetadataWriter) provider.getMetadataWriter();
    writer.setConnector(conn);

    reader = (AccumuloMrsPyramidMetadataReader) provider.getMetadataReader();
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
  } // end finalizeExternalSave

  
  @Ignore
  @Test
  @Category(UnitTest.class)
  public void testWrite() throws IOException
  {

    Assert.assertEquals("Classification incorrect", Classification.Continuous, metadata.getClassification());
    Assert.assertEquals("Max zoom incorrect", 10, metadata.getMaxZoomLevel());
    Assert.assertEquals("Tile size incorrect", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT, metadata.getTilesize());

    // set a couple values in meta.
    metadata.setClassification(Classification.Categorical);
    metadata.setMaxZoomLevel(100);
    metadata.setTilesize(100);
    
    // check the values changed
    Assert.assertEquals("Classification wasn't overwritten correctly", Classification.Categorical, metadata.getClassification());
    Assert.assertEquals("Max zoom wasn't overwritten correctly", 100, metadata.getMaxZoomLevel());
    Assert.assertEquals("Tile size wasn't overwritten correctly", 100, metadata.getTilesize());
    
    writer.write(metadata);
    
    MrsPyramidMetadata newMeta = reader.read();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    newMeta.save(bos);
    String newMetaStr = new String(bos.toByteArray());
    bos.close();
    
    // check the values changed
    Assert.assertEquals("Classification wasn't overwritten correctly", Classification.Categorical, newMeta.getClassification());
    Assert.assertEquals("Max zoom wasn't overwritten correctly", 100, newMeta.getMaxZoomLevel());
    Assert.assertEquals("Tile size wasn't overwritten correctly", 100, newMeta.getTilesize());
    
    // check the entire JSON
    Assert.assertFalse("JSON dumps are same!", originalMeta.equals(newMetaStr));
    
    // now set the metadata back to the old values
    metadata.setClassification(Classification.Continuous);
    metadata.setMaxZoomLevel(10);
    metadata.setTilesize(MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT);
    
    writer.write(metadata);
  
  } // end testWrite


  @Ignore
  @Test
  @Category(UnitTest.class)
  public void testWriteProvided() throws IOException
  {
    Assert.assertEquals("Classification incorrect", Classification.Continuous, metadata.getClassification());
    Assert.assertEquals("Max zoom incorrect", 10, metadata.getMaxZoomLevel());
    Assert.assertEquals("Tile size incorrect", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT, metadata.getTilesize());
    
    // set a couple values in meta.
    metadata.setClassification(Classification.Categorical);
    metadata.setMaxZoomLevel(100);
    metadata.setTilesize(100);
    
    // check the values changed
    Assert.assertEquals("Classification wasn't overwritten correctly", Classification.Categorical, metadata.getClassification());
    Assert.assertEquals("Max zoom wasn't overwritten correctly", 100, metadata.getMaxZoomLevel());
    Assert.assertEquals("Tile size wasn't overwritten correctly", 100, metadata.getTilesize());

    // write the metadata
    writer.write(metadata);
    
    MrsPyramidMetadata newMeta = reader.read();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    newMeta.save(bos);
    String newMetaStr = new String(bos.toByteArray());
    bos.close();

    // check the values changed
    Assert.assertEquals("Classification wasn't overwritten correctly", Classification.Categorical, newMeta.getClassification());
    Assert.assertEquals("Max zoom wasn't overwritten correctly", 100, newMeta.getMaxZoomLevel());
    Assert.assertEquals("Tile size wasn't overwritten correctly", 100, newMeta.getTilesize());

    // check the entire JSON
    Assert.assertFalse("JSON dumps are same!", originalMeta.equals(newMetaStr));
    
    // now set the metadata back to the old values
    metadata.setClassification(Classification.Continuous);
    metadata.setMaxZoomLevel(10);
    metadata.setTilesize(MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT);
    
    writer.write(metadata);
    
  } // end testWriteProvided


  @Ignore
  @Test
  @Category(UnitTest.class)
  public void testWriteReloadWorks() throws IOException
  {    
    metadata = reader.read();
    
    Assert.assertEquals("Classification incorrect", Classification.Continuous, metadata.getClassification());
    Assert.assertEquals("Max zoom incorrect", 10, metadata.getMaxZoomLevel());
    Assert.assertEquals("Tile size incorrect", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT, metadata.getTilesize());
    
    // make a new metadata object
    MrsPyramidMetadata newmeta = new MrsPyramidMetadata();
    
    // set a couple values in meta.
    newmeta.setPyramid(metadata.getPyramid());
    newmeta.setClassification(Classification.Categorical);
    newmeta.setMaxZoomLevel(100);
    newmeta.setTilesize(100);

    // write the metadata
    writer.write(newmeta);

    // check the values changed in the original metadata
    Assert.assertEquals("Classification wasn't overwritten correctly", Classification.Categorical, metadata.getClassification());
    Assert.assertEquals("Max zoom wasn't overwritten correctly", 100, metadata.getMaxZoomLevel());
    Assert.assertEquals("Tile size wasn't overwritten correctly", 100, metadata.getTilesize());
    
  }

} // end AccumuloMrsPyramidMetadataWriterTest
