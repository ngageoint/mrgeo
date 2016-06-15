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

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mrgeo.data.accumulo.utils.AccumuloConnector;
import org.mrgeo.data.accumulo.utils.MrGeoAccumuloConstants;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsPyramidMetadataWriter;
import org.mrgeo.data.image.MrsPyramidMetadataWriterContext;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.image.MrsPyramidMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;

public class AccumuloMrsPyramidMetadataFileWriter implements MrsPyramidMetadataWriter
{

  
  private static final Logger log = LoggerFactory.getLogger(AccumuloMrsPyramidMetadataFileWriter.class);
  
  private final MrsImageDataProvider provider;

  private String workDir = null;
  

  public AccumuloMrsPyramidMetadataFileWriter(String workDir, MrsImageDataProvider provider,
                                              MrsPyramidMetadataWriterContext context)
  {
    //this.provider = (AccumuloMrsImageDataProvider)provider;
    this.workDir = workDir;
    this.provider = provider;
    //this.context = context;
  }
  
  /**
   * Constructor for HdfsMrsPyramidMetadataWriter.
   * @param provider MrsImageDataProvider
   * @param context MrsPyramidMetadataWriterContext
   */
  public AccumuloMrsPyramidMetadataFileWriter(MrsImageDataProvider provider,
                                              MrsPyramidMetadataWriterContext context)
  {
    //this.provider = (AccumuloMrsImageDataProvider)provider;
    this.provider = provider;
    //this.context = context;
  }
  
  
  /**
   * Write the (already loaded) metadata for the provider to Accumulo
   * @throws IOException
   * @see MrsPyramidMetadataWriter#write()
   */
  @Override
  public void write() throws IOException
  {
    MrsPyramidMetadata metadata = provider.getMetadataReader(null).read();

    // need to determine if the write is to a bulk dir
    
    write(metadata);
  } // end write
  
  
  @Override
  public void write(MrsPyramidMetadata metadata) throws IOException{
    // write the metadata object to hdfs
    Properties mrgeoAccProps = AccumuloConnector.getAccumuloProperties();
    ColumnVisibility cv;
    if(mrgeoAccProps.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_VIZ) == null){
      cv = new ColumnVisibility();
    } else {
      cv = new ColumnVisibility(mrgeoAccProps.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_VIZ));
    }
    Path path = new Path(workDir, "meta.rf");
    FileSystem fs = HadoopFileUtils.getFileSystem(path);
    if (fs.exists(path))
    {
      fs.delete(path, false);
    }
    
    log.debug("Saving metadata to " + path.toString());
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    String metadataStr = null;
    try{
      metadata.save(baos);
      metadataStr = baos.toString();
      baos.close();
      
    } catch(IOException ioe){
      throw new RuntimeException(ioe.getMessage());
    }
    
    
    FileSKVWriter metaWrite = FileOperations.getInstance().openWriter(path.toString(), fs, fs.getConf(), AccumuloConfiguration.getDefaultConfiguration());
    
    metaWrite.startDefaultLocalityGroup();
    
    Key metKey = new Key(MrGeoAccumuloConstants.MRGEO_ACC_METADATA,
        MrGeoAccumuloConstants.MRGEO_ACC_METADATA,
        MrGeoAccumuloConstants.MRGEO_ACC_CQALL);
    Value metValue = new Value(metadataStr.getBytes());
    metaWrite.append(metKey, metValue);
    metaWrite.close();  
    
  } // end write

  
} // end AccumuloMrsPyramidMetadataFileWriter
