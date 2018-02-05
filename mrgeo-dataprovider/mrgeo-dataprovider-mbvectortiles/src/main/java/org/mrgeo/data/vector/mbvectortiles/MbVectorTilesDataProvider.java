/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.data.vector.mbvectortiles;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.vector.*;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.hdfs.vector.HdfsVectorDataProvider;
import org.mrgeo.utils.FileUtils;
import org.mrgeo.utils.tms.Bounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MbVectorTilesDataProvider extends VectorDataProvider
{
  static Logger log = LoggerFactory.getLogger(MbVectorTilesDataProvider.class);

  private static Map<String, String> localCopies = new HashMap<String, String>();

  private Configuration conf;

  protected static boolean canOpen(Configuration conf,
          String input,
          ProviderProperties providerProperties) throws IOException
  {
    MbVectorTilesSettings dbSettings = parseResourceName(input, conf, providerProperties);
    SQLiteConnection conn = null;
    try {
      conn = getDbConnection(dbSettings, conf);
      return true;
    }
    catch(IOException e) {
      log.info("Unable to open MB vector tiles database: " + dbSettings.getFilename(), e);
    }
    finally {
      if (conn != null) {
        conn.dispose();
      }
    }
    return false;
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File must be specified by the user")
  static SQLiteConnection getDbConnection(MbVectorTilesSettings dbSettings,
                                          Configuration conf) throws IOException
  {
    String filename = dbSettings.getFilename();
    Path filepath = new Path(filename);
    FileSystem fs = HadoopFileUtils.getFileSystem(conf, filepath);

    File dbFile = null;
    if (fs instanceof LocalFileSystem) {
      dbFile = new File(filepath.toUri().getPath());
    }
    else {
      String localName = localCopies.get(filename);
      if (localName == null) {
        dbFile = new File(FileUtils.createUniqueTmpDir(), new File(filename).getName());
        Path localFilePath = new Path("file://" + dbFile.getAbsolutePath());
        log.info("Attempting to copy MB tiles file " + filename +
                " to the local machine at " + dbFile.getAbsolutePath());
        fs.copyToLocalFile(false, filepath, localFilePath, true);
        dbFile.deleteOnExit();
        localCopies.put(filename, dbFile.getAbsolutePath());
      }
      else {
        log.info("Using a copy of " + filename +
                " already transferred to the local machine at " + localName);
        dbFile = new File(localName);
      }
    }

    try {
      return new SQLiteConnection(dbFile).open(false);
    }
    catch(SQLiteException e) {
      throw new IOException("Unable to open MB tiles file: " + dbFile.getAbsolutePath(), e);
    }
  }

  public MbVectorTilesDataProvider(Configuration conf, String inputPrefix,
                                   String input,
                                   ProviderProperties providerProperties)
  {
    super(inputPrefix, input, providerProperties);
    this.conf = conf;
  }

  @Override
  public VectorMetadataReader getMetadataReader()
  {
    // Not yet implemented. The metadata for mb tiles vector features
    // is potentially different for every feature. So it doesn't make
    // sense to provide metadata here.
    return null;
  }

  @Override
  public VectorMetadataWriter getMetadataWriter()
  {
    // Not yet implemented. The metadata for mb tiles vector features
    // is potentially different for every feature. So it doesn't make
    // sense to write metadata here.
    return null;
  }

  @Override
  public VectorReader getVectorReader() throws IOException
  {
    // Not yet implemented
    return null;
  }

  @Override
  public VectorReader getVectorReader(VectorReaderContext context) throws IOException
  {
    // Not yet implemented
    return null;
  }

  @Override
  public VectorWriter getVectorWriter() throws IOException
  {
    // Not yet implemented
    return null;
  }

  @Override
  public RecordReader<FeatureIdWritable, Geometry> getRecordReader() throws IOException
  {
    MbVectorTilesSettings results = parseResourceName(getResourceName(), conf, getProviderProperties());
    return new MbVectorTilesRecordReader(results);
  }

  @Override
  public RecordWriter<FeatureIdWritable, Geometry> getRecordWriter()
  {
    // Not yet implemented
    return null;
  }

  @Override
  public VectorInputFormatProvider getVectorInputFormatProvider(VectorInputFormatContext context) throws IOException
  {
    MbVectorTilesSettings results = parseResourceName(getResourceName(), conf, getProviderProperties());
    return new MbVectorTilesInputFormatProvider(context, this, results);
  }

  @Override
  public VectorOutputFormatProvider getVectorOutputFormatProvider(VectorOutputFormatContext context) throws IOException
  {
    // Not yet implemented
    return null;
  }

  @Override
  public void delete() throws IOException
  {
    // Not yet implemented
  }

  @Override
  public void move(String toResource) throws IOException
  {
    // Not yet implemented
  }

  MbVectorTilesSettings parseResourceName() throws IOException
  {
    return parseResourceName(getResourceName(), conf, getProviderProperties());
  }

  /**
   * Parses the input string into the url, username, password, query,
   * and geometry column name. Each of the settings is separated by
   * a semi-colon. Each of the settings themselves are formatted as
   * "name=value".
   *
   * @param input
   */
  private static MbVectorTilesSettings parseResourceName(String input,
                                                         Configuration conf,
                                                         ProviderProperties providerProperties) throws IOException
  {
    Map<String, String> settings = new HashMap<String, String>();
    parseDataSourceSettings(input, settings);
    String filename;
    if (settings.containsKey("filename")) {
      filename = HdfsVectorDataProvider.resolveNameToPath(conf,
              settings.get("filename"),
              providerProperties, false).toString();
    }
    else {
      throw new IOException("Missing expected filename setting");
    }

    Bounds bbox = null;
    if (settings.containsKey("bbox")) {
      String strBbox = settings.get("bbox");
      bbox = Bounds.fromCommaString(strBbox);
    }

    String[] layers = null;
    if (settings.containsKey("layers")) {
      String strLayers = settings.get("layers");
      layers = strLayers.split(",");
    }

    int zoom = -1;
    if (settings.containsKey("zoom")) {
      try {
        String strZoom = settings.get("zoom");
        zoom = Integer.parseInt(strZoom);
      }
      catch(NumberFormatException nfe) {
        throw new IOException("Invlid value specified for zoom: " + settings.get("zoom"));
      }
    }

    int recordsPerPartition = 10000;
    if (settings.containsKey("partition_size")) {
      try {
        String strPartitionSize = settings.get("partition_size");
        recordsPerPartition = Integer.parseInt(strPartitionSize);
      }
      catch(NumberFormatException nfe) {
        throw new IOException("Invlid value specified for zoom: " + settings.get("zoom"));
      }
    }

    return new MbVectorTilesSettings(filename, layers, zoom, recordsPerPartition, bbox);
  }
}
