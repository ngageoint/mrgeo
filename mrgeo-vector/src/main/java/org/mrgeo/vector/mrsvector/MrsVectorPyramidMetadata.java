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

package org.mrgeo.vector.mrsvector;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.util.DefaultPrettyPrinter;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.pyramid.MrsPyramidMetadata;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

//import org.mrgeo.utils.Stats.Stats;

public class MrsVectorPyramidMetadata extends MrsPyramidMetadata
{
  public final static String METADATA = "metadata";

  private static final long serialVersionUID = 1L;
  private static final Logger log = LoggerFactory.getLogger(MrsVectorPyramidMetadata.class);

  /**
   * VectorMetadata stores metadata for each zoom level of
   * vector data. 
   */
  public static class VectorMetadata extends MrsPyramidMetadata.TileMetadata implements Serializable
  {
    private static final long serialVersionUID = 1L;

    // basic constructor
    public VectorMetadata()
    {
    }
  } // end VectorMetadata
  
  private VectorMetadata[] vectorData = null; // data specific to a single
  
//  private ImageStats[] stats;

  /**
   * Loading a metadata file from the local file system.  The objects of
   * the file are stored in a json format.  This enables the ObjectMapper
   * to parse out the values correctly.
   * 
   * @param file metadata file on the local file system to load
   * @return a valid MrsVectorPyramidMetadata object
   * @throws JsonGenerationException
   * @throws JsonMappingException
   * @throws IOException
   */
  public static MrsVectorPyramidMetadata load(final File file) throws JsonGenerationException,
  JsonMappingException, IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    final MrsVectorPyramidMetadata metadata = mapper.readValue(file, MrsVectorPyramidMetadata.class);

    // make sure the name of the pyramid is set correctly for where the
    // metadata file was pulled
    metadata.setPyramid("file://" + file.getParentFile().getAbsolutePath());

    return metadata;
  } // end load - File


  /**
   * Loading metadata from an InputStream.  The objects of
   * the file are stored in a json format.  This enables the ObjectMapper
   * to parse out the values correctly.
   * 
   * @param stream - the stream attached to the metadata input
   * @return a valid MrsVectorPyramidMetadata object
   * @throws JsonGenerationException
   * @throws JsonMappingException
   * @throws IOException
   */
  public static MrsVectorPyramidMetadata load(final InputStream stream) throws JsonGenerationException,
  JsonMappingException, IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    final MrsVectorPyramidMetadata metadata = mapper.readValue(stream, MrsVectorPyramidMetadata.class);
    return metadata;
  } // end load - InputStream
  

  /**
   * Loading metadata from HDFS.  The objects of
   * the file are stored in a json format.  This enables the ObjectMapper
   * to parse out the values correctly.
   * 
   * @param path - the location of the metadata file
   * @return a valid MrsVectorPyramidMetadata object
   * @throws JsonGenerationException
   * @throws JsonMappingException
   * @throws IOException
   */
  public static MrsVectorPyramidMetadata load(final Path path) throws JsonGenerationException,
  JsonMappingException, IOException
  {
    
    // attach to hdfs and create an input stream for the file
    FileSystem fs = HadoopFileUtils.getFileSystem(path);
    log.debug("Physically loading image metadata from " + path.toString());
    final InputStream is = HadoopFileUtils.open(path); // fs.open(path);
    try
    {
      // load the metadata from the input stream
      final MrsVectorPyramidMetadata metadata = load(is);

      // set the fully qualified path for the metadata file
      Path fullPath = path.makeQualified(fs);
      metadata.setPyramid(fullPath.getParent().toString());
      return metadata;
    }
    finally
    {
      is.close();
    }
  } // end load - Path

  
  /**
   * Loading metadata from a string.  This is more work then the above
   * methods.  This class will look at the incoming string and determine
   * which data source will be used.
   * 
   * Valid input is:
   *   Accumulo:
   *     accumulo:tableName/zoomlevel
   *     accumulo:tableName
   *   HDFS:
   *     hdfs://location/in/hdfs
   *     location/in/hdfs
   *   Local file system:
   *     file://location/in/local/file/system
   *     file:/location/in/local/file/system
   *     location/in/local/file/system
   *     
   * Note: Without the file: or hdfs: at the beginning, the code below
   * will look for a valid path in HDFS then look in the local file system.
   * Also, notice that the "datasource" property within the properties
   * object is ignored in this setup.
   * 
   * @param name - input string to distinguish location of requested metadata
   * @return a valid MrsVectorPyramidMetadata object
   * @throws JsonGenerationException
   * @throws JsonMappingException
   * @throws IOException
   */
  public static MrsVectorPyramidMetadata load(final String name) throws JsonGenerationException,
  JsonMappingException, IOException
  {
    
    // pull the properties from the environment
    final Properties p = MrGeoProperties.getInstance();

    log.debug("looking for metadata object for vector pyramid: " + name);
    
    // take care of accumulo right away
    if(name.startsWith("accumulo:")){

      // TODO: Implement Accumulo
      throw new IOException("Accumulo storage for vector data is not yet implemented");
//      return AccumuloBackend.load(name, p);
      
    }
    // take care of designated hdfs
    else if(name.startsWith("hdfs://"))
    {
      
      return HdfsBackend.load(name, p);

    }
    // take care of designated local file system
    else if(name.startsWith("file:"))
    {
      String tmp = name;
      if(tmp.startsWith("file://"))
      {
        tmp = tmp.replace("file://", "");
      }
      else
      {
        tmp = tmp.replace("file:", "");
      }

      // add the metadata portion
      if(!tmp.endsWith("/metadata"))
      {
        tmp += "/metadata";
      }
      
      // create a file handle
      final File file = new File(tmp);
      if (file.exists())
      {
        return load(file);
      }

    }

    // name is not in HDFS and need to check in local file system
    String tmp = name;

    // make sure it is metadata that is the file to open
    if(!tmp.endsWith("/metadata")){
      tmp += "/metadata";
    }

    
    // if we are here then hdfs and possibly the local file system need to be searched
    Path path = new Path(tmp);
    if(HadoopFileUtils.exists(path))
    {

      // jump out of this method
      return load(path);

    }
    
    // name is not in HDFS and need to check in local file system
    File f = new File(tmp);
    if(f.exists()){
      
      // jump out of this method
      return load(f);

    }

    // throw file not found
    // Note: this is expected behavior in some of the 
    throw new FileNotFoundException("Cannot open metadata file " + name);

  } // end load - string

  @Override
  public String getName(int zoomlevel)
  {
    if (vectorData != null && zoomlevel < vectorData.length && vectorData[zoomlevel].name != null)
    {
      return vectorData[zoomlevel].name;
    }
    return null;
  }

  // TODO: This method is HDFS specific and must be removed if we ever move MrsVector
  // access into the data access layer.
  public String getZoomName(int zoomlevel)
  {
    if (vectorData != null && zoomlevel < vectorData.length && vectorData[zoomlevel].name != null)
    {
      return pyramid + "/" + vectorData[zoomlevel].name;
    }
    return null;
  }

  public void setVectorMetadata(final VectorMetadata[] metadata)
  {
    vectorData = metadata;
  }

  public VectorMetadata[] getVectorMetadata()
  {
    return vectorData;
  }

  
  @Override
  public LongRectangle getTileBounds(int zoomlevel)
  {
    if (vectorData != null)
    {
      if (zoomlevel < vectorData.length)
      {
        return vectorData[zoomlevel].tileBounds;
      }

      // If we have _some_ tilebounds, calculate the bounds for the higher level
      return getOrCreateTileBounds(zoomlevel);
    }
    return null;
  }

  @Override
  public LongRectangle getOrCreateTileBounds(int zoomlevel)
  {
    if (vectorData != null && zoomlevel < vectorData.length)
    {
      return vectorData[zoomlevel].tileBounds;
    }

    LongRectangle tilebounds = getTileBounds(zoomlevel);
    if (tilebounds == null)
    {
      TMSUtils.Bounds b = new TMSUtils.Bounds(bounds.getMinX(), bounds.getMinY(),
          bounds.getMaxX(), bounds.getMaxY());

      TMSUtils.TileBounds tb = TMSUtils.boundsToTile(b, zoomlevel, tilesize);
      tilebounds = new LongRectangle(tb.w, tb.s, tb.e, tb.n);
    }
    return tilebounds;
  }

  @Override
  public void setName(int zoomlevel, String name)
  {
    if (vectorData == null || zoomlevel > maxZoomLevel)
    {
      setMaxZoomLevel(zoomlevel);
    }
    vectorData[zoomlevel].name = name;
  }

  @Override
  public void setTileBounds(int zoomlevel, LongRectangle tileBounds)
  {
    if (vectorData == null || zoomlevel > maxZoomLevel)
    {
      setMaxZoomLevel(zoomlevel);
    }
    vectorData[zoomlevel].tileBounds = tileBounds;
  }

  public void save() throws JsonGenerationException, JsonMappingException, IOException
  {
    save(true);
  }

  
  public void save(final boolean overwrite) throws JsonGenerationException, JsonMappingException,
  IOException
  {
    if(pyramid.startsWith("accumulo:")){
      throw new IOException("Accumulo storage of vector metadata is not yet implemented");
//      Properties p = MrGeoProperties.getInstance();
//      AccumuloBackend.save(pyramid, this, p);
//      return;
    }
    
    final Path path = new Path(getPyramid() + "/" + METADATA);
    final FileSystem fs = HadoopFileUtils.getFileSystem(path);
    if (fs.exists(path))
    {
      if (overwrite)
      {
        fs.delete(path, false);
      }
      else
      {
        throw new IOException("File already exists: " + path.toString());
      }
    }

    log.debug("Saving metadata to " + path.toString());
    final FSDataOutputStream os = HadoopFileUtils.getFileSystem(path).create(path);
    save(os);
    os.close();
  }

  public void save(final File file) throws JsonGenerationException, JsonMappingException,
  IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    try
    {
      mapper.writer(new DefaultPrettyPrinter()).writeValue(file, this);
    }
    catch (NoSuchMethodError e)
    {
      // if we don't have the pretty printer, just write the json
      mapper.writeValue(file, this);
    }

  }

  public void save(final OutputStream stream) throws JsonGenerationException, JsonMappingException,
  IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    try
    {
      mapper.writer(new DefaultPrettyPrinter()).writeValue(stream, this);
    }
    catch (NoSuchMethodError e)
    {
      // if we don't have the pretty printer, just write the json
      mapper.writeValue(stream, this);
    }
  }

  public void save(final Path path) throws JsonGenerationException, JsonMappingException,
  IOException
  {
    log.debug("Saving metadata to " + path.toString());
    final FSDataOutputStream os = HadoopFileUtils.getFileSystem(path).create(path);
    save(os);
    os.close();
  }
  
  public void setMaxZoomLevel(final int zoomlevel)
  {
    if (vectorData == null)
    {
      for (int i = 0; i <= zoomlevel; i++)
      {
        vectorData = (VectorMetadata[]) ArrayUtils.add(vectorData, new VectorMetadata());
      }
    }
    else if (zoomlevel < maxZoomLevel)
    {
      vectorData = (VectorMetadata[]) ArrayUtils.subarray(vectorData, 0, zoomlevel + 1);
    }
    else if (zoomlevel > maxZoomLevel)
    {
      for (int i = maxZoomLevel + 1; i <= zoomlevel; i++)
      {
        vectorData = (VectorMetadata[]) ArrayUtils.add(vectorData, new VectorMetadata());
      }
    }
    this.maxZoomLevel = zoomlevel;
  }

//  public void setStats(final ImageStats[] stats)
//  {
//    this.stats = stats;
//  }
//
//  public ImageStats[] getStats()
//  {
//    return stats;
//  }

  /**
   * Internal class to handle hdfs reading and writing of metadata object
   */
  static class HdfsBackend
  {
    
    /**
     * Pull metadata object out of HDFS
     * 
     * @param name - location of metadata
     * @param p - properties object
     * @return a valid MrsVectorPyramidMetadata object
     * @throws IOException - if there is a problem or the file cannot be found.
     */
    static MrsVectorPyramidMetadata load(String name, Properties p) throws IOException
    {
      MrsVectorPyramidMetadata metadata = null;
      // get the path for the metadata object
      final Path metapath = findMetadata(new Path(name));
        
      if(metapath == null){
        throw new IOException("Cannot find " + name);
      }

      // load the file from HDFS
      metadata = MrsVectorPyramidMetadata.load(metapath);

      return metadata;
    }  // end load
    
    
    /**
     * The will locate the metadata for the data pyramid
     * 
     * @param base is the base directory of the pyramid in HDFS.
     * @return Path to the metadata file in HDFS.
     * @throws IOException
     */
    static Path findMetadata(final Path base) throws IOException
    {
      final FileSystem fs = HadoopFileUtils.getFileSystem(base);
      Path meta = new Path(base, "metadata");

      // metadata file exists at this level
      if (fs.exists(meta))
      {
        return meta;
      }

      // try one level up (for a single map file case)
//      meta = new Path(base, "../metadata");
//      if (fs.exists(meta))
//      {
//        return meta;
//      }

      // try two levels up (for the multiple map file case)
//      meta = new Path(base, "../../metadata");
//      if (fs.exists(meta))
//      {
//        return meta;
//      }

      return null;
    } // end findMetadata
    
    static void save(String name, MrsVectorPyramidMetadata metadata, Properties p)
    {
      
    } // end save
    
    
    
  } // end HdfsBackend
}
