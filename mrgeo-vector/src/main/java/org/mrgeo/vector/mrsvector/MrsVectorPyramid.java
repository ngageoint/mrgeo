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

package org.mrgeo.vector.mrsvector;

import org.apache.commons.collections.map.LRUMap;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.mrgeo.pyramid.MrsPyramid;
import org.mrgeo.pyramid.MrsPyramidMetadata;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.TMSUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class MrsVectorPyramid extends MrsPyramid
{
  private static class MrsVectorPyramidCache
  {
    static MrsVectorPyramidCache instance = new MrsVectorPyramidCache();

    private final int CACHE_SIZE = 50;

    @SuppressWarnings("unchecked")
    private final Map<String, MrsVectorPyramid> cache = Collections.synchronizedMap(new LRUMap(CACHE_SIZE));

    public MrsVectorPyramidCache()
    {
    }

    @SuppressWarnings("unused")
    public static void clear()
    {
      instance.cache.clear();
    }

    public static MrsVectorPyramid get(final String name)
    {
      return instance.cache.get(name);
    }

    @SuppressWarnings("unused")
    public static boolean isInCache(final String name)
    {
      return (instance.cache.containsKey(name));
    }

    public static void put(final String name, final MrsVectorPyramid pyramid)
    {
      instance.cache.put(name, pyramid);
    }

    public static void remove(final String name)
    {
      instance.cache.remove(name);
    }
  }

  final private MrsVectorPyramidMetadata metadata;

  protected MrsVectorPyramid(final String name) throws IOException
  {
    super();
    metadata = MrsVectorPyramidMetadata.load(name);
  }

  public static boolean delete(final String name)
  {
    try
    {
      MrsVectorPyramidCache.remove(name);
      HadoopFileUtils.delete(name);

      return true;
    }
    catch (final IOException e)
    {
    }

    return false;
  }

  public static MrsVectorPyramid loadPyramid(final String name) throws IOException
  {
    return MrsVectorPyramid.open(name);
  }

  public static MrsVectorPyramid open(final String name) throws IOException
  {
    try
    {
      MrsVectorPyramid pyramid = MrsVectorPyramidCache.get(name);
      if (pyramid == null)
      {
        pyramid = new MrsVectorPyramid(name);
        // Add the pyramid to the cache with the name passed in (which
        // is usually not fully qualified) as well as its fully qualified
        // name. This is a patch for the more general problem of attempting
        // the access the same pyramid from the cache with these different
        // names and getting different pyramid objects. This was a problem
        // while building pyramids because the pyramid levels were only
        // being updated in one of the pyramid objects, and the other one
        // was also being accessed, but the levels stored in its metadata
        // were old.
        MrsVectorPyramidCache.put(name, pyramid);
        MrsVectorPyramidCache.put(pyramid.getName(), pyramid);
      }

      return pyramid;
    }
    catch(JsonGenerationException e)
    {
      throw new IOException(e);
    }
    catch(JsonMappingException e)
    {
      throw new IOException(e);
    }
  }

  public static boolean isValid(final String name)
  {
    try
    {
      MrsVectorPyramid.open(name);
      return true;
    }
    catch (JsonGenerationException e)
    {
    }
    catch (JsonMappingException e)
    {
    }
    catch (IOException e)
    {
    }

    return false;
  }

  /**
   * Be sure to also call MrsVector.close() on the returned MrsVector, 
   * or else there'll be a leak
   * 
   * @return
   * @throws IOException
   */
  public MrsVector getHighestResVector() throws IOException
  {
    return getVector(getMaximumLevel());
  }

  /**
   * Be sure to also call MrsVector.close() on the returned MrsVector, 
   * or else there'll be a leak
   * 
   * @return
   * @throws IOException
   */
  public MrsVector getVector(final int level) throws IOException
  {
    return MrsVector.open(metadata, level);
  }

  public MrsVectorPyramidMetadata getMetadata()
  {
    return metadata;
  }

  protected MrsPyramidMetadata getMetadataInternal()
  {
    return metadata;
  }

  public static void calculateMetadata(String pyramidname, int zoom, int tileSize,
      Bounds bounds, String protectionLevel)
      throws JsonGenerationException, JsonMappingException, IOException
  {
    MrsVectorPyramidMetadata metadata = new MrsVectorPyramidMetadata();

    metadata.setPyramid(pyramidname);
    metadata.setMaxZoomLevel(zoom);
    metadata.setBounds(bounds);
    metadata.setName(zoom);
    metadata.setTilesize(tileSize);
    metadata.setProtectionLevel(protectionLevel);

    TMSUtils.Bounds b = new TMSUtils.Bounds(bounds.getMinX(), bounds.getMinY(),
        bounds.getMaxX(), bounds.getMaxY());

    TMSUtils.TileBounds tb = TMSUtils.boundsToTileExact(b, zoom, tileSize);
    metadata.setTileBounds(zoom, new LongRectangle(tb.w, tb.s, tb.e, tb.n));

    final Path path = new Path(metadata.getPyramid(), MrsVectorPyramidMetadata.METADATA);
    metadata.save(path);
  }
}
