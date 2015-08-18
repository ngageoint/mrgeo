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

package org.mrgeo.image;

import com.google.common.base.Optional;
import com.google.common.cache.*;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.mrgeo.pyramid.MrsPyramid;
import org.mrgeo.pyramid.MrsPyramidMetadata;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.KVIterator;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImagePyramidMetadataWriter;
import org.mrgeo.data.tile.MrsTileReader;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.Raster;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class MrsImagePyramid extends MrsPyramid
{
static Logger log = LoggerFactory.getLogger(MrsImagePyramid.class);

private final static int IMAGE_CACHE_SIZE = 3;
private final static int IMAGE_CACHE_EXPIRE = 10; // minutes

final LoadingCache<Integer, Optional<MrsImage>> imageCache = CacheBuilder.newBuilder()
    .maximumSize(IMAGE_CACHE_SIZE)
    .expireAfterAccess(IMAGE_CACHE_EXPIRE, TimeUnit.SECONDS)
    .removalListener(
        new RemovalListener<Integer, Optional<MrsImage>>()
        {
          @Override
          public void onRemoval(final RemovalNotification<Integer, Optional<MrsImage>> notification)
          {
            if (notification.getValue().isPresent())
            {
              log.debug("image cache removal: " + provider.getResourceName() + "/" + notification.getKey());

              notification.getValue().get().close();
            }
          }})
    .build(new CacheLoader<Integer, Optional<MrsImage>>()
    {
      @Override
      public Optional<MrsImage> load(final Integer level) throws IOException
      {
        log.debug("image cache miss: " + provider.getResourceName() + "/" + level);

        return Optional.fromNullable(MrsImage.open(provider, level));
      }
    });


final protected MrsImageDataProvider provider;

protected MrsImagePyramid(MrsImageDataProvider provider)
{
  super();

  this.provider = provider;

}

public static void calculateMetadataWithProvider(final String pyramidname, final int zoom,
    final MrsImageDataProvider provider,
    final AdHocDataProvider statsProvider,
    final double[] defaultValues,
    final Bounds bounds, final Configuration conf,
    final String protectionLevel,
    final Properties providerProperties) throws IOException
{
  // update the pyramid level stats
  if (statsProvider != null)
  {
    ImageStats[] levelStats = null;
    levelStats = ImageStats.readStats(statsProvider);

    calculateMetadata(pyramidname, zoom, provider, levelStats, defaultValues,
        bounds, conf, protectionLevel, providerProperties);
  }
}

public static void calculateMetadata(final String pyramidname, final int zoom,
    final MrsImageDataProvider provider,
    final ImageStats[] levelStats,
    final double[] defaultValues,
    final Bounds bounds, final Configuration conf,
    final String protectionLevel,
    final Properties providerProperties) throws IOException
{

  MrsImagePyramidMetadata metadata;
  try
  {
    metadata = provider.getMetadataReader().read();
    if (metadata.getMaxZoomLevel() < zoom)
    {
      metadata.setMaxZoomLevel(zoom);
    }
  }
  catch (IOException e)
  {
    metadata = new MrsImagePyramidMetadata();
    metadata.setMaxZoomLevel(zoom);
  }

  metadata.setPyramid(pyramidname);
  metadata.setBounds(bounds);
  metadata.setName(zoom);
  metadata.setDefaultValues(defaultValues);

  if(protectionLevel != null && !protectionLevel.equals("null"))
  {
    metadata.setProtectionLevel(protectionLevel);
  }

  // HACK!!! (kinda...) Need to make metadata is there so the provider can get the
  //          MrsTileReader (it does a canOpen(), which makes sure metadata is present)
  MrsImagePyramidMetadataWriter metadataWriter = provider.getMetadataWriter();
  metadataWriter.write(metadata);

  final MrsTileReader<Raster> reader = provider.getMrsTileReader(zoom);
  try
  {

    final KVIterator<TileIdWritable, Raster> rasterIter = reader.get();

    if (rasterIter != null && rasterIter.hasNext())
    {
      final Raster raster = rasterIter.next();

      final int tilesize = raster.getWidth();

      final TMSUtils.Bounds b = new TMSUtils.Bounds(bounds.getMinX(), bounds.getMinY(),
          bounds.getMaxX(), bounds.getMaxY());

      final TMSUtils.TileBounds tb = TMSUtils.boundsToTile(b, zoom, tilesize);
      metadata.setTileBounds(zoom, new LongRectangle(tb.w, tb.s, tb.e, tb.n));

      final TMSUtils.Pixel pll = TMSUtils.latLonToPixels(bounds.getMinY(), bounds.getMinX(), zoom,
          tilesize);
      final TMSUtils.Pixel pur = TMSUtils.latLonToPixels(bounds.getMaxY(), bounds.getMaxX(), zoom,
          tilesize);
      metadata.setPixelBounds(zoom, new LongRectangle(0, 0, pur.px - pll.px, pur.py - pll.py));

      metadata.setBands(raster.getNumBands());
      metadata.setTilesize(tilesize);
      metadata.setTileType(raster.getTransferType());
      // update the pyramid level stats
      metadata.setImageStats(zoom, levelStats);

      // set the image level stats too which are the same as the max zoom level
      metadata.setStats(levelStats);
    }
    else
    {
      log.error("Error calculating MrsImagePyramid metadata, could not get a valid raster from the MrsImage");
    }

    provider.getMetadataWriter(null).write(metadata);
  }
  finally
  {
    reader.close();
  }
}

//  public static boolean delete(final String name)
//  {
//    try
//    {
//      pyramidCache.invalidate(name);
//      HadoopFileUtils.delete(name);
//
//      return true;
//    }
//    catch (final IOException e)
//    {
//    }
//
//    return false;
//  }

public static boolean isValid(final String name, final Properties providerProperties)
{
  try
  {
    MrsImagePyramid.open(name, providerProperties);
    return true;
  }
  catch (final JsonGenerationException e)
  {
  }
  catch (final JsonMappingException e)
  {
  }
  catch (final IOException e)
  {
  }

  return false;
}

@Deprecated
public static MrsImagePyramid loadPyramid(final String name,
    final Properties providerProperties) throws IOException
{
  return MrsImagePyramid.open(name, providerProperties);
}

public static MrsImagePyramid open(final String name,
    final Properties providerProperties) throws IOException
{
  MrsImageDataProvider provider = DataProviderFactory.getMrsImageDataProvider(name,
      AccessMode.READ, providerProperties);
  return new MrsImagePyramid(provider);
}

public static MrsImagePyramid open(final String name,
    final Configuration conf) throws IOException
{
  MrsImageDataProvider provider = DataProviderFactory.getMrsImageDataProvider(name,
      AccessMode.READ, conf);
  return new MrsImagePyramid(provider);
}

public static MrsImagePyramid open(final MrsImageDataProvider provider) throws IOException
{
  return new MrsImagePyramid(provider);
}

public MrsImagePyramidMetadata.Classification getClassification() throws IOException
{
  return provider.getMetadataReader().read().getClassification();
}

/**
 * Be sure to also call MrsImage.close() on the returned MrsImage, or else there'll be a leak
 *
 * @return
 * @throws IOException
 */
public MrsImage getHighestResImage() throws IOException
{
  return getImage(getMaximumLevel());
}

/**
 * Be sure to also call MrsImage.close() on the returned MrsImage, or else there'll be a leak
 *
 * @return
 * @throws IOException
 */
public MrsImage getImage(final int level) throws IOException
{
  try
  {
    Optional<MrsImage> o = imageCache.get(level);

    //      System.out.println("get image: " + metadata.getPyramid() + "/" + level +
    //        " (" + (o.isPresent() ? "found" : "null") + ")");
    return o.isPresent() ? o.get() : null;
  }
  catch (final ExecutionException e)
  {
    if (e.getCause() instanceof IOException)
    {
      throw (IOException)e.getCause();
    }
    throw new IOException(e);
  }
  //     return MrsImage.open(metadata, level);
}

public MrsImagePyramidMetadata getMetadata() throws IOException
{
  return provider.getMetadataReader().read();
}

@SuppressWarnings("static-method")
public String getScaleType()
{
  return null;
}

@SuppressWarnings("static-method")
public ImageStats getStats()
{
  return null;
}

@Override
public String getName()
{
  return provider.getResourceName();
}

@Override
protected MrsPyramidMetadata getMetadataInternal()
{
  try
  {
    return provider.getMetadataReader().read();
  }
  catch (IOException e)
  {
    e.printStackTrace();
  }

  return null;
}

//  public static void reloadMetadata(String pyramid) throws IOException
//  {
//    if (pyramid != null && !pyramid.isEmpty())
//    {
//      //    System.out.println("invalidating cache for : " + pyramid);
//      //    pyramidCache.invalidate(pyramid);
//
//      // just like the open, we need to unqualify the pyramid name to remove any hdfs://... stuff
//      Path unqualified = HadoopFileUtils.unqualifyPath(new Path(pyramid));
//      MrsImagePyramid py = pyramidCache.getIfPresent(unqualified.toString());
//
//      if (py != null)
//      {
//        MrsImagePyramidMetadata metadata = py.getMetadata();
//        metadata.reload();
//      }
//
//      log.debug("invalidating pyramid cache: " + unqualified.toString());
//    }
//  }



}
