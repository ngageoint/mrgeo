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

package org.mrgeo.image;

import com.google.common.base.Optional;
import com.google.common.cache.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.KVIterator;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImageReader;
import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.tms.Bounds;
import org.mrgeo.utils.tms.Pixel;
import org.mrgeo.utils.tms.TMSUtils;
import org.mrgeo.utils.tms.TileBounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class MrsPyramid
{
static Logger log = LoggerFactory.getLogger(MrsPyramid.class);

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


final private MrsImageDataProvider provider;

private MrsPyramid(MrsImageDataProvider provider)
{
  super();

  this.provider = provider;

}

public Bounds getBounds()
{
  return getMetadataInternal().getBounds();
}

public LongRectangle getTileBounds(int zoomLevel)
{
  return getMetadataInternal().getTileBounds(zoomLevel);
}

public int getTileSize()
{
  return getMetadataInternal().getTilesize();
}

public int getMaximumLevel()
{
  return getMetadataInternal().getMaxZoomLevel();
}

public int getNumLevels()
{
  return getMetadataInternal().getMaxZoomLevel();
}

/**
 * Return true if there is data at each of the pyramid levels.
 *
 * @return
 */
public boolean hasPyramids()
{
  MrsPyramidMetadata metadata = getMetadataInternal();
  return metadata.hasPyramids();
}

@SuppressWarnings("squid:S1166") // Exception caught and handled
public static void calculateMetadata(final String pyramidname, final int zoom,
    final MrsImageDataProvider provider,
    final ImageStats[] levelStats,
    final double[] defaultValues,
    final Bounds bounds, final String protectionLevel) throws IOException
{
  MrsPyramidMetadata metadata;
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
    metadata = new MrsPyramidMetadata();
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


  final MrsImageReader reader = provider.getMrsTileReader(zoom);
  try
  {

    final KVIterator<TileIdWritable, MrGeoRaster> rasterIter = reader.get();

    if (rasterIter != null && rasterIter.hasNext())
    {
      final MrGeoRaster raster = rasterIter.next();

      calculateMetadata(zoom, raster, provider, levelStats, metadata);
    }
  }
  finally
  {
    reader.close();
  }
}

public static void calculateMetadata(final int zoom,
    final MrGeoRaster raster,
    final MrsImageDataProvider provider,
    final ImageStats[] levelStats,
    final MrsPyramidMetadata metadata) throws IOException
{

  final int tilesize = raster.width();

  final Bounds bounds = metadata.getBounds();

  final TileBounds tb = TMSUtils.boundsToTile(bounds, zoom, tilesize);
  metadata.setTileBounds(zoom, new LongRectangle(tb.w, tb.s, tb.e, tb.n));

  final Pixel pll = TMSUtils.latLonToPixels(bounds.s, bounds.w, zoom,
      tilesize);
  final Pixel pur = TMSUtils.latLonToPixels(bounds.n, bounds.e, zoom,
      tilesize);
  metadata.setPixelBounds(zoom, new LongRectangle(0, 0, pur.px - pll.px, pur.py - pll.py));

  metadata.setBands(raster.bands());
  metadata.setTilesize(tilesize);
  metadata.setTileType(raster.datatype());

  metadata.setName(zoom, Integer.toString(zoom));
  // update the pyramid level stats
  metadata.setImageStats(zoom, levelStats);

  // set the image level stats too which are the same as the max zoom level
  if (zoom == metadata.getMaxZoomLevel())
  {
    metadata.setStats(levelStats);
  }

  provider.getMetadataWriter().write(metadata);
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

@SuppressWarnings("squid:S1166") // Exception caught and handled
public static boolean isValid(final String name, final ProviderProperties providerProperties)
{
  try
  {
    MrsPyramid.open(name, providerProperties);
    return true;
  }
  catch (final IOException ignored)
  {
  }

  return false;
}

@Deprecated
public static MrsPyramid loadPyramid(final String name,
    final ProviderProperties providerProperties) throws IOException
{
  return MrsPyramid.open(name, providerProperties);
}

public static MrsPyramid open(final String name,
    final ProviderProperties providerProperties) throws IOException
{
  MrsImageDataProvider provider = DataProviderFactory.getMrsImageDataProvider(name,
      AccessMode.READ, providerProperties);
  return new MrsPyramid(provider);
}

public static MrsPyramid open(final String name,
    final Configuration conf) throws IOException
{
  MrsImageDataProvider provider = DataProviderFactory.getMrsImageDataProvider(name,
      AccessMode.READ, conf);
  return new MrsPyramid(provider);
}

public static MrsPyramid open(final MrsImageDataProvider provider) throws IOException
{
  return new MrsPyramid(provider);
}

public MrsPyramidMetadata.Classification getClassification() throws IOException
{
  return provider.getMetadataReader().read().getClassification();
}

/**
 * Be sure to also call MrsImage.close() on the returned MrsImage, or else there'll be a leak
 *
 */
public MrsImage getHighestResImage() throws IOException
{
  return getImage(getMaximumLevel());
}

/**
 * Be sure to also call MrsImage.close() on the returned MrsImage, or else there'll be a leak
 *
 */
@SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE", justification = "We _are_ checking!")
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

public MrsPyramidMetadata getMetadata() throws IOException
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

public String getName()
{
  return provider.getResourceName();
}

private MrsPyramidMetadata getMetadataInternal()
{
  try
  {
    return provider.getMetadataReader().read();
  }
  catch (IOException e)
  {
    log.error("Exception thrown {}", e);
  }

  return null;
}

}
