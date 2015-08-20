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

package org.mrgeo.opimage;

import org.apache.commons.lang3.ArrayUtils;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.image.MrsImage;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.mapreduce.formats.TileClusterInfo;
import org.mrgeo.mapreduce.formats.TileCollection;
import org.mrgeo.rasterops.OpImageUtils;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.media.jai.ImageLayout;
import javax.media.jai.RasterFactory;
import javax.media.jai.SourcelessOpImage;
import java.awt.image.ColorModel;
import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Map;

@SuppressWarnings("unchecked")
public class MrsPyramidOpImage extends SourcelessOpImage implements Serializable
{
public static class MrsImageOpImageException extends RuntimeException
{
  private static final long serialVersionUID = 1L;
  private final Exception origException;

  public MrsImageOpImageException(final Exception e)
  {
    this.origException = e;
  }

  public MrsImageOpImageException(final String msg)
  {
    this.origException = new Exception(msg);
  }

  @Override
  public void printStackTrace()
  {
    origException.printStackTrace();
  }
}

private class CachedRaster
{
  final public long tx;
  final public long ty;

  final TileCollection<Raster>.Cluster rasterCluster;

  public CachedRaster(final long tx, final long ty, final TileCollection<Raster>.Cluster rasterCluster)
  {
    this.rasterCluster = rasterCluster;
    this.tx = tx;
    this.ty = ty;
  }

}

// TODO: This class is SOLELY here because the getProperties() method below sends back a
// hashtable, and one of the entries is a MrsImage. The calling method may or may not
// close the image. Therefore, we'll do it here.
//
// This should go away when the properties ValidRegion and GeographicTranslator either
// go away or are converted to strings, OR if MrsImageReader is separated from the
// MrsImage.
@SuppressWarnings("rawtypes")
private class ClosingHashTable extends Hashtable
{
  private static final long serialVersionUID = 1L;

  public ClosingHashTable()
  {
  }

  @Override
  protected void finalize() throws Throwable
  {
    for (final Object o : values())
    {
      if (o instanceof MrsImage)
      {
        System.out.println("Closing image from properties...: " +
            Integer.toHexString(System.identityHashCode(o)) + ": " + o.toString());
        ((MrsImage) o).close();
      }
    }
    super.finalize();
  }
}

private static final Logger log = LoggerFactory.getLogger(MrsPyramidOpImage.class);

private static final long serialVersionUID = 1L;

// The cachedRaster is used during map/reduce of single tiles. The record
// reader will
// load the tile from the sequencefile/acculumo table and set it here. This
// will
// significantly speed up operating on tiles.
private transient CachedRaster cachedRaster = null;
private transient TileClusterInfo tileClusterInfo;

private MrsImageDataProvider dp = null;
private int zoomlevel;
private final int tileSize;

// A "blank" raster, for when the tile request is outside the image bounds
private transient Raster emptyRaster = null;

public MrsPyramidOpImage(final MrsImageDataProvider dp, final int zoomlevel, final ImageLayout layout,
    final Map<?, ?> configuration, final SampleModel sampleModel, final int minX, final int minY,
    final int width, final int height, final int tileSize, final TileClusterInfo tileClusterInfo)
{
  super(layout, configuration, sampleModel, minX, minY, width, height);

  this.dp = dp;
  this.zoomlevel = zoomlevel;
  this.tileSize = tileSize;
  this.tileClusterInfo = tileClusterInfo;
}

public static MrsPyramidOpImage create(final MrsImageDataProvider dp, final Long level,
    final TileClusterInfo tileClusterInfo)
    throws IOException
{
  // since this is a SourcelessOpImage, we'll need to calculate the layout
  // ourselves...
  final MrsImagePyramidMetadata metadata = dp.getMetadataReader().read();

  final int tileSize = metadata.getTilesize();
  final int minX = (int) tileClusterInfo.getOffsetX() * tileSize;
  final int minY = (int) tileClusterInfo.getOffsetY() * tileSize;
  final int zoomlevel = level.intValue();
  final int width = tileClusterInfo.getWidth() * tileSize;
  final int height = tileClusterInfo.getHeight() * tileSize;

  MrsImagePyramid pyramid = MrsImagePyramid.open(dp);
  MrsImage image = pyramid.getImage(zoomlevel);
  Raster raster = image.getAnyTile();

  final SampleModel sampleModel = raster.getSampleModel();
  final ImageLayout layout = calculateLayout(dp, zoomlevel);
  return new MrsPyramidOpImage(dp, zoomlevel, layout, null, sampleModel, minX, minY,
      width, height, tileSize, tileClusterInfo);
}

public static MrsPyramidOpImage create(MrsImageDataProvider dp, final TileClusterInfo tileClusterInfo)
    throws IOException
{
  int zoomlevel = dp.getMetadataReader().read().getMaxZoomLevel();
  return create(dp, (long) zoomlevel, tileClusterInfo);
}

private static ImageLayout calculateLayout(final MrsImageDataProvider dp,
    final int zoomlevel) throws IOException
{
  final MrsImagePyramidMetadata metadata = dp.getMetadataReader().read();

  final LongRectangle pixelbounds = metadata.getPixelBounds(zoomlevel);

  MrsImagePyramid pyramid = MrsImagePyramid.open(dp);
  MrsImage image = pyramid.getImage(zoomlevel);
  Raster raster = image.getAnyTile();

  final ColorModel colorModel = RasterUtils.createColorModel(raster);

  return new ImageLayout(0, 0, (int) pixelbounds.getWidth(),
      (int) pixelbounds.getHeight(), 0, 0, metadata.getTilesize(), metadata.getTilesize(),
      raster.getSampleModel(), colorModel);
}

@Override
public Raster computeTile(final int tx, final int ty)
{
  log.debug("computing tile qtx: {} ty: {}", tx, ty);

  if (cachedRaster != null && cachedRaster.rasterCluster != null)
  {
    long mrsPyramidTileX = cachedRaster.tx + tx;
    // If the required tile is outside the world tile boundaries, then
    // wrap around to get the tile.
    if (mrsPyramidTileX < 0)
    {
      mrsPyramidTileX += TMSUtils.numXTiles(zoomlevel);
    }
    else
    {
      final long numXTiles = TMSUtils.numXTiles(zoomlevel);
      if (mrsPyramidTileX >= numXTiles)
      {
        mrsPyramidTileX -= numXTiles;
      }
    }
    final long mrsPyramidTileY = cachedRaster.ty + ty;
    if (TMSUtils.isValidTile(mrsPyramidTileX, mrsPyramidTileY, zoomlevel))
    {
      final long tileId = TMSUtils.tileid(mrsPyramidTileX, mrsPyramidTileY, zoomlevel);
      final Raster raster = cachedRaster.rasterCluster.get(new Long(tileId));
      if (raster != null)
      {
        return raster;
      }
    }
  }
  return blankRaster();
}

@Override
public int getHeight()
{
  return tileClusterInfo.getHeight() * tileSize;
}

@Override
public int getMaxTileX()
{
  return getMinTileX() + getNumXTiles() - 1;
}

@Override
public int getMaxTileY()
{
  return getMinTileY() + getNumYTiles() - 1;
}

@Override
public int getMaxX()
{
  return getMinX() + (tileClusterInfo.getWidth() * tileSize) - 1;
}

@Override
public int getMaxY()
{
  return getMinY() + (tileClusterInfo.getHeight() * tileSize) - 1;
}

@Override
public int getMinTileX()
{
  return (int) tileClusterInfo.getOffsetX();
}

@Override
public int getMinTileY()
{
  return (int) tileClusterInfo.getOffsetY();
}

@Override
public int getMinX()
{
  return (int) tileClusterInfo.getOffsetX() * tileSize;
}

@Override
public int getMinY()
{
  return (int) tileClusterInfo.getOffsetY() * tileSize;
}

@Override
public int getNumXTiles()
{
  return tileClusterInfo.getWidth();
}

@Override
public int getNumYTiles()
{
  return tileClusterInfo.getHeight();
}

@Override
public Object getProperty(final String name)
{
  if (name.equals(OpImageUtils.NODATA_PROPERTY))
  {
    // TODO: When band support is included, we need to remove the
    // hard-coding of band 0 below.
    try
    {
      return dp.getMetadataReader().read().getDefaultValue(0);
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    return Double.NaN;
  }

  return super.getProperty(name);
}

@Override
public String[] getPropertyNames()
{
  final String[] parentprops = super.getPropertyNames();

  final String[] props = {
      OpImageUtils.NODATA_PROPERTY };

  return ArrayUtils.addAll(parentprops, props);
}

public MrsImageDataProvider getDataProvider()
{
  return dp;
}

@Override
public Raster getTile(final int tileX, final int tileY)
{
  return super.getTile(tileX, -tileY);
}

@Override
public int getTileHeight()
{
  return tileSize;
}

@Override
public int getTileWidth()
{
  return tileSize;
}

@Override
public int getWidth()
{
  return tileClusterInfo.getWidth() * tileSize;
}

public int getZoomlevel()
{
  return zoomlevel;
}

public void setInputInfo(final long tx, final long ty,
    final TileCollection<Raster>.Cluster rasterCluster)
{
  cachedRaster = new CachedRaster(tx, ty, rasterCluster);
}

public void setInputInfo(final long tileid,
    final TileCollection<Raster>.Cluster rasterCluster)
{
  final TMSUtils.Tile t = TMSUtils.tileid(tileid, zoomlevel);
  setInputInfo(t.tx, t.ty, rasterCluster);
}

public void setInputRaster(long tileid, Raster raster)
{
  TileCollection<Raster> tc = new TileCollection<>();
  tc.set("temp-cluster", tileid, raster);

  setInputInfo(tileid, tc.getCluster("temp-cluster"));
}

public void setInputRaster(final long tx, final long ty, Raster raster)
{
  setInputRaster(TMSUtils.tileid(tx, ty, zoomlevel), raster);
}

public void setZoomlevel(final int zoom) throws IOException
{
  zoomlevel = zoom;
  cachedRaster = null;

  setImageLayout(calculateLayout(dp, zoomlevel));
}

@Override
public String toString()
{
  return getClass().getSimpleName() + " pyramid: " + dp.getResourceName();
}

@Override
@SuppressWarnings("rawtypes")
protected Hashtable getProperties()
{
  final ClosingHashTable result = new ClosingHashTable();

  final Hashtable s = super.getProperties();
  if (s != null)
  {
    result.putAll(s);
  }

  // TODO: When band support is included, we need to remove the
  // hard-coding of band 0 below.
  try
  {
    result.put(OpImageUtils.NODATA_PROPERTY,
        dp.getMetadataReader().read().getDefaultValue(0));
  }
  catch (IOException e)
  {
    result.put(OpImageUtils.NODATA_PROPERTY,
        Double.NaN);
  }
  return result;
}

private Raster blankRaster()
{
  if (emptyRaster == null)
  {
    try
    {
      final MrsImagePyramidMetadata metadata = dp.getMetadataReader().read();
      final double[] defaults = metadata.getDefaultValues();

      // build the cachedRaster
      final WritableRaster constRaster = RasterFactory.createWritableRaster(sampleModel,
          new java.awt.Point(0, 0));

      // flood fill
      final int w = sampleModel.getWidth();
      final int h = sampleModel.getHeight();
      final double[] data = new double[w * h];

      double def = defaults[0];

      Arrays.fill(data, def);
      for (int b = 0; b < metadata.getBands(); b++)
      {
        // this takes care of different default values per band (unlikely, but
        // possible)
        if (def != defaults[b])
        {
          def = defaults[b];
          Arrays.fill(data, def);
        }
        constRaster.setSamples(0, 0, w, h, 0, data);
      }

      emptyRaster = constRaster;
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
  }

  return emptyRaster;
}
}
