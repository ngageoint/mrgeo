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

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.mrgeo.tile.TileNotFoundException;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.KVIterator;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImagePyramidReaderContext;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.data.tile.MrsTileReader;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.geom.Rectangle2D;
import java.awt.image.*;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

/**
 * @author tim.tisler
 * 
 */
public class MrsImage
{

  private static final Logger log = LoggerFactory.getLogger(MrsImage.class);

  // create a simple color model
  protected ColorModel colorModel = null;
  protected SampleModel sampleModel = null;
  protected String measurement = "Ratio"; // not sure what this is for...
  private MrsTileReader<Raster> reader = null; // The MrsImageReader for fetching the tiles
  private MrsImagePyramidMetadata metadata = null; // image metadata

  private int zoomlevel = -1; // current zoom level of this image
  private int tilesize = -1; // size of a tile (here for convenience, it is in the metadata)
  private MrsImageDataProvider provider;
  private MrsImagePyramidReaderContext context;

  private MrsImage(MrsImageDataProvider provider, final int zoomlevel)
  {
    context = new MrsImagePyramidReaderContext();
    context.setZoomlevel(zoomlevel);
    this.provider = provider;

    openReader();

    if (System.getProperty("mrgeo.profile", "false").compareToIgnoreCase("true") == 0)
    {
      LeakChecker.instance().add(
        this,
        ExceptionUtils.getFullStackTrace(new Throwable(
            "MrsImage creation stack(ignore the Throwable...)")));
    }
  }

  public static MrsImage open(MrsImageDataProvider provider, final int zoomlevel) throws IOException
  {
    try
    {
      MrsImagePyramidMetadata meta = provider.getMetadataReader(null).read();
      // Check to see if there is an image at this zoom level before opening
      final String name = meta.getName(zoomlevel);
      if (name != null)
      {
        final MrsImage image = new MrsImage(provider, zoomlevel);
        return image;
      }
    }
    // TODO this seems weird to catch and eat these here...
    catch (final MrsImageException e)
    {
      // e.printStackTrace();
    }
    catch (final NullPointerException e)
    {
      // e.printStackTrace();
    }

    return null;
  }


  public static MrsImage open(final String name, final int zoomlevel,
      final Properties providerProperties) throws IOException
  {
    if (name == null)
    {
      throw new IOException("Unable to open image. Resource name is empty.");
    }
    MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(name,
        AccessMode.READ, providerProperties);
    return open(dp, zoomlevel);
  }

  /**
   * Releases any resources held by this image, in particular its reader. Be sure to call this after
   * done using an MrsImage, and if necessary, in a finally block of a try/catch/finally
   */
  public void close()
  {
    if (reader != null)
    {
      LeakChecker.instance().remove(this);

      reader.close();
      reader = null;
    }
  }

  public LatLng convertToLatLng(final double px, final double py)
  {
    return new LatLng(convertToWorldY(py), convertToWorldX(px));
  }

  public LatLng convertToPixelCenterLatLng(final double px, final double py)
  {
    return new LatLng(convertToWorldY(py + 0.5), convertToWorldX(px + 0.5));
  }

  public final double convertToWorldX(final double px)
  {
    final double resolution = TMSUtils.resolution(getZoomlevel(), getTilesize());
    final Bounds bounds = getMetadata().getBounds();

    return bounds.getMinX() + (px * resolution);
  }

  public final double convertToWorldY(final double py)
  {
    final double resolution = TMSUtils.resolution(getZoomlevel(), getTilesize());
    final Bounds bounds = getMetadata().getBounds();

    return bounds.getMinY() + ((getPixelMaxY() - py) * resolution);
  }

  public Raster getAnyTile()
  {
    if (reader == null)
    {
      openReader();
    }
    final Iterator<Raster> it = reader.get();
    return it.next();
  }

  /**
   * @return
   */
  public Bounds getBounds()
  {
    return getMetadata().getBounds();
  }

  public Bounds getBounds(final int tx, final int ty)
  {
    final double bounds[] = TMSUtils.tileSWNEBoundsArray(tx, ty, getZoomlevel(), getTilesize());

    return new Bounds(bounds[0], bounds[1], bounds[2], bounds[3]);
  }


  /**
   * Retrieves the overall minimum and maximum raster values in the source data
   * 
   * @return array with the overall minimum value in first position and overall maximum value in the
   *         second
   */
  public double[] getExtrema()
  {
    final double[] extrema = new double[3];
    final ImageStats stats = getMetadata().getImageStats(getZoomlevel(), 0);
    if (stats != null)
    {
      extrema[1] = stats.max;
      extrema[0] = Math.max(0.0, stats.min);
    }
    else
    {
      extrema[1] = 1.0;
      extrema[0] = 0.0;
    }
    return extrema;
  }

  public long getHeight()
  {
    return getMetadata().getPixelBounds(getZoomlevel()).getHeight();
  }

  public Rectangle2D getImageBounds()
  {
    return new Rectangle2D.Double(getPixelMinX(), getPixelMinY(), getWidth(), getHeight());
  }

  public long getMaxTileX()
  {
    return getMetadata().getTileBounds(getZoomlevel()).getMaxX();
  }

  public long getMaxTileY()
  {
    return getMetadata().getTileBounds(getZoomlevel()).getMaxY();
  }

  public int getMaxZoomlevel()
  {
    return getMetadata().getMaxZoomLevel();
  }

  public MrsImagePyramidMetadata getMetadata()
  {
    if (metadata == null)
    {
      try
      {
        metadata = provider.getMetadataReader().read();
      }
      catch (IOException e)
      {
        log.error("Unable to read metadata for " + provider.getResourceName(), e);
      }
    }
    return metadata;
  }

  public long getMinTileX()
  {
    return getMetadata().getTileBounds(getZoomlevel()).getMinX();
  }

  public long getMinTileY()
  {
    return getMetadata().getTileBounds(getZoomlevel()).getMinY();
  }

  public String getName()
  {
    return metadata.getName(zoomlevel);
  }

  public long getNumXTiles()
  {
    return getMetadata().getTileBounds(getZoomlevel()).getWidth();
  }

  public long getNumYTiles()
  {
    return getMetadata().getTileBounds(getZoomlevel()).getHeight();
  }

  public long getPixelMaxX()
  {
    return getMetadata().getPixelBounds(getZoomlevel()).getMaxX();
  }

  public long getPixelMaxY()
  {
    return getMetadata().getPixelBounds(getZoomlevel()).getMaxY();
  }

  public long getPixelMinX()
  {
    return getMetadata().getPixelBounds(getZoomlevel()).getMinX();
  }

  public long getPixelMinY()
  {
    return getMetadata().getPixelBounds(getZoomlevel()).getMinY();
  }

  public LongRectangle getPixelRect()
  {
    return new LongRectangle(getPixelMinX(), getPixelMinY(), getWidth(), getHeight());
  }

  /**
   * Returns a rendered image for the MrsImage data
   * 
   * @return rendered image
   * @throws IOException
   */
  public RenderedImage getRenderedImage() throws IOException
  {
    return getRenderedImage(null);
  }

  /**
   * Returns a rendered image for the MrsImage data
   * 
   * @param bounds
   *          requested bounds
   * @return rendered image
   * @throws IOException
   */
  public RenderedImage getRenderedImage(final Bounds bounds)
  {
    Raster mergedRaster = getRaster(bounds);
    BufferedImage mergedImage = null;

    if (mergedRaster != null)
    {
      log.debug("Rendering merged image...");
      mergedImage = RasterUtils.makeBufferedImage(mergedRaster);
      log.debug("Merged image rendered.");
    }

    return mergedImage;
  }

/**
 * Returns a raster for the MrsImage data
 *
 * @return raster
 * @throws IOException
 */
public Raster getRaster() throws IOException
{
  return getRaster(null);
}

/**
 * Returns a raster image for the MrsImage data
 *
 * @param bounds
 *          requested bounds
 * @return raster
 */
public Raster getRaster(final Bounds bounds)
{
  log.debug("Merging to raster...");
  Raster mergedRaster = null;
  try
  {
    log.debug("Merging tiles...");
    if (bounds != null)
    {
      log.debug("with bounds: " + bounds.toString());
      mergedRaster = RasterTileMerger.mergeTiles(this, bounds);
    }
    else
    {
      mergedRaster = RasterTileMerger.mergeTiles(this);
    }
    log.debug("Tiles merged.");
  }
  catch (final MrsImageException e)
  {
    log.error(e.getMessage());
  }

  return mergedRaster;
}


  public Raster getTile(final long tx, final long ty) throws TileNotFoundException
  {
    if (tx < getMinTileX() || tx > getMaxTileX() || ty < getMinTileY() || ty > getMaxTileY())
    {
      // throw new IllegalArgumentException("x/y out of range. (" + String.valueOf(tx) + ", " +
      // String.valueOf(ty) + ") range: " + String.valueOf(getMinTileX()) + ", " +
      // String.valueOf(getMinTileY()) + " to " + String.valueOf(getMaxTileX()) + ", " +
      // String.valueOf(getMaxTileY()) + " (inclusive)");

      final String msg = String.format(
        "Tile x/y out of range. (%d, %d) range: (%d, %d) to (%d, %d) (inclusive)", tx, ty,
        getMinTileX(), getMinTileY(), getMaxTileX(), getMaxTileY());
      throw new TileNotFoundException(msg);
    }
    if (reader == null)
    {
      openReader();
    }
    return reader.get(new TileIdWritable(TMSUtils.tileid(tx, ty, getZoomlevel())));
  }

  public LongRectangle getTileBounds()
  {
    return getMetadata().getTileBounds(getZoomlevel());
  }

  public int getTileHeight()
  {
    return getTilesize();
  }

  public KVIterator<TileIdWritable, Raster> getTiles(final LongRectangle tileBounds)
  {
    if (reader == null)
    {
      openReader();
    }
    return reader.get(tileBounds);
  }

  public KVIterator<TileIdWritable, Raster> getTiles(final TileIdWritable start,
    final TileIdWritable end)
  {
    if (reader == null)
    {
      openReader();
    }
    return reader.get(start, end);
  }

  public int getTilesize()
  {
    if (tilesize < 0)
    {
      tilesize = getMetadata().getTilesize();
    }
    return tilesize;
  }

  public int getTileType()
  {
    return getMetadata().getTileType();
  }

  public int getTileWidth()
  {
    return getTilesize();
  }

  public long getWidth()
  {
    return getMetadata().getPixelBounds(getZoomlevel()).getWidth();
  }

  public int getZoomlevel()
  {
    if (zoomlevel < 0)
    {
      if (reader == null)
      {
        openReader();
      }
      zoomlevel = reader.getZoomlevel();
    }
    return zoomlevel;
  }

  public boolean isTileEmpty(final long tx, final long ty)
  {
    if (reader == null)
    {
      openReader();
    }
    return !reader.exists(new TileIdWritable(TMSUtils.tileid(tx, ty, getZoomlevel())));
  }

  public void setScaleType(final String measurement)
  {
    // this.measurement = measurement;
    // final FileSystem fs = HadoopUtils.getFileSystem(toc);
    // final FSDataOutputStream dos = fs.create(toc);
    // writeXml(dos);
    // dos.close();
    throw new NotImplementedException("MrsImage2.setScaleType() Not Implemented");

  }

  // @SuppressWarnings({ "unchecked", "rawtypes" })
  // protected Hashtable getProperties()
  // {
  // final Hashtable result = new Hashtable();
  // result.put(ValidRegion.PROPERTY_STRING, this);
  // result.put(GeographicTranslator.PROPERTY_STRING, this);
  // result.put(GeoTiffOpImage.NULL_PROPERTY, Double.NaN);
  // result.put(MEASUREMENT_STRING, measurement);
  // return result;
  // }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + ": " + getMetadata().getPyramid() + ":" + getZoomlevel();
  }

  // log an error if finalize() is called and close() was not called
  @Override
  protected void finalize() throws Throwable
  {
    try
    {
      if (reader != null)
      {
        log.info("MrsImage.finalize(): looks like close() was not called. This is a potential " +
            "memory leak. Please call close(). The MrsImage points to \"" + metadata.getPyramid() +
            "\" with zoom level " + getZoomlevel());
      }
    }
    finally
    {
      super.finalize();
    }
  }

  private void openReader()
  {
    try
    {
      reader = provider.getMrsTileReader(context);
      if (reader == null)
      {
        throw new MrsImageException("Error Reading Image");
      }
    }
    catch (IOException e)
    {
      throw new MrsImageException(e);
    }
  }
}