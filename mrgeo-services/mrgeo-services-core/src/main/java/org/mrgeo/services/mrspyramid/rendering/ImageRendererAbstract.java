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

package org.mrgeo.services.mrspyramid.rendering;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconstConstants;
import org.gdal.osr.SpatialReference;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImagePyramidMetadataReader;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.image.MrsImage;
import org.mrgeo.image.MrsImageException;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.rasterops.ColorScale;
import org.mrgeo.resources.KmlGenerator;
import org.mrgeo.tile.TileNotFoundException;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.GDALUtils;
import org.mrgeo.utils.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Properties;

/**
 * Base class for WMS image response handlers; Each image format should subclass this.
 */
public abstract class ImageRendererAbstract implements ImageRenderer
{
private static final Logger log = LoggerFactory.getLogger(ImageRendererAbstract.class);

// currently, we only support WGS84, but this is here as a class member in
// case other coord sys's are ever supported
protected String srs;

// requested zoom level
protected int zoomLevel = -1;

// true if an empty image is being returned
protected boolean isTransparent = false;

private MrsImage image;

public ImageRendererAbstract()
{
  this.srs = GDALUtils.EPSG4326;
}

public ImageRendererAbstract(final SpatialReference srs)
{
  this.srs = srs.ExportToWkt();
}

public ImageRendererAbstract(final String srsWkt)
{
  this.srs = srsWkt;
}

/**
 * @return KML String
 * @throws IOException
 */
public static String asKml(final String pyrName, final Bounds bounds, final int width,
    final int height, final ColorScale cs, String requestUrl,
    final Properties providerProperties) throws IOException
{
  final URL url = new URL(requestUrl);
  if (!requestUrl.endsWith("/"))
  {
    requestUrl += "/";
  }
  requestUrl += "KmlGenerator";
  final String wmsHost = url.getHost() + ":" + url.getPort();
  return KmlGenerator
      .getKmlBodyAsString("kml", requestUrl, bounds, pyrName, wmsHost, null, MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT,
          providerProperties);
}

/**
 * Determines the appropriate image zoom level for an image request
 *
 * @param metadata source pyramid metadata
 * @param bounds   requested bounds
 * @param width    requested output image width
 * @param height   requested output image height
 * @return a zoom level
 * @throws Exception
 */
private static int getZoomLevel(final MrsImagePyramidMetadata metadata, final Bounds bounds,
    final int width, final int height) throws Exception
{
  log.debug("Determining zoom level for {}", metadata.getPyramid());
  final double pixelSizeLon = bounds.getWidth() / width;
  final double pixelSizeLat = bounds.getHeight() / height;
  final int tileSize = metadata.getTilesize();
  final int zoomY = TMSUtils.zoomForPixelSize(pixelSizeLat, tileSize);
  final int zoomX = TMSUtils.zoomForPixelSize(pixelSizeLon, tileSize);
  int zoomLevel = zoomX;
  if (zoomY > zoomX)
  {
    zoomLevel = zoomY;
  }
  log.debug("Originally calculated zoom level: {}", zoomLevel);
  // don't allow zooming past the highest res available image
  if (zoomLevel > metadata.getMaxZoomLevel())
  {
    zoomLevel = metadata.getMaxZoomLevel();
  }
  log.debug("final zoom level: {}", zoomLevel);
  return zoomLevel;
}

/**
 * Determines if a requested zoom level is valid for the requested data. This is a workaround to
 * support non-pyramid data requests against the MrsImagePyramid interface. *This is not a long
 * term solution.*
 *
 * @param metadata  source pyramid metadata
 * @param zoomLevel requested zoom level
 * @return true if the source data contains an image at the requested zoom level; false otherwise
 * @throws IOException
 * @todo poorly implemented method, I think...but enough to get by for now; rewrite so no
 * exception is thrown in case where image doesn't exist OR this more likely this won't be
 * needed at all once we rework MrsImagePyramid/MrsImage for non-pyramid data
 */
private static boolean isZoomLevelValid(final MrsImagePyramidMetadata metadata,
    final int zoomLevel) throws IOException
{
  return (metadata.getName(zoomLevel) != null);
}

@SuppressWarnings("unused")
private static void printImageInfo(final RenderedImage image, final String prefix)
{
  if (log.isDebugEnabled())
  {
    log.debug("{} width: {}", prefix, image.getWidth());
    log.debug("{} height: {} ", prefix, image.getHeight());
    log.debug("{} tile width: {}", prefix, image.getTileWidth());
    log.debug("{} tile height: {}", prefix, image.getTileHeight());
    log.debug("{} min tile x: {}", prefix, image.getMinTileX());
    log.debug("{} min num tiles x: {}", prefix, image.getNumXTiles());
    log.debug("{} min tile y: {}", prefix, image.getMinTileY());
    log.debug("{} min num tiles y: {}", prefix, image.getNumYTiles());
    log.debug("{} tile grid x offset: {}", prefix, image.getTileGridXOffset());
    log.debug("{} tile grid y offset: {}", prefix, image.getTileGridYOffset());
  }
}

private static MrsImage getImageForScale(final MrsImagePyramid pyramid, final double scale)
    throws JsonGenerationException, JsonMappingException, IOException
{
  log.debug("Retrieving image for for scale {} ...", scale);

  final int zoom = TMSUtils.zoomForPixelSize(scale, pyramid.getMetadata().getTilesize());

  final MrsImage image = pyramid.getImage(zoom);
  if (image == null)
  {
    throw new IllegalArgumentException("A valid scale couldn't be matched.");
  }

  log.debug("Image for scale {} retrieved", scale);
  return image;
}

/*
 * (non-Javadoc)
 *
 * @see org.mrgeo.services.wms.ImageRenderer#getDefaultValue()
 */
@Override
public double[] getDefaultValues()
{
  if (image != null)
  {
    return image.getMetadata().getDefaultValuesDouble();
  }
  return new double[]{-1.0};
}

/*
 * (non-Javadoc)
 *
 * @see org.mrgeo.services.wms.ImageRenderer#getExtrema()
 */
@Override
public double[] getExtrema()
{
  if (image != null)
  {
    if (zoomLevel == -1)
    {
      return image.getExtrema();
    }
    return image.getMetadata().getExtrema(zoomLevel);
  }
  return null;
}

/*
 * (non-Javadoc)
 *
 * @see org.mrgeo.services.wms.ImageRenderer#outputIsTransparent()
 */
@Override
public boolean outputIsTransparent()
{
  return isTransparent;
}

/**
 * Implements image rendering for GetMap requests
 *
 * @param pyramidName name of the source data
 * @param bounds      requested bounds
 * @param width       requested width
 * @param height      requested height
 * @return image rendering of the requested bounds at the requested size
 * @throws Exception
 */
@Override
public Raster renderImage(final String pyramidName, final Bounds bounds, final int width,
    final int height, final Properties providerProperties, final String epsg) throws Exception
{
  if (log.isDebugEnabled())
  {
    log.debug("requested bounds: {}", bounds.toString());
    log.debug("requested bounds width: {}", bounds.getWidth());
    log.debug("requested bounds height: {}", bounds.getHeight());
    log.debug("requested width: {}", width);
    log.debug("requested height: {}", height);
  }

  MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(pyramidName,
      AccessMode.READ, providerProperties);
  MrsImagePyramidMetadataReader r = dp.getMetadataReader();
  final MrsImagePyramidMetadata pyramidMetadata = r.read();
  isTransparent = false;

  // get the correct zoom level based on the requested bounds
  zoomLevel = getZoomLevel(pyramidMetadata, bounds, width, height);
  int tilesize = pyramidMetadata.getTilesize();

  // get the correct image in the pyramid based on the zoom level
  if (!pyramidMetadata.hasPyramids())
  {
    image = MrsImage.open(dp, pyramidMetadata.getMaxZoomLevel());
  }
  else
  {
    image = MrsImage.open(dp, zoomLevel);

    if (image == null)
    {
      image = MrsImage.open(dp, pyramidMetadata.getMaxZoomLevel());
    }
  }

  try
  {
    // return empty data when requested bounds is completely outside of the
    // image
    if (!bounds.intersects(pyramidMetadata.getBounds()))
    {
      log.debug("request bounds does not intersect image bounds");
      isTransparent = true;
      return RasterUtils.createEmptyRaster(width, height, pyramidMetadata.getBands(),
          pyramidMetadata.getTileType(), pyramidMetadata.getDefaultValue(0));
    }

    // there is no way to handle non-existing zoom levels above non-pyramid'd
    // data yet; no need to
    // check zoom level validity if we know there are pyramids present
    // TODO: get rid of this
    if (!pyramidMetadata.hasPyramids() && !isZoomLevelValid(pyramidMetadata, zoomLevel))
    {
      log.warn("Requested zoom level {} does not exist in source imagery.", zoomLevel);
      isTransparent = true;
      return RasterUtils.createEmptyRaster(width, height, pyramidMetadata.getBands(),
          pyramidMetadata.getTileType(), pyramidMetadata.getDefaultValue(0));
    }

    // merge together all tiles that fall within the requested bounds
    final Raster merged = image.getRaster(bounds);
    if (merged != null)
    {
      log.debug("merged image width: {}", merged.getWidth());
      log.debug("merged image height: {}", merged.getHeight());

      TMSUtils.Bounds requestedBounds = bounds.getTMSBounds();
      TMSUtils.TileBounds tb = TMSUtils.boundsToTile(requestedBounds, zoomLevel, tilesize);
      TMSUtils.Bounds actualBounds = TMSUtils.tileToBounds(tb, zoomLevel, tilesize);

      TMSUtils.Pixel requestedUL =
          TMSUtils.latLonToPixelsUL(requestedBounds.n, requestedBounds.w, zoomLevel, tilesize);
      TMSUtils.Pixel requestedLR =
          TMSUtils.latLonToPixelsUL(requestedBounds.s, requestedBounds.e, zoomLevel, tilesize);


      TMSUtils.Pixel actualUL =
          TMSUtils.latLonToPixelsUL(actualBounds.n, actualBounds.w, zoomLevel, tilesize);
//      TMSUtils.Pixel actualLR =
//          TMSUtils.latLonToPixelsUL(actualBounds.s, actualBounds.e, zoomLevel, tilesize);

      int offsetX = (int) (requestedUL.px - actualUL.px);
      int offsetY = (int) (requestedUL.py - actualUL.py);

      int croppedW = (int) (requestedLR.px - requestedUL.px);
      int croppedH = (int) (requestedLR.py - requestedUL.py);

      Raster cropped = merged.createChild(offsetX, offsetY, croppedW, croppedH, 0, 0, null);

      Dataset src = GDALUtils.toDataset(cropped, pyramidMetadata.getDefaultValue(0));
      Dataset dst = GDALUtils.createEmptyMemoryRaster(src, width, height);

      final double res = TMSUtils.resolution(zoomLevel, tilesize);

      final double[] srcxform = new double[6];

      // set the transform for the src
      srcxform[0] = requestedBounds.w; /* top left x */
      srcxform[1] = res; /* w-e pixel resolution */
      srcxform[2] = 0; /* 0 */
      srcxform[3] = requestedBounds.n; /* top left y */
      srcxform[4] = 0; /* 0 */
      srcxform[5] = -res; /* n-s pixel resolution (negative value) */

      src.SetGeoTransform(srcxform);

      // now change only the resolution for the dst
      final double[] dstxform = new double[6];

      dstxform[0] = requestedBounds.w; /* top left x */
      dstxform[1] = (requestedBounds.e - requestedBounds.w) / width;
      ; /* w-e pixel resolution */
      dstxform[2] = 0; /* 0 */
      dstxform[3] = requestedBounds.n; /* top left y */
      dstxform[4] = 0; /* 0 */
      dstxform[5] = (requestedBounds.s - requestedBounds.n) / height; /* n-s pixel resolution (negative value) */

      dst.SetGeoTransform(dstxform);


      int resample = gdalconstConstants.GRA_Bilinear;
      if (pyramidMetadata.getClassification() == MrsImagePyramidMetadata.Classification.Categorical)
      {
        // use gdalconstConstants.GRA_Mode for categorical, which may not exist in earlier versions of gdal,
        // in which case we will use GRA_NearestNeighbour
        try
        {
          Field mode = gdalconstConstants.class.getDeclaredField("GRA_Mode");
          if (mode != null)
          {
            resample = mode.getInt(gdalconstConstants.class);
          }
        }
        catch (Exception e)
        {
          resample = gdalconstConstants.GRA_NearestNeighbour;
        }
      }

      // default is WGS84
      String dstcrs = GDALUtils.EPSG4326;
      if (epsg != null && !epsg.equalsIgnoreCase("epsg:4326"))
      {
        SpatialReference crs = new SpatialReference();
        crs.SetWellKnownGeogCS(epsg);

        dstcrs = crs.ExportToWkt();
      }

      log.debug("Scaling image...");
      gdal.ReprojectImage(src, dst, GDALUtils.EPSG4326, dstcrs, resample);
      log.debug("Image scaled.");

      return GDALUtils.toRaster(dst);
    }

    log.error("Error processing request for image: {}", pyramidName);

    log.error("requested bounds: {}", bounds.toString());
    log.error("requested bounds width: {}", bounds.getWidth());
    log.error("requested bounds height: {}", bounds.getHeight());
    log.error("requested width: {}", width);
    log.error("requested height: {}", height);

    // isTransparent = true;
    // return ImageUtils.getTransparentImage(width, height);
    throw new IOException("Error processing request for image: " + pyramidName);
  }
  finally
  {
    if (image != null)
    {
      image.close();
    }
  }
}

/**
 * Implements image rendering for GetMosaic requests
 *
 * @param pyramidName name of the source data
 * @param bounds      requested bounds
 * @return image rendering of the requested bounds
 * @throws Exception
 */
@Override
public Raster renderImage(final String pyramidName, final Bounds bounds,
    final Properties providerProperties, final String epsg) throws Exception
{
  // assuming dimensions for tiles at all image levels are the same
  final MrsImagePyramid p = MrsImagePyramid.open(pyramidName, providerProperties);
  final int tileSize = p.getMetadata().getTilesize();
  return renderImage(pyramidName, bounds, tileSize, tileSize, providerProperties, epsg);
}

/**
 * Implements the image rendering for GetTile requests
 *
 * @param pyramidName name of the source data
 * @param tileColumn  x tile coordinate
 * @param tileRow     y tile coordinate
 * @param scale       requested image resolution
 * @return image rendering of the requested tile
 * @throws Exception
 */
@Override
public Raster renderImage(final String pyramidName, final int tileColumn, final int tileRow,
    final double scale, final Properties providerProperties) throws Exception
{

  try
  {
    MrsImagePyramid pyramid = MrsImagePyramid.open(pyramidName, providerProperties);
    image = getImageForScale(pyramid, scale);
    if (image == null)
    {
      throw new MrsImageException("Can't open image for scale: " + pyramidName + " scale: " + scale);
    }
    final Raster raster = image.getTile(tileColumn, tileRow);
    log.debug("Retrieving tile {}, {}", tileColumn, tileRow);
    return raster;
  }
  catch (final IOException e)
  {
    throw new IOException("Unable to open pyramid: " + HadoopFileUtils.unqualifyPath(pyramidName));
  }
  // don't need to close, the image cache will take care of it...
  // finally
  // {
  // if (image != null)
  // {
  // image.close();
  // }
  // }


}

@Override
public Raster renderImage(final String pyramidName, final int tileColumn, final int tileRow,
    final double scale, final String maskName, final double maskMax,
    Properties providerProperties) throws Exception
{
  MrsImagePyramid pyramid = null;
  MrsImagePyramidMetadata metadata = null;
  try
  {
    pyramid = MrsImagePyramid.open(pyramidName, providerProperties);
    image = getImageForScale(pyramid, scale);
    if (image == null)
    {
      throw new MrsImageException("Can't open image for scale: " + pyramidName + " scale: " + scale);
    }
    final Raster raster = image.getTile(tileColumn, tileRow);
    log.debug("Retrieving tile {}, {}", tileColumn, tileRow);

    metadata = pyramid.getMetadata();

    final double[] nodata = metadata.getDefaultValues();
    final WritableRaster writable = RasterUtils.makeRasterWritable(raster);

    MrsImagePyramid maskPyramid = MrsImagePyramid.open(maskName, providerProperties);
    MrsImage maskImage = getImageForScale(maskPyramid, scale);

    final Raster maskRaster = maskImage.getTile(tileColumn, tileRow);
    log.debug("Retrieving mask tile {}, {}", tileColumn, tileRow);

    final MrsImagePyramidMetadata maskMetadata = maskPyramid.getMetadata();
    final double[] maskNodata = maskMetadata.getDefaultValues();

    for (int w = 0; w < maskRaster.getWidth(); w++)
    {
      for (int h = 0; h < maskRaster.getHeight(); h++)
      {
        boolean masked = true;
        for (int b = 0; b < maskRaster.getNumBands(); b++)
        {
          final double maskPixel = maskRaster.getSampleDouble(w, h, b);
          if (maskPixel <= maskMax && Double.compare(maskPixel, maskNodata[b]) != 0)
          {
            masked = false;
            break;
          }
        }

        if (masked)
        {
          for (int b = 0; b < writable.getNumBands(); b++)
          {
            writable.setSample(w, h, b, nodata[b]);
          }
        }
      }
    }

    return writable;
  }
  catch (final TileNotFoundException e)
  {
    if (pyramid != null)
    {
      if (metadata == null)
      {
        metadata = pyramid.getMetadata();
      }

      return RasterUtils.createEmptyRaster(metadata.getTilesize(), metadata.getTilesize(),
          metadata.getBands(), metadata.getTileType(), metadata.getDefaultValue(0));
    }
    throw new IOException("Unable to open pyramid: " + HadoopFileUtils.unqualifyPath(pyramidName));
  }
  catch (final IOException e)
  {
    throw new IOException("Unable to open pyramid: " + HadoopFileUtils.unqualifyPath(pyramidName));
  }
  // don't need to close, the image cache will take care of it...
  // finally
  // {
  // if (image != null)
  // {
  // image.close();
  // }
  // }

}

@Override
public Raster renderImage(final String pyramidName, final int tileColumn, final int tileRow,
    final double scale, final String maskName, final Properties providerProperties) throws Exception
{
  return renderImage(pyramidName, tileColumn, tileRow, scale, maskName, Double.MAX_VALUE, providerProperties);
}

@Override
public Raster renderImage(final String pyramidName, final int tileColumn, final int tileRow,
    final int zoom, final Properties providerProperties) throws Exception
{
  try
  {
    MrsImagePyramid pyramid = MrsImagePyramid.open(pyramidName, providerProperties);

    image = pyramid.getImage(zoom);
    if (image == null)
    {
      throw new MrsImageException("Zoom level not found: " + pyramidName + " level: " + zoom);
    }
    final Raster raster = image.getTile(tileColumn, tileRow);
    log.debug("Retrieving tile {}, {}", tileColumn, tileRow);
    return raster;
  }
  catch (final IOException e)
  {
    throw new IOException("Unable to open pyramid: " + HadoopFileUtils.unqualifyPath(pyramidName));
  }
  // don't need to close, the image cache will take care of it...
  // finally
  // {
  // if (image != null)
  // {
  // image.close();
  // }
  // }
}

@Override
public Raster renderImage(final String pyramidName, final int tileColumn, final int tileRow,
    final int zoom, final String maskName, final double maskMax,
    final Properties providerProperties) throws Exception
{
  MrsImagePyramid pyramid = null;
  MrsImagePyramidMetadata metadata = null;
  try
  {
    pyramid = MrsImagePyramid.open(pyramidName, providerProperties);
    image = pyramid.getImage(zoom);
    if (image == null)
    {
      throw new MrsImageException("Zoom level not found: " + pyramidName + " level: " + zoom);
    }
    final Raster raster = image.getTile(tileColumn, tileRow);
    log.debug("Retrieving tile {}, {}", tileColumn, tileRow);

    metadata = pyramid.getMetadata();
    final double[] nodata = metadata.getDefaultValues();
    final WritableRaster writable = RasterUtils.makeRasterWritable(raster);

    MrsImagePyramid maskPyramid = MrsImagePyramid.open(maskName, providerProperties);
    MrsImage maskImage = maskPyramid.getImage(zoom);

    final Raster maskRaster = maskImage.getTile(tileColumn, tileRow);
    log.debug("Retrieving mask tile {}, {}", tileColumn, tileRow);

    final MrsImagePyramidMetadata maskMetadata = maskPyramid.getMetadata();
    final double[] maskNodata = maskMetadata.getDefaultValues();

    for (int w = 0; w < maskRaster.getWidth(); w++)
    {
      for (int h = 0; h < maskRaster.getHeight(); h++)
      {
        boolean masked = true;
        for (int b = 0; b < maskRaster.getNumBands(); b++)
        {
          final double maskPixel = maskRaster.getSampleDouble(w, h, b);
          if (maskPixel <= maskMax && Double.compare(maskPixel, maskNodata[b]) != 0)
          {
            masked = false;
            break;
          }
        }

        if (masked)
        {
          for (int b = 0; b < writable.getNumBands(); b++)
          {
            writable.setSample(w, h, b, nodata[b]);
          }
        }
      }
    }

    return writable;
  }
  catch (final TileNotFoundException e)
  {
//      throw e;
    if (pyramid != null)
    {
      if (metadata == null)
      {
        metadata = pyramid.getMetadata();
      }

      return RasterUtils.createEmptyRaster(metadata.getTilesize(), metadata.getTilesize(),
          metadata.getBands(), metadata.getTileType(), metadata.getDefaultValue(0));
    }
    throw new IOException("Unable to open pyramid: " + HadoopFileUtils.unqualifyPath(pyramidName));
  }
  catch (final IOException e)
  {
    throw new IOException("Unable to open pyramid: " + HadoopFileUtils.unqualifyPath(pyramidName));
  }
  // don't need to close, the image cache will take care of it...
  // finally
  // {
  // if (image != null)
  // {
  // image.close();
  // }
  // }

}

@Override
public Raster renderImage(final String pyramidName, final int tileColumn, final int tileRow,
    final int zoom, final String maskName,
    final Properties providerProperties) throws Exception
{
  return renderImage(pyramidName, tileColumn, tileRow, zoom, maskName, Double.MAX_VALUE,
      providerProperties);
}

/**
 * Calculates the envelope of all image tiles that touch the requested bounds
 *
 * @param bounds   requested bounds
 * @param zoom     calculated zoom level for the requested bounds
 * @param tilesize tile size of the source data
 * @return an envelope containing all source image tiles touching the requested bounds
 */
protected double[] getImageTileEnvelope(final Bounds bounds, final int zoom,
    final int tilesize)
{
  log.debug("Calculating image tile envelope...");


  final double res = TMSUtils.resolution(zoom, tilesize);

  final double[] xform = new double[6];

  xform[0] = bounds.getMinY(); /* top left x */
  xform[1] = res; /* w-e pixel resolution */
  xform[2] = 0; /* 0 */
  xform[3] = bounds.getMaxY(); /* top left y */
  xform[4] = 0; /* 0 */
  xform[5] = -res; /* n-s pixel resolution (negative value) */

  return xform;
}

}