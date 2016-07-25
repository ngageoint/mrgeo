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

package org.mrgeo.services.mrspyramid.rendering;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconstConstants;
import org.gdal.osr.SpatialReference;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.DataProviderNotFound;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsPyramidMetadataReader;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.data.tile.TileNotFoundException;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.image.MrsImage;
import org.mrgeo.image.MrsImageException;
import org.mrgeo.image.MrsPyramid;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.resources.KmlGenerator;
import org.mrgeo.services.utils.RequestUtils;
import org.mrgeo.utils.GDALJavaUtils;
import org.mrgeo.utils.GDALUtils;
import org.mrgeo.utils.tms.Bounds;
import org.mrgeo.utils.tms.Pixel;
import org.mrgeo.utils.tms.TMSUtils;
import org.mrgeo.utils.tms.TileBounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;

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

private String imageName = null;

public ImageRendererAbstract()
{
  this.srs = GDALUtils.EPSG4326();
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
public static String asKml(final String pyrName, final Bounds bounds, String requestUrl,
    final ProviderProperties providerProperties) throws IOException
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
private static int getZoomLevel(final MrsPyramidMetadata metadata, final Bounds bounds,
    final int width, final int height) throws Exception
{
  log.debug("Determining zoom level for {}", metadata.getPyramid());
  final double pixelSizeLon = bounds.width() / width;
  final double pixelSizeLat = bounds.height() / height;
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
 * support non-pyramid data requests against the MrsPyramid interface. *This is not a long
 * term solution.*
 *
 * @param metadata  source pyramid metadata
 * @param zoomLevel requested zoom level
 * @return true if the source data contains an image at the requested zoom level; false otherwise
 * @throws IOException
 */
private static boolean isZoomLevelValid(final MrsPyramidMetadata metadata,
    final int zoomLevel) throws IOException
{
  return (metadata.getName(zoomLevel) != null);
}

@SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD", justification = "kept for debugging")
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

private static MrsImage getImageForScale(final MrsPyramid pyramid, final double scale)
    throws IOException
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
  try
  {
    MrsImageDataProvider dp = getDataProvider();
    if (dp != null)
    {
      MrsPyramidMetadata metadata = dp.getMetadataReader().read();
      return metadata.getDefaultValues();
    }
  }
  catch (IOException e)
  {
    e.printStackTrace();
  }

  return new double[]{-1.0};
}


private MrsImageDataProvider getDataProvider()
{
  if (imageName != null)
  {
    try
    {
      return
          DataProviderFactory.getMrsImageDataProvider(imageName, AccessMode.READ, (ProviderProperties) null);


    }
    catch (DataProviderNotFound ignored)
    {
    }
  }

  return null;
}


/*
 * (non-Javadoc)
 *
 * @see org.mrgeo.services.wms.ImageRenderer#getExtrema()
 */
@SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "API")
@Override
public double[] getExtrema()
{
  try
  {
    MrsImageDataProvider dp = getDataProvider();
    if (dp != null)
    {
      MrsPyramidMetadata metadata = dp.getMetadataReader().read();
      if (zoomLevel == -1)
      {
        return metadata.getExtrema(0);
      }
      else
      {
        return metadata.getExtrema(zoomLevel);
      }
    }
  }
  catch (IOException e)
  {
    e.printStackTrace();
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
 * @param requestBounds      requested bounds
 * @param width       requested width
 * @param height      requested height
 * @return image rendering of the requested bounds at the requested size
 * @throws Exception
 */
@SuppressFBWarnings(value = "REC_CATCH_EXCEPTION", justification = "GDAL may have thow exception enabled")
@Override
public Raster renderImage(final String pyramidName, final Bounds requestBounds, final int width,
    final int height, final ProviderProperties providerProperties, final String epsg) throws Exception
{
  imageName = pyramidName;

  if (log.isDebugEnabled())
  {
    log.debug("requested bounds: {}", requestBounds.toString());
    log.debug("requested bounds width: {}", requestBounds.width());
    log.debug("requested bounds height: {}", requestBounds.height());
    log.debug("requested width: {}", width);
    log.debug("requested height: {}", height);
  }

  MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(pyramidName,
      AccessMode.READ, providerProperties);
  MrsPyramidMetadataReader r = dp.getMetadataReader();
  final MrsPyramidMetadata pyramidMetadata = r.read();
  isTransparent = false;

  Bounds wgs84Bounds = requestBounds;
  SpatialReference dstcrs = new SpatialReference();
  dstcrs.ImportFromEPSG(4326);
  if (epsg != null)
  {
    String[] code = epsg.split(":");
    int epsgCode = Integer.parseInt(code[1]);
    // We only need to transform the image if the destination
    // SRS is different from the source (4326)
    if (epsgCode != 4326)
    {
      // Reproject bounds to EPSG:4326 if necessary
      wgs84Bounds = RequestUtils.reprojectBoundsToWGS84(requestBounds, epsg);
      dstcrs.ImportFromEPSG(epsgCode);
    }
  }

  // get the correct zoom level based on the requested bounds
  zoomLevel = getZoomLevel(pyramidMetadata, wgs84Bounds, width, height);
  int tilesize = pyramidMetadata.getTilesize();

  // return empty data when requested bounds is completely outside of the
  // image
  if (!wgs84Bounds.intersects(pyramidMetadata.getBounds()))
  {
    log.debug("request bounds does not intersect image bounds");
    log.debug("requested bounds in wgs84: " + wgs84Bounds.toString());
    log.debug("image bounds: " + pyramidMetadata.getBounds().toString());
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

  MrsImage image;
  // get the correct image in the pyramid based on the zoom level
  if (!pyramidMetadata.hasPyramids())
  {
    log.warn("Getting image at max zoom " + pyramidMetadata.getMaxZoomLevel());
    image = MrsImage.open(dp, pyramidMetadata.getMaxZoomLevel());
  }
  else
  {
    log.debug("Getting image at zoom " + zoomLevel);
    image = MrsImage.open(dp, zoomLevel);

    if (image == null)
    {
      log.warn("Could not get image at expected zoom, getting image at max zoom " + pyramidMetadata.getMaxZoomLevel());
      image = MrsImage.open(dp, pyramidMetadata.getMaxZoomLevel());
    }
  }

  if (image == null)
  {
    log.error("Image " + pyramidName + "does not exist");
    throw new IOException("Image " + pyramidName + "does not exist");
  }
  else
  {
    try
    {

      // merge together all tiles that fall within the requested bounds
      final Raster merged = image.getRaster(wgs84Bounds);
      if (merged != null)
      {
        log.debug("merged image width: {}", merged.getWidth());
        log.debug("merged image height: {}", merged.getHeight());

        TileBounds tb = TMSUtils.boundsToTile(wgs84Bounds, zoomLevel, tilesize);
        Bounds actualBounds = TMSUtils.tileToBounds(tb, zoomLevel, tilesize);

        Pixel requestedUL =
            TMSUtils.latLonToPixelsUL(wgs84Bounds.n, wgs84Bounds.w, zoomLevel, tilesize);
        log.debug("Requested UL pixel: " + requestedUL.toString());
        Pixel requestedLR =
            TMSUtils.latLonToPixelsUL(wgs84Bounds.s, wgs84Bounds.e, zoomLevel, tilesize);
        log.debug("Requested LR pixel: " + requestedLR.toString());


        Pixel actualUL =
            TMSUtils.latLonToPixelsUL(actualBounds.n, actualBounds.w, zoomLevel, tilesize);
        log.debug("Actual UL pixel: " + actualUL.toString());
        Pixel actualLR =
            TMSUtils.latLonToPixelsUL(actualBounds.s, actualBounds.e, zoomLevel, tilesize);
        log.debug("Actual LR pixel: " + actualLR.toString());

        int offsetX = (int) (requestedUL.px - actualUL.px);
        int offsetY = (int) (requestedUL.py - actualUL.py);
        log.debug("Requested offset = " + offsetX + ", " + offsetY);

        int croppedW = (int) (requestedLR.px - requestedUL.px) + 1;
        if (offsetX + croppedW > merged.getWidth()) {
          croppedW = merged.getWidth() - offsetX;
        }
        int croppedH = (int) (requestedLR.py - requestedUL.py) + 1;
        if (offsetY + croppedH > merged.getHeight()) {
          croppedH = merged.getHeight() - offsetY;
        }
        Raster cropped = merged.createChild(offsetX, offsetY, croppedW, croppedH, 0, 0, null);
        log.debug("cropped image width: {}", cropped.getWidth());
        log.debug("cropped image height: {}", cropped.getHeight());

        Dataset src = GDALUtils.toDataset(cropped, pyramidMetadata.getDefaultValue(0), null);
        Dataset dst = GDALUtils.createEmptyMemoryRaster(src, width, height);

        SpatialReference srccrs = new SpatialReference();
        srccrs.ImportFromEPSG(4326);
        log.debug("WGS84 bounds: {}", wgs84Bounds.toString());
        log.debug("WGS84 requested bounds width: {}", wgs84Bounds.width());
        log.debug("WGS84 requested bounds height: {}", wgs84Bounds.height());

        final double res = TMSUtils.resolution(zoomLevel, tilesize);
        final double[] srcxform = new double[6];

        log.debug("res = " + res);
        // set the transform for the src
        srcxform[0] = wgs84Bounds.w; /* top left x */
        srcxform[1] = res; /* w-e pixel resolution */
        srcxform[2] = 0; /* 0 */
        srcxform[3] = wgs84Bounds.n; /* top left y */
        srcxform[4] = 0; /* 0 */
        srcxform[5] = -res; /* n-s pixel resolution (negative value) */

        src.SetGeoTransform(srcxform);
        src.SetProjection(srccrs.ExportToWkt());

        // now change only the resolution for the dst
        final double[] dstxform = new double[6];

        log.debug("dst horizontal res = " + ((requestBounds.e - requestBounds.w) / width));
        log.debug("dst vertical res = " + ((requestBounds.s - requestBounds.n) / height));
        dstxform[0] = requestBounds.w; /* top left x */
        dstxform[1] = (requestBounds.e - requestBounds.w) / width; /* w-e pixel resolution */
        dstxform[2] = 0; /* 0 */
        dstxform[3] = requestBounds.n; /* top left y */
        dstxform[4] = 0; /* 0 */
        dstxform[5] = (requestBounds.s - requestBounds.n) / height; /* n-s pixel resolution (negative value) */

        dst.SetGeoTransform(dstxform);
        dst.SetProjection(dstcrs.ExportToWkt());

        int resample = gdalconstConstants.GRA_Bilinear;
        if (pyramidMetadata.getClassification() == MrsPyramidMetadata.Classification.Categorical)
        {
          // use gdalconstConstants.GRA_Mode for categorical, which may not exist in earlier versions of gdal,
          // in which case we will use GRA_NearestNeighbour
          try
          {
            Field mode = gdalconstConstants.class.getDeclaredField("GRA_Mode");
            resample = mode.getInt(gdalconstConstants.class);
          }
          catch (Exception e)
          {
            resample = gdalconstConstants.GRA_NearestNeighbour;
          }
        }

        gdal.ReprojectImage(src, dst, srccrs.ExportToWkt(), dstcrs.ExportToWkt(), resample);
        log.debug("Returning a reprojected image");
        return GDALUtils.toRaster(dst);
      }

      log.error("Error processing request for image: {}", pyramidName);

      // isTransparent = true;
      // return ImageUtils.getTransparentImage(width, height);
      throw new IOException("Error processing request for image: " + pyramidName);
    }
    finally
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
    final ProviderProperties providerProperties, final String epsg) throws Exception
{
  imageName = pyramidName;

  // assuming dimensions for tiles at all image levels are the same
  final MrsPyramid p = MrsPyramid.open(pyramidName, providerProperties);
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
    final double scale, final ProviderProperties providerProperties) throws Exception
{
  imageName = pyramidName;

  try
  {
    MrsPyramid pyramid = MrsPyramid.open(pyramidName, providerProperties);
    MrsImage image = getImageForScale(pyramid, scale);
    final Raster raster = image.getTile(tileColumn, tileRow);
    log.debug("Retrieving tile {}, {}", tileColumn, tileRow);

    image.close();

    return raster;
  }
  catch (final IOException e)
  {
    throw new IOException("Unable to open pyramid: " + HadoopFileUtils.unqualifyPath(pyramidName));
  }
}

@Override
public Raster renderImage(final String pyramidName, final int tileColumn, final int tileRow,
    final double scale, final String maskName, final double maskMax,
    ProviderProperties providerProperties) throws Exception
{
  imageName = pyramidName;

  MrsPyramid pyramid = null;
  MrsPyramidMetadata metadata = null;
  try
  {
    pyramid = MrsPyramid.open(pyramidName, providerProperties);
    MrsImage image = getImageForScale(pyramid, scale);
    final Raster raster = image.getTile(tileColumn, tileRow);
    log.debug("Retrieving tile {}, {}", tileColumn, tileRow);

    metadata = pyramid.getMetadata();

    final double[] nodata = metadata.getDefaultValues();
    final WritableRaster writable = RasterUtils.makeRasterWritable(raster);

    MrsPyramid maskPyramid = MrsPyramid.open(maskName, providerProperties);
    MrsImage maskImage = getImageForScale(maskPyramid, scale);

    final Raster maskRaster = maskImage.getTile(tileColumn, tileRow);
    log.debug("Retrieving mask tile {}, {}", tileColumn, tileRow);

    final MrsPyramidMetadata maskMetadata = maskPyramid.getMetadata();
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
    image.close();

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
    final double scale, final String maskName, final ProviderProperties providerProperties) throws Exception
{
  return renderImage(pyramidName, tileColumn, tileRow, scale, maskName, Double.MAX_VALUE, providerProperties);
}

@Override
public Raster renderImage(final String pyramidName, final int tileColumn, final int tileRow,
    final int zoom, final ProviderProperties providerProperties) throws Exception
{
  imageName = pyramidName;

  try
  {
    MrsPyramid pyramid = MrsPyramid.open(pyramidName, providerProperties);

    MrsImage image = pyramid.getImage(zoom);
    if (image == null)
    {
      throw new MrsImageException("Zoom level not found: " + pyramidName + " level: " + zoom);
    }
    final Raster raster = image.getTile(tileColumn, tileRow);
    log.debug("Retrieving tile {}, {}", tileColumn, tileRow);

    image.close();

    return raster;
  }
  catch (final IOException e)
  {
    throw new IOException("Unable to open pyramid: " + HadoopFileUtils.unqualifyPath(pyramidName));
  }
}

@Override
public Raster renderImage(final String pyramidName, final int tileColumn, final int tileRow,
    final int zoom, final String maskName, final double maskMax,
    final ProviderProperties providerProperties) throws Exception
{
  imageName = pyramidName;

  MrsPyramid pyramid = null;
  MrsPyramidMetadata metadata = null;
  try
  {
    pyramid = MrsPyramid.open(pyramidName, providerProperties);
    MrsImage image = pyramid.getImage(zoom);
    if (image == null)
    {
      throw new MrsImageException("Zoom level not found: " + pyramidName + " level: " + zoom);
    }
    final Raster raster = image.getTile(tileColumn, tileRow);
    log.debug("Retrieving tile {}, {}", tileColumn, tileRow);

    metadata = pyramid.getMetadata();
    final double[] nodata = metadata.getDefaultValues();
    final WritableRaster writable = RasterUtils.makeRasterWritable(raster);

    MrsPyramid maskPyramid = MrsPyramid.open(maskName, providerProperties);
    MrsImage maskImage = maskPyramid.getImage(zoom);

    final Raster maskRaster = maskImage.getTile(tileColumn, tileRow);
    log.debug("Retrieving mask tile {}, {}", tileColumn, tileRow);

    final MrsPyramidMetadata maskMetadata = maskPyramid.getMetadata();
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

    image.close();

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
    final ProviderProperties providerProperties) throws Exception
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

  xform[0] = bounds.w; /* top left x */
  xform[1] = res; /* w-e pixel resolution */
  xform[2] = 0; /* 0 */
  xform[3] = bounds.n; /* top left y */
  xform[4] = 0; /* 0 */
  xform[5] = -res; /* n-s pixel resolution (negative value) */

  return xform;
}

}