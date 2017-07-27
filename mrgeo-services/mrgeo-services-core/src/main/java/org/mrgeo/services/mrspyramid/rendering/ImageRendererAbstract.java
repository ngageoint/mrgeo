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

package org.mrgeo.services.mrspyramid.rendering;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconstConstants;
import org.gdal.osr.CoordinateTransformation;
import org.gdal.osr.SpatialReference;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.data.*;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsPyramidMetadataReader;
import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.data.tile.TileNotFoundException;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.image.*;
import org.mrgeo.resources.KmlGenerator;
import org.mrgeo.services.utils.RequestUtils;
import org.mrgeo.utils.GDALUtils;
import org.mrgeo.utils.tms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  srs = GDALUtils.EPSG4326();
}

public ImageRendererAbstract(SpatialReference srs)
{
  this.srs = srs.ExportToWkt();
}

public ImageRendererAbstract(String srsWkt)
{
  srs = srsWkt;
}

/**
 * @return KML String
 */
public static String asKml(String pyrName, Bounds bounds, String requestUrl,
    ProviderProperties providerProperties) throws IOException
{
  URL url = new URL(requestUrl);
  if (!requestUrl.endsWith("/"))
  {
    requestUrl += "/";
  }
  requestUrl += "KmlGenerator";
  String wmsHost = url.getHost() + ":" + url.getPort();
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
 */
private static int getZoomLevel(MrsPyramidMetadata metadata, Bounds bounds,
    int width, int height)
{
  log.debug("Determining zoom level for {}", metadata.getPyramid());
  double pixelSizeLon = bounds.width() / width;
  double pixelSizeLat = bounds.height() / height;
  int tileSize = metadata.getTilesize();
  int zoomY = TMSUtils.zoomForPixelSize(pixelSizeLat, tileSize);
  int zoomX = TMSUtils.zoomForPixelSize(pixelSizeLon, tileSize);
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
 */
private static boolean isZoomLevelValid(MrsPyramidMetadata metadata,
    int zoomLevel)
{
  return (metadata.getName(zoomLevel) != null);
}


private static MrsImage getImageForScale(MrsPyramid pyramid, double scale)
    throws IOException
{
  log.debug("Retrieving image for for scale {} ...", scale);

  int zoom = TMSUtils.zoomForPixelSize(scale, pyramid.getMetadata().getTilesize());

  MrsImage image = pyramid.getImage(zoom);
  if (image == null)
  {
    throw new IllegalArgumentException("A valid scale couldn't be matched.");
  }

  log.debug("Image for scale {} retrieved", scale);
  return image;
}

//private static int parseEpsgCode(String epsg)
//{
//  String prefix = "epsg:";
//  int index = epsg.toLowerCase().indexOf(prefix);
//  if (index >= 0)
//  {
//    try
//    {
//      return Integer.parseInt(epsg.substring(index + prefix.length()));
//    }
//    catch (NumberFormatException ignored)
//    {
//    }
//  }
//  throw new IllegalArgumentException("Invalid EPSG code: " + epsg);
//}

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
    log.error("Exception thrown", e);
  }

  return new double[]{-1.0};
}

@Override
public double[][] getQuantiles()
{
  try
  {
    MrsImageDataProvider dp = getDataProvider();
    if (dp != null)
    {
      MrsPyramidMetadata metadata = dp.getMetadataReader().read();
      return metadata.getQuantiles();
    }
  }
  catch (IOException e)
  {
    log.error("Exception thrown", e);
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
      ImageStats stats = metadata.getStats(0);
      if (stats != null)
      {
        return new double[]{stats.min, stats.max};
      }
    }
  }
  catch (IOException e)
  {
    log.error("Exception thrown", e);
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
 * @param pyramidName   name of the source data
 * @param requestBounds requested bounds (in dest projection coordinates)
 * @param width         requested width
 * @param height        requested height
 * @return image rendering of the requested bounds at the requested size
 */
@SuppressWarnings("squid:S1166") // Exception caught and handled
@SuppressFBWarnings(value = "REC_CATCH_EXCEPTION", justification = "GDAL may have throw exception enabled")
@Override
public MrGeoRaster renderImage(String pyramidName, Bounds requestBounds, int width,
    int height, ProviderProperties providerProperties, String epsg) throws ImageRendererException
{
  try
  {
    imageName = pyramidName;

    if (log.isDebugEnabled())
    {
      log.debug("requested bounds: {}", requestBounds);
      log.debug("requested bounds width: {}", requestBounds.width());
      log.debug("requested bounds height: {}", requestBounds.height());
      log.debug("requested width: {}", width);
      log.debug("requested height: {}", height);
    }

    MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(pyramidName,
        AccessMode.READ, providerProperties);
    MrsPyramidMetadataReader r = dp.getMetadataReader();
    MrsPyramidMetadata pyramidMetadata = r.read();
    isTransparent = false;

    Bounds wgs84Bounds = requestBounds;

    // We need to transform the image if the destination SRS is different from the source (4326)
    if (epsg != null && !epsg.equalsIgnoreCase("epsg:4326"))
    {
      wgs84Bounds = RequestUtils.reprojectBoundsToWGS84(requestBounds, epsg);
    }

    // get the correct zoom level based on the requested bounds
    zoomLevel = getZoomLevel(pyramidMetadata, wgs84Bounds, width, height);

    // return empty data when requested bounds is completely outside of the
    // image
    if (!wgs84Bounds.intersects(pyramidMetadata.getBounds()))
    {
      log.debug("request bounds does not intersect image bounds");
      log.debug("requested bounds in wgs84: " + wgs84Bounds);
      log.debug("image bounds: " + pyramidMetadata.getBounds());
      isTransparent = true;
      return MrGeoRaster.createEmptyRaster(width, height, pyramidMetadata.getBands(),
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
      return MrGeoRaster.createEmptyRaster(width, height, pyramidMetadata.getBands(),
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
        log.warn(
            "Could not get image at expected zoom, getting image at max zoom " + pyramidMetadata.getMaxZoomLevel());
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
      Dataset src = null;
      Dataset dst = null;
      try {
        src = mosaicToDataset(image, zoomLevel, wgs84Bounds, pyramidMetadata.getDefaultValues());
        dst = GDALUtils.createEmptyDiskBasedRaster(src, width, height);

        log.debug("WGS84 bounds: {}", wgs84Bounds);
        log.debug("WGS84 requested bounds width: {}", wgs84Bounds.width());
        log.debug("WGS84 requested bounds height: {}", wgs84Bounds.height());

        // now change only the resolution for the dst
        double[] dstxform = new double[6];

        // default is WGS84
        String dstcrs = GDALUtils.EPSG4326();
        if (epsg != null && !epsg.equalsIgnoreCase("epsg:4326")) {
          SpatialReference scrs = new SpatialReference();
          scrs.ImportFromEPSG(4326);

          SpatialReference dcrs = new SpatialReference();
          dcrs.SetFromUserInput(epsg);

          dstcrs = dcrs.ExportToWkt();

          CoordinateTransformation ct = new CoordinateTransformation(scrs, dcrs);

          double[] ll = ct.TransformPoint(wgs84Bounds.w, wgs84Bounds.s);
          double[] tr = ct.TransformPoint(wgs84Bounds.e, wgs84Bounds.n);

          dstxform[0] = ll[0]; /* top left x */
          dstxform[1] = (tr[0] - ll[0]) / width; /* w-e pixel resolution */
          dstxform[2] = 0; /* 0 */
          dstxform[3] = tr[1]; /* top left y */
          dstxform[4] = 0; /* 0 */
          dstxform[5] = (ll[1] - tr[1]) / height; /* n-s pixel resolution (negative value) */
        } else {
          dstxform[0] = wgs84Bounds.w; /* top left x */
          dstxform[1] = (wgs84Bounds.e - wgs84Bounds.w) / width; /* w-e pixel resolution */
          dstxform[2] = 0; /* 0 */
          dstxform[3] = wgs84Bounds.n; /* top left y */
          dstxform[4] = 0; /* 0 */
          dstxform[5] = (wgs84Bounds.s - wgs84Bounds.n) / height; /* n-s pixel resolution (negative value) */
        }

        dst.SetGeoTransform(dstxform);
        dst.SetProjection(dstcrs);

        int resample = gdalconstConstants.GRA_Bilinear;
        if (pyramidMetadata.getClassification() == MrsPyramidMetadata.Classification.Categorical) {
          // use gdalconstConstants.GRA_Mode for categorical, which may not exist in earlier versions of gdal,
          // in which case we will use GRA_NearestNeighbour
          try {
            Field mode = gdalconstConstants.class.getDeclaredField("GRA_Mode");
            resample = mode.getInt(gdalconstConstants.class);
          } catch (Exception e) {
            resample = gdalconstConstants.GRA_NearestNeighbour;
          }
        }

        log.debug("Scaling image...");
        gdal.ReprojectImage(src, dst, GDALUtils.EPSG4326(), dstcrs, resample);
        log.debug("Image scaled.");

        return MrGeoRaster.fromDataset(dst);
      }
      finally {
        if (src != null) {
          GDALUtils.delete(src, true);
        }
        if (dst != null) {
          GDALUtils.delete(dst, true);
        }
        image.close();
      }
    }
  }
  catch (Exception e)
  {
    log.error("Error rendering image to request bounds", e);
    throw new ImageRendererException(e);
  }
}

Dataset mosaicToDataset(MrsImage image, int zoomlevel,
                        Bounds requestedBounds, double[] nodatas) throws IOException
{
  MrsPyramidMetadata metadata = image.getMetadata(); // make sure metadata is loaded

  int tilesize = metadata.getTilesize();
  TileBounds tileBounds = TMSUtils.boundsToTile(requestedBounds, zoomlevel, tilesize);

  Bounds actualBounds = TMSUtils.tileToBounds(tileBounds, zoomlevel, tilesize);
  Pixel actualUL =
          TMSUtils.latLonToPixelsUL(actualBounds.n, actualBounds.w, zoomLevel, tilesize);
  log.debug("Actual UL pixel: " + actualUL);
  Pixel actualLR =
          TMSUtils.latLonToPixelsUL(actualBounds.s, actualBounds.e, zoomLevel, tilesize);
  log.debug("Actual LR pixel: " + actualLR);
  Pixel requestedUL =
          TMSUtils.latLonToPixelsUL(requestedBounds.n, requestedBounds.w, zoomLevel, tilesize);
  log.debug("Requested UL pixel: " + requestedUL);
  Pixel requestedLR =
          TMSUtils.latLonToPixelsUL(requestedBounds.s, requestedBounds.e, zoomLevel, tilesize);
  log.debug("Requested LR pixel: " + requestedLR);
  int dsWidth = (int)(requestedLR.px - requestedUL.px);
  int dsHeight = (int)(requestedLR.py - requestedUL.py);
  int gdaltype = GDALUtils.toGDALDataType(metadata.getTileType());
  Dataset ds = GDALUtils.createEmptyDiskBasedRaster(dsWidth, dsHeight, metadata.getBands(),
          gdaltype, nodatas);
  double[] xform = new double[6];
  xform[0] = requestedBounds.w;
  xform[1] = requestedBounds.width() / dsWidth;
  xform[2] = 0;
  xform[3] = requestedBounds.n;
  xform[4] = 0;
  xform[5] = -requestedBounds.height() / dsHeight;

  ds.SetProjection(GDALUtils.EPSG4326());
  ds.SetGeoTransform(xform);
  for (int b = 0; b < ds.GetRasterCount(); b++) {
    Band band = ds.GetRasterBand(b + 1); // gdal bands are 1's based
    if (nodatas != null) {
      if (b < nodatas.length) {
        band.SetNoDataValue(nodatas[b]);
      } else {
        band.SetNoDataValue(nodatas[nodatas.length - 1]);
      }
    }
  }
  // the iterator is _much_ faster than requesting individual tiles...
  // final KVIterator<TileIdWritable, Raster> iter = image.getTiles(TileBounds
  // .convertToLongRectangle(tileBounds));
  for (long row = tileBounds.s; row <= tileBounds.n; row++) {
    TileIdWritable rowStart = new TileIdWritable(TMSUtils.tileid(tileBounds.w, row, zoomlevel));
    TileIdWritable rowEnd = new TileIdWritable(TMSUtils.tileid(tileBounds.e, row, zoomlevel));

    KVIterator<TileIdWritable, MrGeoRaster> iter = image.getTiles(rowStart, rowEnd);
    while (iter.hasNext()) {
      MrGeoRaster source = iter.currentValue();
      if (source != null) {
        Tile tile = TMSUtils.tileid(iter.currentKey().get(), zoomlevel);
        Bounds b = TMSUtils.tileBounds(tile.tx, tile.ty, zoomlevel, tilesize);
        source.copyToDataset(ds, dsWidth, dsHeight, requestedBounds, b, tilesize, zoomlevel, gdaltype);
      }
    }
    if (iter instanceof CloseableKVIterator) {
      try {
        ((CloseableKVIterator) iter).close();
      } catch (IOException e) {
        log.error("Exception thrown", e);
      }
    }
  }
  return ds;
}

/**
 * Implements image rendering for GetMosaic requests
 *
 * @param pyramidName name of the source data
 * @param bounds      requested bounds
 * @return image rendering of the requested bounds
 */
@Override
public MrGeoRaster renderImage(String pyramidName, Bounds bounds,
    ProviderProperties providerProperties, String epsg) throws ImageRendererException
{
  imageName = pyramidName;

  try
  {
    // assuming dimensions for tiles at all image levels are the same
    MrsPyramid p = MrsPyramid.open(pyramidName, providerProperties);
    int tileSize = p.getMetadata().getTilesize();
    return renderImage(pyramidName, bounds, tileSize, tileSize, providerProperties, epsg);
  }
  catch (ImageRendererException | IOException e)
  {
    throw new ImageRendererException(e);
  }
}

/**
 * Implements the image rendering for GetTile requests
 *
 * @param pyramidName name of the source data
 * @param tileColumn  x tile coordinate
 * @param tileRow     y tile coordinate
 * @param scale       requested image resolution
 * @return image rendering of the requested tile
 */
@Override
public MrGeoRaster renderImage(String pyramidName, int tileColumn, int tileRow,
    double scale, ProviderProperties providerProperties) throws ImageRendererException
{
  imageName = pyramidName;

  try
  {
    MrsPyramid pyramid = MrsPyramid.open(pyramidName, providerProperties);
    try (MrsImage image = getImageForScale(pyramid, scale))
    {
      log.debug("Retrieving tile {}, {}", tileColumn, tileRow);
      return image.getTile(tileColumn, tileRow);
    }
  }
  catch (IOException e)
  {
    throw new ImageRendererException("Unable to open pyramid: " + HadoopFileUtils.unqualifyPath(pyramidName), e);
  }
}

@Override
@SuppressWarnings("squid:S2583")  // false positive, "if (metadata==null)" will _not_ always evaluate true
public MrGeoRaster renderImage(String pyramidName, int tileColumn, int tileRow,
    double scale, String maskName, double maskMax,
    ProviderProperties providerProperties) throws ImageRendererException
{
  imageName = pyramidName;

  MrsPyramid pyramid = null;
  MrsPyramidMetadata metadata = null;
  try
  {
    pyramid = MrsPyramid.open(pyramidName, providerProperties);
    try (MrsImage image = getImageForScale(pyramid, scale))
    {
      MrGeoRaster raster = image.getTile(tileColumn, tileRow);
      log.debug("Retrieving tile {}, {}", tileColumn, tileRow);

      metadata = pyramid.getMetadata();

      double[] nodata = metadata.getDefaultValues();

      MrsPyramid maskPyramid = MrsPyramid.open(maskName, providerProperties);
      MrsImage maskImage = getImageForScale(maskPyramid, scale);

      MrGeoRaster maskRaster = maskImage.getTile(tileColumn, tileRow);
      log.debug("Retrieving mask tile {}, {}", tileColumn, tileRow);

      MrsPyramidMetadata maskMetadata = maskPyramid.getMetadata();
      double[] maskNodata = maskMetadata.getDefaultValues();

      return maskRaster(maskMax, raster, nodata, maskRaster, maskNodata);
    }
  }
  catch (TileNotFoundException e)
  {
    try
    {
      if (pyramid != null)
      {
        if (metadata == null)
        {
          metadata = pyramid.getMetadata();
        }

        return MrGeoRaster.createEmptyRaster(metadata.getTilesize(), metadata.getTilesize(),
            metadata.getBands(), metadata.getTileType(), metadata.getDefaultValue(0));
      }
    }
    catch (IOException e1)
    {
      throw new ImageRendererException(e1);
    }
    throw new ImageRendererException("Unable to open pyramid: " + HadoopFileUtils.unqualifyPath(pyramidName), e);
  }
  catch (IOException e)
  {
    throw new ImageRendererException("Unable to open pyramid: " + HadoopFileUtils.unqualifyPath(pyramidName), e);
  }
}

@Override
public MrGeoRaster renderImage(String pyramidName, int tileColumn, int tileRow,
    double scale, String maskName, ProviderProperties providerProperties)
    throws ImageRendererException
{
  return renderImage(pyramidName, tileColumn, tileRow, scale, maskName, Double.MAX_VALUE, providerProperties);
}

@Override
public MrGeoRaster renderImage(String pyramidName, int tileColumn, int tileRow,
    int zoom, ProviderProperties providerProperties) throws ImageRendererException
{
  imageName = pyramidName;

  try
  {
    MrsPyramid pyramid = MrsPyramid.open(pyramidName, providerProperties);

    try (MrsImage image = pyramid.getImage(zoom))
    {
      log.debug("Retrieving tile {}, {}", tileColumn, tileRow);
      return image.getTile(tileColumn, tileRow);
    }
  }
  catch (IOException | NullPointerException e)
  {
    throw new ImageRendererException("Unable to open image: " + HadoopFileUtils.unqualifyPath(pyramidName + " at zoom level " + zoom), e);
  }
}

@Override
@SuppressWarnings("squid:S2583") // metadata may be null when TileNotFoundException is thrown, need to check...
public MrGeoRaster renderImage(String pyramidName, int tileColumn, int tileRow,
    int zoom, String maskName, double maskMax,
    ProviderProperties providerProperties) throws ImageRendererException
{
  imageName = pyramidName;

  MrsPyramid pyramid = null;
  MrsPyramidMetadata metadata = null;
  try
  {
    pyramid = MrsPyramid.open(pyramidName, providerProperties);
    try (MrsImage image = pyramid.getImage(zoom))
    {
      if (image == null)
      {
        throw new MrsImageException("Zoom level not found: " + pyramidName + " level: " + zoom);
      }
      MrGeoRaster raster = image.getTile(tileColumn, tileRow);
      log.debug("Retrieving tile {}, {}", tileColumn, tileRow);

      metadata = pyramid.getMetadata();
      double[] nodata = metadata.getDefaultValues();

      MrsPyramid maskPyramid = MrsPyramid.open(maskName, providerProperties);
      MrsImage maskImage = maskPyramid.getImage(zoom);

      MrGeoRaster maskRaster = maskImage.getTile(tileColumn, tileRow);
      log.debug("Retrieving mask tile {}, {}", tileColumn, tileRow);

      MrsPyramidMetadata maskMetadata = maskPyramid.getMetadata();
      double[] maskNodata = maskMetadata.getDefaultValues();

      return maskRaster(maskMax, raster, nodata, maskRaster, maskNodata);
    }
  }
  catch (TileNotFoundException e)
  {
    try
    {
      if (pyramid != null)
      {
        if (metadata == null)
        {
          metadata = pyramid.getMetadata();
        }

        return MrGeoRaster.createEmptyRaster(metadata.getTilesize(), metadata.getTilesize(),
            metadata.getBands(), metadata.getTileType(), metadata.getDefaultValue(0));
      }
    }
    catch (IOException e1)
    {
      throw new ImageRendererException(e1);
    }
    throw new ImageRendererException("Unable to open pyramid: " + HadoopFileUtils.unqualifyPath(pyramidName), e);
  }
  catch (IOException e)
  {
    throw new ImageRendererException("Unable to open pyramid: " + HadoopFileUtils.unqualifyPath(pyramidName), e);
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
public MrGeoRaster renderImage(String pyramidName, int tileColumn, int tileRow,
    int zoom, String maskName,
    ProviderProperties providerProperties) throws ImageRendererException
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
protected double[] getImageTileEnvelope(Bounds bounds, int zoom,
    int tilesize)
{
  log.debug("Calculating image tile envelope...");


  double res = TMSUtils.resolution(zoom, tilesize);

  double[] xform = new double[6];

  xform[0] = bounds.w; /* top left x */
  xform[1] = res; /* w-e pixel resolution */
  xform[2] = 0; /* 0 */
  xform[3] = bounds.n; /* top left y */
  xform[4] = 0; /* 0 */
  xform[5] = -res; /* n-s pixel resolution (negative value) */

  return xform;
}

@SuppressWarnings("squid:S1166") // Exception caught and handled
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

private MrGeoRaster maskRaster(double maskMax, MrGeoRaster raster, double[] nodata, MrGeoRaster maskRaster,
    double[] maskNodata)
{
  for (int w = 0; w < maskRaster.width(); w++)
  {
    for (int h = 0; h < maskRaster.height(); h++)
    {
      boolean masked = true;
      for (int b = 0; b < maskRaster.bands(); b++)
      {
        double maskPixel = maskRaster.getPixelDouble(w, h, b);
        if (maskPixel <= maskMax && Double.compare(maskPixel, maskNodata[b]) != 0)
        {
          masked = false;
          break;
        }
      }

      if (masked)
      {
        for (int b = 0; b < raster.bands(); b++)
        {
          raster.setPixel(w, h, b, nodata[b]);
        }
      }
    }
  }
  return raster;
}

}