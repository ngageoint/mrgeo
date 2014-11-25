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

package org.mrgeo.services.mrspyramid.rendering;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.geotools.coverage.CoverageFactoryFinder;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.processing.Operations;
import org.geotools.geometry.GeneralEnvelope;
import org.geotools.referencing.CRS;
import org.mrgeo.image.MrsImage;
import org.mrgeo.image.MrsImageException;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.image.geotools.GeotoolsRasterUtils;
import org.mrgeo.rasterops.ColorScale;
import org.mrgeo.tile.TileNotFoundException;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImagePyramidMetadataReader;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.services.KmlGenerator;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.TMSUtils;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.io.IOException;
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
  protected CoordinateReferenceSystem coordSys;

  // requested source data
  protected GridCoverage2D coverage;

  // requested zoom level
  protected int zoomLevel = -1;

  // true if an empty image is being returned
  protected boolean isTransparent = false;

  private MrsImage image;
  
  public ImageRendererAbstract()
  {
    try
    {
      GeotoolsRasterUtils.addMissingEPSGCodes();

      // I have no idea why we need the "true" here, but it doesn't work without it...
      this.coordSys = CRS.decode("EPSG:4326", true);
    }
    catch (final NoSuchAuthorityCodeException e)
    {
    }
    catch (final FactoryException e)
    {
    }
  }

  public ImageRendererAbstract(final CoordinateReferenceSystem coordSys)
  {
    this.coordSys = coordSys;
  }

  /**
   * 
   * 
   * @param pyrName
   * @param bounds
   * @param width
   * @param height
   * @param cs
   * @param requestUrl
   * @return
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
        .getKmlBodyAsString("kml", requestUrl, bounds, pyrName, wmsHost, null, "512",
            providerProperties);
    }

  /**
   * Determines the appropriate image zoom level for an image request
   * 
   * @param metadata
   *          source pyramid metadata
   * @param bounds
   *          requested bounds
   * @param width
   *          requested output image width
   * @param height
   *          requested output image height
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
   * @param metadata
   *          source pyramid metadata
   * @param zoomLevel
   *          requested zoom level
   * @return true if the source data contains an image at the requested zoom level; false otherwise
   * @throws IOException
   * @todo poorly implemented method, I think...but enough to get by for now; rewrite so no
   *       exception is thrown in case where image doesn't exist OR this more likely this won't be
   *       needed at all once we rework MrsImagePyramid/MrsImage for non-pyramid data
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
    return new double[] { -1.0 };
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
   * @param pyramidName
   *          name of the source data
   * @param bounds
   *          requested bounds
   * @param width
   *          requested width
   * @param height
   *          requested height
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

    final int tileSize = pyramidMetadata.getTilesize();

    // get the correct zoom level based on the requested bounds
    zoomLevel = getZoomLevel(pyramidMetadata, bounds, width, height);

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
      if (!bounds.toEnvelope().intersects(pyramidMetadata.getBounds().toEnvelope()))
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
      final RenderedImage mergedImage = image.getRenderedImage(bounds);

      // final TMSUtils.Bounds b = new TMSUtils.Bounds(bounds.getMinX(), bounds.getMinY(), bounds
      // .getMaxX(), bounds.getMaxY());
      // final TMSUtils.Bounds tb = TMSUtils.tileBounds(b, zoomLevel, tileSize);
      // final Bounds mb = new Bounds(tb.w, tb.s, tb.e, tb.n);
      //
      // GeoTiffExporter.export(mergedImage, mb, new File("mergedTiles.tiff"), null, -9999);

      if (mergedImage != null)
      {
        log.debug("merged image width: {}", mergedImage.getWidth());
        log.debug("merged image height: {}", mergedImage.getHeight());

        // determine the envelope of the merged tiles from the corresponding requested bounds
        final GeneralEnvelope imageEnvelope = getImageTileEnvelope(bounds, zoomLevel, tileSize);
        final GeneralEnvelope outputEnvelope = getOutputEnvelope(bounds);

        if (!imageEnvelope.intersects(outputEnvelope, true))
        {
          throw new Exception("Image bounds does not intersect output bounds.");
        }

        // create the output image, resample it, and crop it down to the
        // requested bounds
        final GridCoverageFactory factory = CoverageFactoryFinder.getGridCoverageFactory(null);
        /* final GridCoverage2D */coverage = factory.create("renderer", mergedImage, imageEnvelope);

        // GeoTiffExporter.export(coverage.getRenderedImage(), mb, new File("coverage.tiff"), null,
        // Float.NaN);

        // printImageInfo(coverage.getRenderedImage(), "raw image");

        log.debug("Cropping image...");
        coverage = (GridCoverage2D) Operations.DEFAULT.crop(coverage, outputEnvelope);
        log.debug("Image cropped.");
        // GeoTiffExporter.export(coverage.getRenderedImage(), bounds, new File("cropped.tiff"),
        // null,
        // Float.NaN);

        //Reproject gridcoverage here
        String sourceEPSG = "EPSG:4326";
        if (!(epsg == null) && !sourceEPSG.equalsIgnoreCase(epsg)) {
            log.debug("Reprojecting image to " + epsg + "...");
            CoordinateReferenceSystem targetCRS = CRS.decode(epsg, true);
            coverage = (GridCoverage2D) Operations.DEFAULT.resample(coverage,targetCRS);
        }

        log.debug("Scaling image...");
        final Raster scaled = RasterUtils.scaleRasterNearest(coverage.getRenderedImage().getData(),
          width, height);

        // GeoTiffExporter.export(RasterUtils.makeBufferedImage(scaled), bounds, new File(
        // "scaled.tiff"), null, Float.NaN);

        return scaled;
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
   * @param pyramidName
   *          name of the source data
   * @param bounds
   *          requested bounds
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
   * @param pyramidName
   *          name of the source data
   * @param tileColumn
   *          x tile coordinate
   * @param tileRow
   *          y tile coordinate
   * @param scale
   *          requested image resolution
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
      try
      {
        final Raster raster = image.getTile(tileColumn, tileRow);
        log.debug("Retrieving tile {}, {}", tileColumn, tileRow);

        return raster;
      }
      catch (final TileNotFoundException e)
      {
        throw e;
//        final MrsImagePyramidMetadata metadata = pyramid.getMetadata();
//        return RasterUtils.createEmptyRaster(metadata.getTilesize(), metadata.getTilesize(),
//          metadata.getBands(), metadata.getTileType(), metadata.getDefaultValue(0));
      }
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
      try
      {
        final Raster raster = image.getTile(tileColumn, tileRow);
        log.debug("Retrieving tile {}, {}", tileColumn, tileRow);

        return raster;
      }
      catch (final TileNotFoundException e)
      {
        throw e;
//        final MrsImagePyramidMetadata metadata = pyramid.getMetadata();
//        return RasterUtils.createEmptyRaster(metadata.getTilesize(), metadata.getTilesize(),
//          metadata.getBands(), metadata.getTileType(), metadata.getDefaultValue(0));
      }
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
   * @param bounds
   *          requested bounds
   * @param zoom
   *          calculated zoom level for the requested bounds
   * @param tileSize
   *          tile size of the source data
   * @return an envelope containing all source image tiles touching the requested bounds
   */
  protected GeneralEnvelope getImageTileEnvelope(final Bounds bounds, final int zoom,
    final int tileSize)
  {
    log.debug("Calculating image tile envelope...");
    final TMSUtils.Bounds outputBounds = new TMSUtils.Bounds(bounds.getMinX(), bounds.getMinY(),
      bounds.getMaxX(), bounds.getMaxY());
    final TMSUtils.Bounds tileBounds = TMSUtils.tileBounds(outputBounds, zoom, tileSize);

    final GeneralEnvelope imageEnvelope = new GeneralEnvelope(new double[] { tileBounds.w,
      tileBounds.s }, new double[] { tileBounds.e, tileBounds.n });

    imageEnvelope.setCoordinateReferenceSystem(coordSys);
    if (log.isDebugEnabled())
    {
      log.debug("Image tile envelope: {}", imageEnvelope.toString());
    }
    return imageEnvelope;
  }

  /**
   * Creates an envelope for the requested bounds
   * 
   * @param bounds
   *          requested bounds
   * @return an envelope exactly equal to the requested bounds
   */
  protected GeneralEnvelope getOutputEnvelope(final Bounds bounds)
  {
    final GeneralEnvelope outputEnvelope = new GeneralEnvelope(new double[] { bounds.getMinX(),
      bounds.getMinY() }, new double[] { bounds.getMaxX(), bounds.getMaxY() });

    outputEnvelope.setCoordinateReferenceSystem(coordSys);

    if (log.isDebugEnabled())
    {
      log.debug("output envelope: {}", outputEnvelope.toString());
    }
    return outputEnvelope;
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

}