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

package org.mrgeo.image.geotools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.geotools.coverage.CoverageFactoryFinder;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.io.*;
import org.geotools.coverage.processing.CoverageProcessingException;
import org.geotools.coverage.processing.OperationJAI;
import org.geotools.coverage.processing.Operations;
import org.geotools.factory.Hints;
import org.geotools.geometry.Envelope2D;
import org.geotools.geometry.GeneralEnvelope;
import org.geotools.metadata.iso.citation.Citations;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.geotools.referencing.factory.PropertyAuthorityFactory;
import org.geotools.referencing.factory.ReferencingFactoryContainer;
import org.geotools.util.logging.Logging;
import org.jaitools.CollectionFactory;
import org.jaitools.media.jai.zonalstats.Result;
import org.jaitools.media.jai.zonalstats.ZonalStats;
import org.jaitools.media.jai.zonalstats.ZonalStatsDescriptor;
import org.jaitools.numeric.Range;
import org.jaitools.numeric.Statistic;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.image.ImageStats;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.image.MrsImagePyramidMetadata.Classification;
import org.mrgeo.rasterops.GeoTiffExporter;
import org.mrgeo.rasterops.SimpleTileCache;
import org.mrgeo.utils.*;
import org.opengis.coverage.Coverage;
import org.opengis.coverage.SampleDimensionType;
import org.opengis.geometry.DirectPosition;
import org.opengis.geometry.Envelope;
import org.opengis.parameter.GeneralParameterValue;
import org.opengis.parameter.ParameterValue;
import org.opengis.parameter.ParameterValueGroup;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import javax.imageio.spi.IIORegistry;
import javax.imageio.spi.ImageInputStreamSpi;
import javax.media.jai.*;
import javax.media.jai.operator.ScaleDescriptor;
import java.awt.*;
import java.awt.color.ColorSpace;
import java.awt.image.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.*;
import java.util.List;
import java.util.logging.LogManager;

public class GeotoolsRasterUtils
{
private static Logger log = LoggerFactory.getLogger(GeotoolsRasterUtils.class);

static
{
  try
  {
    // lower the log level of geotools (it spits out _way_ too much logging)
    LoggingUtils.setLogLevel("org.geotools", LoggingUtils.ERROR);

    // connect geotools to slf4j
    Logging.GEOTOOLS.setLoggerFactory("org.geotools.util.logging.Log4JLoggerFactory");


    // connect imageio-ext (Java logging (JUL)) to slf4j and set its level
    LogManager.getLogManager().reset();
    SLF4JBridgeHandler.install();

    String defaultLevel = LoggingUtils.getDefaultLogLevel();

    if (LoggingUtils.finer(defaultLevel, LoggingUtils.WARN))
    {
      LoggingUtils.setLogLevel("it.geosolutions", defaultLevel);
    }
    else
    {
      LoggingUtils.setLogLevel("it.geosolutions", LoggingUtils.ERROR);
    }
  }

  catch (final Exception e)
  {
    e.printStackTrace();
  }

  // force geotools to use y, x (lat, lon) ordering. This is the default, but we'll set
  // this property here just in case
  System.setProperty("org.geotools.referencing.forceXY", "false");

  addMissingEPSGCodesInternal();

  final ClassLoader cl = Thread.currentThread().getContextClassLoader();
  try
  {
    cl.loadClass("com.sun.medialib.mlib.Image");
  }
  catch (final ClassNotFoundException e)
  {
    // native acceleration is not loaded, disable it...
    System.setProperty("com.sun.media.jai.disableMediaLib", "true");
  }

  JAI.disableDefaultTileCache();
}

private static Set<AbstractGridFormat> formats = null;

public final static int LON_DIMENSION = 0;
public final static int LAT_DIMENSION = 1;

private static boolean epsgLoaded = false;

/**
 * Adds a constant border to the image data of a coverage
 *
 * @param source
 *          source data
 * @param borderSize
 *          size of the border to add to the image
 * @param borderType
 *          type of border to add to the image
 * @return bordered image
 */
public static Coverage addConstantBorder(final Coverage source, final int borderSize,
    final BorderExtender borderType)
{
  final Integer borderSizeInt = borderSize;
  final OperationJAI borderOp = new OperationJAI("Border");
  final ParameterValueGroup params = borderOp.getParameters();
  params.parameter("source").setValue(source);
  params.parameter("leftPad").setValue(borderSizeInt);
  params.parameter("rightPad").setValue(borderSizeInt);
  params.parameter("topPad").setValue(borderSizeInt);
  params.parameter("bottomPad").setValue(borderSizeInt);
  params.parameter("type").setValue(borderType);
  return borderOp.doOperation(params, null);
}

public static void addMissingEPSGCodes()
{
  // noop. The static block above will force a call to addMissingEPSGCodesInternal()...
}

public static MrsImagePyramidMetadata calculateMetaData(final InputStream stream, final String output, final boolean calcStats,
    final String protectionLevel, final boolean categorical) throws Exception
{
  final MrsImagePyramidMetadata metadata = new MrsImagePyramidMetadata();

  calculateMetaData(stream, metadata, output, calcStats, protectionLevel,
      categorical, false);

  return metadata;

}

public static MrsImagePyramidMetadata calculateMetaData(final String input,
    final String protectionLevel, double delme) throws Exception
{
  final String[] inputs = { input };
  return calculateMetaData(inputs, "", false, protectionLevel, false, false);
}

public static MrsImagePyramidMetadata calculateMetaData(final String input, final String output,
    final String protectionLevel) throws Exception
{
  final String[] inputs = { input };
  return calculateMetaData(inputs, output, false, protectionLevel, false, false);
}

public static MrsImagePyramidMetadata calculateMetaData(final String[] inputs, final String output, final boolean calcStats,
    final String protectionLevel, final boolean categorical,
    final boolean overridenodata)
    throws Exception
{
  log.info("Calculating Metadata for output: " + output);

  final MrsImagePyramidMetadata metadata = new MrsImagePyramidMetadata();

  for (final String input : inputs)
  {
    log.info("   load data for : " + input);

    InputStream stream = openImageStream(input);
    try
    {
      calculateMetaData(stream, metadata, output, calcStats, protectionLevel,
          categorical, overridenodata);
    }
    finally
    {
      if (stream != null)
      {
        stream.close();
      }
    }
  }

  return metadata;
}

public static LongRectangle calculateTiles(final AbstractGridCoverage2DReader reader,
    final int tilesize, final int zoomlevel) throws IllegalArgumentException, IOException
{

  // long minTx = Long.MAX_VALUE, minTy = Long.MAX_VALUE;
  // long maxTx = 0, maxTy = 0;

  final GridCoverage2D image = GeotoolsRasterUtils.getImageFromReader(reader, "EPSG:4326");

  final GeneralEnvelope envelope = (GeneralEnvelope) image.getEnvelope();

  log.debug("    image bounds: (lon/lat) " +
      envelope.getMinimum(GeotoolsRasterUtils.LON_DIMENSION) + ", " +
      envelope.getMinimum(GeotoolsRasterUtils.LAT_DIMENSION) + " to " +
      envelope.getMaximum(GeotoolsRasterUtils.LON_DIMENSION) + ", " +
      envelope.getMaximum(GeotoolsRasterUtils.LAT_DIMENSION));

  final TMSUtils.Bounds b = new TMSUtils.Bounds(envelope
      .getMinimum(GeotoolsRasterUtils.LON_DIMENSION), envelope
      .getMinimum(GeotoolsRasterUtils.LAT_DIMENSION), envelope
      .getMaximum(GeotoolsRasterUtils.LON_DIMENSION), envelope
      .getMaximum(GeotoolsRasterUtils.LAT_DIMENSION));

  final TMSUtils.TileBounds tb = TMSUtils.boundsToTile(b, zoomlevel, tilesize);
  final TMSUtils.Bounds tbb = TMSUtils.tileBounds(b, zoomlevel, tilesize);
  log.debug("    tile bounds (x/y): " + tb.w + ", " + tb.s + " to " + tb.e + ", " + tb.n +
      " (inclusive)");
  log.debug("            (lon/lat)  " + tbb.w + ", " + tbb.s + " to " + tbb.e + ", " + tbb.n);

  return new LongRectangle(tb.w, tb.s, tb.e, tb.n);

  // // calculate the starting tile
  // TMSUtils.Pixel p =
  // TMSUtils.latLonToPixels(envelope.getMinimum(GeotoolsRasterUtils.LAT_DIMENSION),
  // envelope.getMinimum(GeotoolsRasterUtils.LON_DIMENSION), zoomlevel, tilesize);
  // TMSUtils.Tile tileStart = TMSUtils.pixelsToTile(p.px, p.py, tilesize);
  //
  // TMSUtils.Tile t = TMSUtils.pixelsToTile(p.px + 1, p.py + 1, tilesize);
  //
  // // startLon is on the tile boundary, so we'll move it to the right
  // if (t.tx > tileStart.tx)
  // {
  // tileStart = new TMSUtils.Tile(t.tx, tileStart.ty);
  // }
  //
  // // startLat is on the tile boundary, so we'll move it up
  // if (t.ty > tileStart.ty)
  // {
  // tileStart = new TMSUtils.Tile(tileStart.ty, t.ty);
  // }
  //
  // // calculate ending tile
  // p = TMSUtils.latLonToPixels(envelope.getMaximum(GeotoolsRasterUtils.LAT_DIMENSION),
  // envelope.getMaximum(GeotoolsRasterUtils.LON_DIMENSION), zoomlevel, tilesize);
  // TMSUtils.Tile tileEnd = TMSUtils.pixelsToTile(p.px, p.py, tilesize);
  //
  // t = TMSUtils.pixelsToTile(p.px - 1, p.py - 1, tilesize);
  //
  // // endLon is on the tile boundary, so we'll move it to the left
  // if (t.tx < tileEnd.tx)
  // {
  // tileEnd = new TMSUtils.Tile(t.tx, tileEnd.ty);
  // }
  //
  // // endLat is on the tile boundary, so we'll move it down
  // if (t.ty < tileEnd.ty)
  // {
  // tileEnd = new TMSUtils.Tile(tileEnd.tx, t.ty);
  // }
  //
  // // set min and max tiles
  // if (minTx > tileStart.tx)
  // {
  // minTx = tileStart.tx;
  // }
  // if (minTx > tileEnd.tx)
  // {
  // minTx = tileEnd.tx;
  // }
  // if (minTy > tileStart.ty)
  // {
  // minTy = tileStart.ty;
  // }
  // if (minTy > tileEnd.ty)
  // {
  // minTy = tileEnd.ty;
  // }
  //
  // if (maxTx < tileStart.tx)
  // {
  // maxTx = tileStart.tx;
  // }
  // if (maxTx < tileEnd.tx)
  // {
  // maxTx = tileEnd.tx;
  // }
  // if (maxTy < tileStart.ty)
  // {
  // maxTy = tileStart.ty;
  // }
  // if (maxTy < tileEnd.ty)
  // {
  // maxTy = tileEnd.ty;
  // }
  //
  // return new LongRectangle( minTx, minTy, maxTx, maxTy);
}

public static int
calculateZoomlevel(final AbstractGridCoverage2DReader reader, final int tilesize)
    throws IllegalArgumentException, CoverageProcessingException, TransformException, IOException
{

  final GridCoverage2D img = getImageFromReader(reader, "EPSG:4326");

  // calculate zoom level for the image
  final GridGeometry2D geometry = img.getGridGeometry();
  final Envelope2D pixelEnvelop = geometry.gridToWorld(new GridEnvelope2D(0, 0, 1, 1));

  final double pixelsizeLon = Math.abs(pixelEnvelop.width);
  final double pixelsizeLat = Math.abs(pixelEnvelop.height);

  final int zx = TMSUtils.zoomForPixelSize(pixelsizeLon, tilesize);
  final int zy = TMSUtils.zoomForPixelSize(pixelsizeLat, tilesize);

  if (zx > zy)
  {
    return zx;
  }

  return zy;
}

public static void closeStreamFromReader(final AbstractGridCoverage2DReader reader)
    throws Exception
{
  // this is a kinda round-about way to close the input file stream, but we don't have
  // access to the actual stream. So we need to get the source from the reader, check for
  // a close method, and invoke it.
  if (reader != null)
  {
    final Object obj = reader.getSource();

    // use reflection to see if we have a close() method.
    try
    {
      final Method m = obj.getClass().getMethod("close", new Class[] {});
      m.invoke(obj, new Object[] {});
    }
    catch (final NoSuchMethodException e)
    {
      // suck this up. It just means there is no close() method, so we ignore it.
    }
    catch (final Exception e)
    {
      throw e;
    }
  }
}

public static GridCoverage2D crop(final GridCoverage2D image, final double left,
    final double bottom, final double right, final double top)
{
  final CoordinateReferenceSystem crs = image.getCoordinateReferenceSystem2D();

  final GeneralEnvelope croppedEnvelope = new GeneralEnvelope(new double[] { left, bottom },
      new double[] { right, top });
  croppedEnvelope.setCoordinateReferenceSystem(crs);

  return (GridCoverage2D) Operations.DEFAULT.crop(image, croppedEnvelope);
}

public static GridCoverage2D crop(final GridCoverage2D image, final long txLeft,
    final long tyBottom, final long txRight, final long tyTop, final int zoomlevel,
    final int tilesize, final double[] defaultValues, final boolean categorical)
{
  final TMSUtils.Bounds blBounds = TMSUtils.tileBounds(txLeft, tyBottom, zoomlevel, tilesize);
  final TMSUtils.Bounds trBounds = TMSUtils.tileBounds(txRight, tyTop, zoomlevel, tilesize);

  final CoordinateReferenceSystem crs = image.getCoordinateReferenceSystem();

  final GeneralEnvelope croppedEnvelope = new GeneralEnvelope(new double[] { blBounds.w,
      blBounds.s }, new double[] { trBounds.e, trBounds.n });
  croppedEnvelope.setCoordinateReferenceSystem(crs);

  return (GridCoverage2D) Operations.DEFAULT.crop(image, croppedEnvelope);
}

public static PlanarImage prepareForCutting(GridCoverage2D geotoolsImage, int zoomlevel, int tilesize, Classification classification)
{

  int ih = (int) geotoolsImage.getGridGeometry().getGridRange2D().getHeight();
  int iw = (int) geotoolsImage.getGridGeometry().getGridRange2D().getWidth();

//      WritableRaster wr = RasterUtils.makeRasterWritable(geotoolsImage.getRenderedImage().getData());
//
//      int cnt = 1;
//      for (int y = 0; y < iih; y++)
//      {
//        for (int x = 0; x < iiw; x++)
//        {
//          wr.setSample(x, y, 0, cnt++);
//        }
//      }
//
//      GridCoverageFactory f = new GridCoverageFactory();
//      geotoolsImage = f.create("foo", wr, envelope);

  GeneralEnvelope envelope = (GeneralEnvelope) geotoolsImage.getEnvelope();
  TMSUtils.Bounds imageBounds = new TMSUtils.Bounds(
      envelope.getMinimum(GeotoolsRasterUtils.LON_DIMENSION),
      envelope.getMinimum(GeotoolsRasterUtils.LAT_DIMENSION),
      envelope.getMaximum(GeotoolsRasterUtils.LON_DIMENSION),
      envelope.getMaximum(GeotoolsRasterUtils.LAT_DIMENSION));

  TMSUtils.TileBounds tiles = TMSUtils.boundsToTile(imageBounds, zoomlevel, tilesize);
  TMSUtils.Bounds tileBounds = TMSUtils.tileBounds(tiles.w, tiles.n, zoomlevel, tilesize);

  tileBounds = TMSUtils.tileBounds(imageBounds, zoomlevel, tilesize);

  DPx tp = DPx.latLonToPixels(tileBounds.n, tileBounds.w, zoomlevel, tilesize);
  DPx ip = DPx.latLonToPixels(imageBounds.n, imageBounds.w, zoomlevel, tilesize);
  DPx ep = DPx.latLonToPixels(imageBounds.s, imageBounds.e, zoomlevel, tilesize);

  float xlatex = (float) (ip.px - tp.px);
  float xlatey = (float) (tp.py - ip.py);

  float scalex = (float)(ep.px - ip.px) / (iw);
  float scaley = (float)(ip.py - ep.py) / (ih);


  // take care of categorical data
  Interpolation interp;
  if (classification != null && classification == Classification.Categorical)
  {
    interp = Interpolation.getInstance(Interpolation.INTERP_NEAREST);
  }
  else
  {
    interp = Interpolation.getInstance(Interpolation.INTERP_BILINEAR);
  }

  RenderingHints qualityHints = new RenderingHints(RenderingHints.KEY_RENDERING,
      RenderingHints.VALUE_RENDER_QUALITY);
  qualityHints.put(JAI.KEY_BORDER_EXTENDER, BorderExtender.createInstance(BorderExtender.BORDER_COPY));


  return ScaleDescriptor.create(geotoolsImage.getRenderedImage(), scalex, scaley, xlatex, xlatey, interp, qualityHints);
}

public static Raster
cutTile(final GridCoverage2D image, final long tx, final long ty, final int zoomlevel,
    final int tilesize, final double[] defaultValues, final boolean categorical)
{
  final TMSUtils.Bounds bounds = TMSUtils.tileBounds(tx, ty, zoomlevel, tilesize);

  final CoordinateReferenceSystem crs = image.getCoordinateReferenceSystem();

  final GeneralEnvelope imageEnvelope = (GeneralEnvelope) image.getEnvelope();

  final GeneralEnvelope croppedEnvelope = new GeneralEnvelope(
      new double[] { bounds.w, bounds.s }, new double[] { bounds.e, bounds.n });
  croppedEnvelope.setCoordinateReferenceSystem(crs);

  // final GridGeometry resizedGeometry = new GridGeometry2D(new GeneralGridEnvelope(new
  // Rectangle(
  // 0, 0, tilesize, tilesize)), croppedEnvelope);
  //
  // final RenderingHints hints;
  //
  // final long freeMemory = Runtime.getRuntime().freeMemory();
  //
  // // if we'll use up 80%+ of available memory, we'll count this as a large image
  // if (imagesize > (freeMemory * 0.80))
  // {
  // // if we've got a large image, turn off the border extender. This will make the cut
  // // take a lot longer, but it _will_ work, otherwise, failure.
  // hints = new RenderingHints(JAI.KEY_BORDER_EXTENDER, null);
  // }
  // else
  // {
  // hints = null;
  // }

  WritableRaster tile;
  final GridCoverage2D cropped = (GridCoverage2D) Operations.DEFAULT.crop(image, croppedEnvelope);

  final boolean edge = !imageEnvelope.contains(croppedEnvelope, true);
  // System.out.println("image:   " + imageEnvelope);
  // System.out.println("cropped: " + croppedEnvelope);
  if (edge)
  {
    Number nodata = Double.NaN;
    if (defaultValues != null)
    {
      nodata = defaultValues[0];
    }

    final Raster cr = cropped.getRenderedImage().getData();

    // Subtracting a small epsilon from the north edge of the tile prevents the pixel
    // that encompasses that edge from being included in the tile immediately
    // below that edge. That edge needs to be included in the tile above it.
    final double n = Math.min(imageEnvelope.getMaximum(LAT_DIMENSION), croppedEnvelope
        .getMaximum(LAT_DIMENSION) - 1e-9);
    final double s = Math.max(imageEnvelope.getMinimum(LAT_DIMENSION), croppedEnvelope
        .getMinimum(LAT_DIMENSION));
    final double e = Math.min(imageEnvelope.getMaximum(LON_DIMENSION), croppedEnvelope
        .getMaximum(LON_DIMENSION));
    final double w = Math.max(imageEnvelope.getMinimum(LON_DIMENSION), croppedEnvelope
        .getMinimum(LON_DIMENSION));

    final TMSUtils.Pixel ul = TMSUtils.latLonToTilePixelUL(n, w, tx, ty, zoomlevel, tilesize);
    final TMSUtils.Pixel lr = TMSUtils.latLonToTilePixelUL(s, e, tx, ty, zoomlevel, tilesize);

    final int dstWidth = Math.min((int) (lr.px - ul.px) + 1, tilesize - (int) ul.px);
    final int dstHeight = Math.min((int) (lr.py - ul.py) + 1, tilesize - (int) ul.py);

    Raster scaled;
    if (categorical)
    {
      scaled = RasterUtils.scaleRasterNearest(cr, dstWidth, dstHeight);
    }
    else
    {
      scaled = RasterUtils.scaleRasterInterp(cr, dstWidth, dstHeight, nodata);
    }

    tile = cr.createCompatibleWritableRaster(tilesize, tilesize);
    RasterUtils.fillWithNodata(tile, nodata.doubleValue());

    final Object data = scaled.getDataElements(scaled.getMinX(), scaled.getMinY(), scaled
        .getWidth(), scaled.getHeight(), null);
    tile.setDataElements((int) ul.px, (int) ul.py, dstWidth, dstHeight, data);

  }
  else if (categorical)
  {
    tile = RasterUtils.scaleRasterNearest(cropped.getRenderedImage().getData(), tilesize,
        tilesize);
  }
  else
  {
    Number nodata = Double.NaN;
    if (defaultValues != null)
    {
      nodata = defaultValues[0];
    }

    tile = RasterUtils.scaleRasterInterp(cropped.getRenderedImage().getData(), tilesize,
        tilesize, nodata);
  }

  // try
  // {
  // QuickExport.saveLocalGeotiff("/data/export/greece", tile, tx, ty, zoomlevel, tilesize,
  // defaultValues[0]);
  // }
  // catch (NoSuchAuthorityCodeException e)
  // {
  // e.printStackTrace();
  // }
  // catch (IOException e)
  // {
  // e.printStackTrace();
  // }
  // catch (FactoryException e)
  // {
  // e.printStackTrace();
  // }

  return tile;

  // Number nodata = Double.NaN;
  // if (defaultValues != null)
  // {
  // nodata = defaultValues[0];
  // }
  //
  // // Geotools' resampler seems to have a bug with Float & Double data using an interpolator.
  // // We'll make our own...
  // resampled = (GridCoverage2D) op.resample(image, crs,
  // resizedGeometry, new NodataInterpolation(new MeanAggregator(), nodata), defaultValues);
  // }
  //
  // // resampled = (GridCoverage2D) Operations.DEFAULT.resample(image, null,
  // // resizedGeometry, Interpolation.getInstance(Interpolation.INTERP_NEAREST), defaultValues);
  //
  // return resampled.getRenderedImage().getData();
}

public static boolean fastAccepts(final Object obj)
{
  if (formats == null)
  {
    loadFormats();
  }

  for (final AbstractGridFormat format : formats)
  {
    if (format.accepts(obj))
    {
      return true;
    }
  }
  return false;
}

public static AbstractGridFormat fastFormatFinder(final Object obj)
{
  if (formats == null)
  {
    loadFormats();
  }

  for (final AbstractGridFormat format : formats)
  {
    if (format.accepts(obj))
    {
      return format;
    }
  }
  return null;
}

public static GridCoverage2D getImageFromReader(final AbstractGridCoverage2DReader reader,
    final String epsg) throws IOException
{
  // /////////////////////////////////////////////////////////////////////
  //
  // Read ignoring overviews with subsampling and crop, using Jai,
  // and customized tilesize
  //
  // /////////////////////////////////////////////////////////////////////
  GridCoverage2D image = null;
  try
  {
    final ParameterValue<OverviewPolicy> policy = AbstractGridFormat.OVERVIEW_POLICY
        .createValue();
    policy.setValue(OverviewPolicy.IGNORE);

    // this will basically read 4 tiles worth of data at once from the disk...
    final ParameterValue<String> gridsize = AbstractGridFormat.SUGGESTED_TILE_SIZE.createValue();
    gridsize.setValue(MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT * 4 + "," + MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT);

    // Setting read type: use JAI ImageRead (true) or ImageReaders read methods (false)
    final ParameterValue<Boolean> useJaiRead = AbstractGridFormat.USE_JAI_IMAGEREAD.createValue();
    useJaiRead.setValue(true);

    // image = reader.read(new GeneralParameterValue[] { policy, useJaiRead });
    image = reader.read(new GeneralParameterValue[] { policy, gridsize, useJaiRead });

    final CoordinateReferenceSystem crs = image.getCoordinateReferenceSystem();
    if (!crs.equals(CRS.decode(epsg)))
    {
      final CoordinateReferenceSystem targetCRS;

      // see if we need to swap lat/lon in the reprojection.
      if (CRS.getAxisOrder(crs) == CRS.AxisOrder.EAST_NORTH)
      {
        targetCRS = CRS.decode(epsg, true); // force lon/lat ordering...
      }
      else
      {
        targetCRS = CRS.decode(epsg); // don't need to swap
      }

      final Operations op;

      final Integer code = CRS.lookupEpsgCode(crs, false);
      if (code == null)
      {
        op = new Operations(new RenderingHints(Hints.LENIENT_DATUM_SHIFT, true));
      }
      else
      {
        op = new Operations(null);
      }
      image = (GridCoverage2D) op.resample(image, targetCRS);
    }
  }
  catch (final Exception e)
  {
    e.printStackTrace();
    throw new IOException(e);
  }

  return image;
}


public static AbstractGridCoverage2DReader openImage(final String image) throws IOException
{
  // final File stream = new File(input);
  log.debug("Loading Image file: " + image);
  try
  {

    final InputStream stream = openImageStream(image);


    final AbstractGridCoverage2DReader reader = openImageFromStream(stream);
    if (reader == null)
    {
      throw new IOException("Error opening a reader for " + image);
    }

    return reader;
  }
  catch (IOException e)
  {
    throw new IOException("Can not load image: " + image, e);
  }
}

public static AbstractGridCoverage2DReader openImageFromFile(final File file) throws IOException
{
  AbstractGridFormat format = GridFormatFinder.findFormat( file );

  if (format != null)
  {
    return format.getReader( file );
  }

  throw new IOException("Unknown geospatial input format");
}


public static AbstractGridCoverage2DReader openImageFromStream(InputStream stream) throws IOException
{
  final AbstractGridFormat format = GeotoolsRasterUtils.fastFormatFinder(stream);
  if (format != null)
  {
    final TileCache tc = new SimpleTileCache();
    // increase the tile cache size to speed things up, 256MB
    final long memCapacity = 268435456;
    tc.setMemoryCapacity(memCapacity);

    final Hints hints = new Hints(JAI.KEY_TILE_CACHE, tc);
    final AbstractGridCoverage2DReader reader = format.getReader(stream, hints);

    return reader;
  }

  stream.close();
  throw new IOException("Unknown geospatial input format");
}


public static Number readNoData(final AbstractGridCoverage2DReader reader)
    throws SecurityException, NoSuchMethodException, IllegalArgumentException,
    IllegalAccessException, InvocationTargetException
{
  Method m = reader.getClass().getMethod("getMetadata", new Class[]{});
  final Object meta = m.invoke(reader, new Object[] {});

  m = meta.getClass().getMethod("getNoData", new Class[] {});

  return (Number) m.invoke(meta, new Object[] {});
}

public static int sampleDimensionToBytes(final SampleDimensionType type)
{
  if (type.equals(SampleDimensionType.REAL_64BITS))
  {
    return 64 >> 3;
  }
  else if (type.equals(SampleDimensionType.REAL_32BITS))
  {
    return 32 >> 3;
  }
  else if (type.equals(SampleDimensionType.SIGNED_16BITS))
  {
    return 16 >> 3;
  }
  else if (type.equals(SampleDimensionType.SIGNED_32BITS))
  {
    return 32 >> 3;
  }
  else if (type.equals(SampleDimensionType.SIGNED_8BITS))
  {
    return 8 >> 3;
  }
  else if (type.equals(SampleDimensionType.UNSIGNED_16BITS))
  {
    return 16 >> 3;
  }
  else if (type.equals(SampleDimensionType.UNSIGNED_1BIT))
  {
    return 1 >> 3;
  }
  else if (type.equals(SampleDimensionType.UNSIGNED_2BITS))
  {
    return 2 >> 3;
  }
  else if (type.equals(SampleDimensionType.UNSIGNED_32BITS))
  {
    return 32 >> 3;
  }
  else if (type.equals(SampleDimensionType.UNSIGNED_4BITS))
  {
    return 4 >> 3;
  }
  else if (type.equals(SampleDimensionType.UNSIGNED_8BITS))
  {
    return 8 >> 3;
  }

  return 0;
}

public static void saveLocalGeotiff(final String filename, final GridCoverage2D coverage,
    final double nodata) throws IOException
{

  final Envelope envelope = coverage.getEnvelope();
  final Bounds bounds = new Bounds(envelope.getMinimum(GeotoolsRasterUtils.LON_DIMENSION),
      envelope.getMinimum(GeotoolsRasterUtils.LAT_DIMENSION), envelope
      .getMaximum(GeotoolsRasterUtils.LON_DIMENSION), envelope
      .getMaximum(GeotoolsRasterUtils.LAT_DIMENSION));

  RenderedImage image = coverage.getRenderedImage();
  int datatype = image.getSampleModel().getDataType();

  if ((datatype == DataBuffer.TYPE_DOUBLE || datatype == DataBuffer.TYPE_FLOAT) && Double.isNaN(nodata))
  {
    GeoTiffExporter.export(image, bounds, new File(filename), true, -9999);
  }
  else
  {
    GeoTiffExporter.export(image, bounds, new File(filename), nodata);
  }
}

public static void saveLocalGeotiff(final String path, final Raster raster, final long tx,
    final long ty, final int zoomlevel, final int tilesize, final double nodata)
    throws IOException, NoSuchAuthorityCodeException, FactoryException
{

  // first build the envelope for the tile
  final TMSUtils.Bounds bounds = TMSUtils.tileBounds(tx, ty, zoomlevel, tilesize);

  final CoordinateReferenceSystem crs = CRS.decode("EPSG:4326");

  final GeneralEnvelope envelope = new GeneralEnvelope(new double[] { bounds.w, bounds.s },
      new double[] { bounds.e, bounds.n });
  envelope.setCoordinateReferenceSystem(crs);

  // final GridGeometry geometry = new GridGeometry2D(new GeneralGridEnvelope(new Rectangle(
  // 0, 0, tilesize, tilesize)), envelope);

  // now build a PlanarImage from the raster
  // final int type = raster.getTransferType();
  final int bands = raster.getNumBands();
  final int offsets[] = new int[bands];
  for (int i = 0; i < offsets.length; i++)
  {
    offsets[i] = 0;
  }

  final BufferedImage img = RasterUtils.makeBufferedImage(raster);

  final String name = ty + "-" + tx + "-" + TMSUtils.tileid(tx, ty, zoomlevel);

  final GridCoverageFactory factory = CoverageFactoryFinder.getGridCoverageFactory(null);
  final GridCoverage2D coverage = factory.create(name, img, envelope);
  saveLocalGeotiff(path  + ".tif", coverage, nodata);
}

public static void saveLocalGeotiff(final String path, final WritableRaster raster,
    final GeneralEnvelope envelope, final double nodata) throws IOException
{
  // now build a PlanarImage from the raster
  final int type = raster.getTransferType();
  final int bands = raster.getNumBands();

  // ColorSpace cs = ColorSpace.getInstance(ColorSpace.CS_sRGB);
  ColorModel cm = PlanarImage.getDefaultColorModel(type, bands);

  if (cm == null)
  {
    if (type == DataBuffer.TYPE_DOUBLE || type == DataBuffer.TYPE_FLOAT)
    {
      cm = new FloatDoubleColorModel(ColorSpace.getInstance(ColorSpace.CS_GRAY), false, false,
          Transparency.OPAQUE, type);
    }
    else
    {
      cm = new ComponentColorModel(ColorSpace.getInstance(ColorSpace.CS_GRAY), false, false,
          Transparency.OPAQUE, type);
    }
  }

  final BufferedImage img = new BufferedImage(cm, raster, false, null);

  final GridCoverageFactory factory = CoverageFactoryFinder.getGridCoverageFactory(null);
  final GridCoverage2D coverage = factory.create(path, img, envelope);

  saveLocalGeotiff(path, coverage, nodata);
}

private static synchronized void addMissingEPSGCodesInternal()
{
  if (epsgLoaded)
  {
    return;
  }

  epsgLoaded = true;

  log.info("Loading missing epsg codes");

  // there are non-standard EPSG codes (like Google's 900913) missing from the
  // main database.
  // this code loads them from an epsg properties file. To add additional
  // codes, just add them to
  // the property file as wkt

  // increase the tile cache size to speed things up, 256MB
  final long memCapacity = 268435456;
  if (JAI.getDefaultInstance().getTileCache().getMemoryCapacity() < memCapacity)
  {
    JAI.getDefaultInstance().getTileCache().setMemoryCapacity(memCapacity);
  }

  try
  {
    final URL epsg = GeotoolsRasterUtils.class.getResource("epsg.properties");

    if (epsg != null)
    {
      final Hints hints = new Hints(Hints.CRS_AUTHORITY_FACTORY, PropertyAuthorityFactory.class,
          Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.FALSE);
      final ReferencingFactoryContainer referencingFactoryContainer = ReferencingFactoryContainer
          .instance(hints);

      final PropertyAuthorityFactory factory = new PropertyAuthorityFactory(
          referencingFactoryContainer, Citations.fromName("EPSG"), epsg);

      ReferencingFactoryFinder.addAuthorityFactory(factory);
      ReferencingFactoryFinder.scanForPlugins(); // hook everything up
    }
    else
    {
      log.error("epsg code resource (epsg.properties) missing from " +
          GeotoolsRasterUtils.class.getPackage().getName() + " package");
    }
  }
  // Set<String> codes = CRS.getSupportedCodes("EPSG");
  // System.out.println(codes.toString());
  // CoordinateReferenceSystem crs = CRS.decode("EPSG:900913");
  // }
  // catch (FactoryException e)
  // {
  // e.printStackTrace();
  // }
  catch (final IOException e)
  {
    e.printStackTrace();
  }
}

private static void calculateMetaData(final InputStream stream,
    final MrsImagePyramidMetadata metadata, final String output, final boolean calcStats,
    final String protectionLevel, final boolean categorical,
    final boolean overridenodata) throws IOException
{
  metadata.setPyramid(output);

  final Properties mrgeoProperties = MrGeoProperties.getInstance();
  final int tilesize = Integer.parseInt(mrgeoProperties.getProperty(MrGeoConstants.MRGEO_MRS_TILESIZE, MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT));

  metadata.setTilesize(tilesize);

  if (categorical)
  {
    metadata.setClassification(Classification.Categorical);
  }
  else
  {
    metadata.setClassification(Classification.Continuous);
  }

  int tiletype = -1;

  final AbstractGridFormat format = GeotoolsRasterUtils.fastFormatFinder(stream);
  if (format == null || format.getClass() == UnknownFormat.class)
  {
    throw new IOException("Unknown geospatial input format for " + stream);
  }

  final AbstractGridCoverage2DReader reader = format.getReader(stream);
  if (reader == null)
  {
    throw new IOException("Error opening a reader for " + stream + " (" + format.getClass() + ")");
  }

  final GridCoverage2D image = getImageFromReader(reader, "EPSG:4326");

  // calculate zoom level for the image
  final GridGeometry2D geometry = image.getGridGeometry();

  int zoomlevel = 1;
  try
  {
    final Envelope2D pixelEnvelop = geometry.gridToWorld(new GridEnvelope2D(0, 0, 1, 1));

    final double pixelsizeLon = Math.abs(pixelEnvelop.width);
    final double pixelsizeLat = Math.abs(pixelEnvelop.height);

    final int zx = TMSUtils.zoomForPixelSize(pixelsizeLon, tilesize);
    final int zy = TMSUtils.zoomForPixelSize(pixelsizeLat, tilesize);

    zoomlevel = zx;
    if (zoomlevel < zy)
    {
      zoomlevel = zy;
    }
    metadata.setName(zoomlevel, "" + zoomlevel);

    if (metadata.getMaxZoomLevel() < zoomlevel)
    {
      metadata.setMaxZoomLevel(zoomlevel);
    }

  }
  catch (final TransformException e1)
  {
  }

  // expand the image bounds (for the metadata)
  Bounds bounds;

  final GeneralEnvelope imageBounds = (GeneralEnvelope) image.getEnvelope();
  if (imageBounds != null)
  {
    final DirectPosition lower = imageBounds.getLowerCorner();
    final DirectPosition upper = imageBounds.getUpperCorner();

    bounds = new Bounds(lower.getOrdinate(LON_DIMENSION), lower.getOrdinate(LAT_DIMENSION), upper
        .getOrdinate(LON_DIMENSION), upper.getOrdinate(LAT_DIMENSION));

    // System.out.println("###: " + bounds.toString());

    final TMSUtils.Pixel lowerPx = TMSUtils.latLonToPixels(lower.getOrdinate(LAT_DIMENSION),
        lower.getOrdinate(LON_DIMENSION), zoomlevel, tilesize);
    final TMSUtils.Pixel upperPx = TMSUtils.latLonToPixels(upper.getOrdinate(LAT_DIMENSION),
        upper.getOrdinate(LON_DIMENSION), zoomlevel, tilesize);

    metadata.setPixelBounds(zoomlevel, new LongRectangle(0, 0, upperPx.px - lowerPx.px,
        upperPx.py - lowerPx.py));
  }
  else
  {
    bounds = Bounds.world;
    metadata.setPixelBounds(zoomlevel, new LongRectangle(0, 0, 0, 0));
  }

  if (metadata.getBounds() == null)
  {
    metadata.setBounds(bounds);
  }
  else
  {
    bounds.expand(metadata.getBounds());
    metadata.setBounds(bounds);
  }

  metadata.setProtectionLevel(protectionLevel);
  // get band count and make sure all the images have the same number
  int bands = metadata.getBands();
  if (bands <= 0)
  {
    bands = image.getNumSampleDimensions();
    metadata.setBands(bands);
  }
  else if (bands != image.getNumSampleDimensions())
  {
    throw new IOException(
        "All images within the set need to have the same number of bands: " + stream + " has " +
            image.getNumSampleDimensions() + ", others have " + bands);
  }

  // get the nodata value

  // use reflection to see if we have a getMetadata() & getNodata() method.
  // these are from the geotiff reader specifically...
  Number nodata = null;

  final double[] defaults = metadata.getDefaultValues();
  if (defaults != null)
  {
    nodata = metadata.getDefaultValue(0);
  }

  try
  {
    Method m = reader.getClass().getMethod("getMetadata", new Class[] {});
    final Object meta = m.invoke(reader, new Object[] {});

    m = meta.getClass().getMethod("getNoData", new Class[] {});

    if (nodata == null)
    {
      nodata = (Number) m.invoke(meta, new Object[] {});

      log.debug("nodata: b: " + nodata.byteValue() + " d: " + nodata.doubleValue() + " f: " +
          nodata.floatValue() + " i: " + nodata.intValue() + " s: " + nodata.shortValue() + " l: " +
          nodata.longValue());
    }
    else if (!overridenodata)
    {
      final double n = (Double) m.invoke(meta, new Object[] {});

      if ((Double.isNaN(nodata.doubleValue()) != Double.isNaN(n)) ||
          (!Double.isNaN(nodata.doubleValue()) && (nodata.doubleValue() != n)))
      {
        throw new IOException(
            "All images within the set need to have the same nodata value: " + stream + " has " +
                m.invoke(meta, new Object[] {}) + ", others have " + nodata.toString());
      }
    }
  }
  catch (final NoSuchMethodException e)
  {
  }
  catch (final IllegalAccessException e)
  {
  }
  catch (final IllegalArgumentException e)
  {
  }
  catch (final InvocationTargetException e)
  {
  }

  if (nodata == null)
  {
    nodata = Double.NaN;
  }

  // final RenderedImage raster = image.getRenderedImage();
  final SampleModel model = image.getRenderedImage().getSampleModel();
  if (tiletype < 0)
  {
    tiletype = model.getDataType();
    metadata.setTileType(tiletype);
  }
  else if (tiletype != model.getDataType())
  {
    throw new IOException("All images within the set need to have the same tile type: " +
        stream + " has " + MrsImagePyramidMetadata.toTileTypeText(model.getDataType()) +
        ", others have " + MrsImagePyramidMetadata.toTileTypeText(tiletype));
  }


  if (calcStats)
  {
    final Map<Integer, List<Double>> minPixelValues = new HashMap<Integer, List<Double>>();
    final Map<Integer, List<Double>> maxPixelValues = new HashMap<Integer, List<Double>>();
    final Map<Integer, List<Double>> meanPixelValues = new HashMap<Integer, List<Double>>();

    // Initialize the min/max map lists for each band, if necessary
    if (minPixelValues.isEmpty())
    {
      for (int i = 0; i < bands; i++)
      {
        minPixelValues.put(i, new ArrayList<Double>());
        maxPixelValues.put(i, new ArrayList<Double>());
        meanPixelValues.put(i, new ArrayList<Double>());
      }
    }
    // set the stats to be calculated
    final Statistic[] statsParam = { Statistic.MIN, Statistic.MAX, Statistic.MEAN // ,
        // Statistic.APPROX_MEDIAN,
        // Statistic.SDEV
    };
    // set the bands to calc stats for
    final Integer[] bandsParam = new Integer[bands];
    for (int i = 0; i < bands; i++)
    {
      bandsParam[i] = i;
    }

    // exclude NODATA value
    final List<Range<Double>> excludeList = CollectionFactory.list();
    excludeList.add(Range.create(nodata.doubleValue(), true, nodata.doubleValue(), true));

    final OperationJAI op = new OperationJAI(new ZonalStatsDescriptor());
    final ParameterValueGroup params = op.getParameters();
    params.parameter("dataImage").setValue(image);
    params.parameter("stats").setValue(statsParam);
    params.parameter("bands").setValue(bandsParam);
    params.parameter("noDataRanges").setValue(excludeList);

    final GridCoverage2D coverage = (GridCoverage2D) op.doOperation(params, null);
    final ZonalStats zs = (ZonalStats) coverage
        .getProperty(ZonalStatsDescriptor.ZONAL_STATS_PROPERTY);

    for (final Result r : zs.results())
    {
      final int b = r.getImageBand();
      final Double v = r.getValue();
      switch (r.getStatistic())
      {
      case MIN:
        minPixelValues.get(b).add(v);
        break;
      case MAX:
        maxPixelValues.get(b).add(v);
        break;
      case MEAN:
        meanPixelValues.get(b).add(v);
        break;
      default:
        break;
      }
    }
    log.debug("first image, band 1 stats, min=" + minPixelValues.get(0).get(0) + ", max=" +
        maxPixelValues.get(0).get(0) + ", mean=" + meanPixelValues.get(0).get(0));

    final ImageStats[] stats = new ImageStats[bands];

    for (int i = 0; i < bands; i++)
    {
      // initialize the stats array
      stats[i] = new ImageStats(Double.MAX_VALUE, -Double.MAX_VALUE);
    }

    // set the min/max on each bands stats object
    // loop through the input images stats arrays
    for (final Integer b : minPixelValues.keySet())
    {
      final List<Double> mins = minPixelValues.get(b);
      final List<Double> maxs = maxPixelValues.get(b);
      final List<Double> means = meanPixelValues.get(b);
      // loop through the bands
      double meansum = 0;
      for (int m = 0; m < mins.size(); m++)
      {
        stats[b].min = Math.min(mins.get(m), stats[b].min);
        stats[b].max = Math.max(maxs.get(m), stats[b].max);
        meansum += means.get(m);
      }
      stats[b].mean = meansum / mins.size();
    }
    metadata.setImageStats(zoomlevel, stats);

    if (metadata.getMaxZoomLevel() == zoomlevel)
    {
      metadata.setStats(stats);
    }
  }

  final TMSUtils.Bounds bn = new TMSUtils.Bounds(bounds.getMinX(), bounds.getMinY(), bounds
      .getMaxX(), bounds.getMaxY());

  final TMSUtils.TileBounds tb = TMSUtils.boundsToTile(bn, zoomlevel, tilesize);
  metadata.setTileBounds(zoomlevel, new LongRectangle(tb.w, tb.s, tb.e, tb.n));

  final double defs[] = new double[bands];
  for (int i = 0; i < bands; i++)
  {
    // we'll save nodata as a double, so we can have NaN, but then other values for the other
    // datatypes
    if (nodata != null)
    {
      defs[i] = nodata.doubleValue();
    }
    else
    {
      defs[i] = Double.NaN;
    }
  }
  metadata.setDefaultValues(defs);

  try
  {
    GeotoolsRasterUtils.closeStreamFromReader(reader);
  }
  catch (Exception e)
  {
    e.printStackTrace();
  }
}

private static class DPx {
  public double px;
  public double py;

  public DPx(double x, double y)
  {
    this.px = x;
    this.py = y;
  }

  static DPx latLonToPixels(double lat, double lon, int zoom, int tilesize)
  {
    double res = TMSUtils.resolution(zoom, tilesize);
    return new DPx(((180.0 + lon) / res), ((90.0 + lat) / res));
  }

}

private static void loadFormats()
{
  orderInputStreamProviders();

  formats = new HashSet<AbstractGridFormat>();

  final Set<GridFormatFactorySpi> spis = GridFormatFinder.getAvailableFormats();
  for (final GridFormatFactorySpi spi : spis)
  {
    formats.add(spi.createFormat());
  }
}

public static void orderInputStreamProviders()
{
  // The input stream providers (SPIs) get created in no particular order. We
  // need to put our (mrgeo) providers at the front of the list, so we can be sure they will
  // be created first, instead of one choosing one of the other types.
  final IIORegistry registry = IIORegistry.getDefaultInstance();

  Iterator<ImageInputStreamSpi> providers = registry.getServiceProviders(
      ImageInputStreamSpi.class, true);

  // final all the mrgeo providers
  final List<ImageInputStreamSpi> mrgeoProviders = new LinkedList<ImageInputStreamSpi>();
  while (providers.hasNext())
  {
    final ImageInputStreamSpi spi = providers.next();

    if ((spi.getClass().getCanonicalName().startsWith("org.mrgeo")))
    {
      mrgeoProviders.add(spi);
    }
  }

  // now find all the other providers and set the mrgeo ones as preferred
  providers = registry.getServiceProviders(ImageInputStreamSpi.class, true);
  while (providers.hasNext())
  {
    final ImageInputStreamSpi spi = providers.next();

    if (!(spi.getClass().getCanonicalName().startsWith("org.mrgeo")))
    {
      for (final ImageInputStreamSpi mrgeo : mrgeoProviders)
      {
        registry.setOrdering(ImageInputStreamSpi.class, mrgeo, spi);
      }
    }
  }

//    providers = registry.getServiceProviders(ImageInputStreamSpi.class, true);
//    System.out.println("ImageInputStreams");
//    while (providers.hasNext())
//    {
//      final ImageInputStreamSpi spi = providers.next();
//      System.out.println("  " + spi.getClass().getCanonicalName());
//
//    }
}

private static InputStream openImageStream(String name) throws IOException
{

  Path path = new Path(name);

  final FileSystem fs = HadoopFileUtils.getFileSystem(path);

  if (fs.exists(path))
  {
    final InputStream stream = fs.open(path, 131072); // give open a 128K buffer

    Configuration localConf = HadoopUtils.createConfiguration();
    // see if were compressed
    final CompressionCodecFactory factory = new CompressionCodecFactory(localConf);
    final CompressionCodec codec = factory.getCodec(path);

    if (codec != null)
    {
      return new HadoopFileUtils.CompressedSeekableStream(codec.createInputStream(stream));
    }

    return stream;
  }

  throw new FileNotFoundException("File not found: " + path.toUri().toString());
}

}
