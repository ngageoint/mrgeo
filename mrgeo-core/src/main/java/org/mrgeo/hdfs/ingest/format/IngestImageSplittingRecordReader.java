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

package org.mrgeo.hdfs.ingest.format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.geotools.coverage.grid.*;
import org.geotools.coverage.grid.io.AbstractGridCoverage2DReader;
import org.geotools.coverage.processing.Operations;
import org.geotools.geometry.DirectPosition2D;
import org.geotools.geometry.GeneralDirectPosition;
import org.geotools.geometry.GeneralEnvelope;
import org.geotools.referencing.CRS;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.image.MrsImageException;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.image.MrsImagePyramidMetadata.Classification;
import org.mrgeo.image.geotools.GeotoolsRasterUtils;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.ImageUtils;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.TMSUtils;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.awt.image.ToolkitImage;

import javax.imageio.ImageIO;
import javax.measure.unit.SI;
import javax.media.jai.*;
import javax.media.jai.operator.ScaleDescriptor;
import java.awt.*;
import java.awt.geom.AffineTransform;
import java.awt.image.*;
import java.awt.image.renderable.ParameterBlock;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

public class IngestImageSplittingRecordReader extends RecordReader<TileIdWritable, RasterWritable>
{
  private static Logger log = LoggerFactory.getLogger(IngestImageSplittingRecordReader.class);

  private long currentTx;
  private long currentTy;

  private long endTx;
  private long endTy;

  private long minTx;
  private long minTy;

  private long maxTx;
  private long maxTy;

  private int zoomlevel;

  private float currentTile; // used for progress ONLY
  private float totalTiles; // used for progress ONLY

  private AbstractGridCoverage2DReader reader = null;
  private GridCoverage2D geotoolsImage = null;
  private PlanarImage image = null;
  private BorderExtenderConstant extender = null;
  private TMSUtils.Bounds imageBounds = null;
  private double pw = Double.NaN;
  private double ph = Double.NaN;
  private double tw = Double.NaN;
  private double th = Double.NaN;
  private double res = Double.NaN;
  private int iw = 0;
  private int ih = 0;

  private int startpx = 0;
  private int startpy = 0;

  private int tilesize = -1;
  private Classification classification = null;
  private Double nodata = null;
  double[] defaults = null;

  private TileIdWritable key = new TileIdWritable();

  private RasterWritable value;

  // for TESTING ONLY
  static int tilecnt = 0;

  private class DPx {
    public double px;
    public double py;

    public DPx(double x, double y)
    {
      this.px = x;
      this.py = y;
    }
  }

  public IngestImageSplittingRecordReader()
  {
//    HdfsImageInputStreamSpi.orderInputStreamProviders();

  }
  @Override
  public void close()
  {
    try
    {
      GeotoolsRasterUtils.closeStreamFromReader(reader);
    }
    catch (final Exception e)
    {
      throw new RuntimeException(e);
    }
  }

  @Override
  public TileIdWritable getCurrentKey() throws IOException, InterruptedException
  {
    return key;
  }

  @Override
  public RasterWritable getCurrentValue() throws IOException, InterruptedException
  {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException
  {
    return currentTile / totalTiles;
  }

  @Override
  public void initialize(final InputSplit split, final TaskAttemptContext context)
      throws IOException, InterruptedException
  {

    if (!(split instanceof IngestImageSplit))
    {
      throw new IOException(
          "InputSplit for IngestImageRecordReader needs to be (or derived from) IngestImageSplit");
    }

    final IngestImageSplit isplit = (IngestImageSplit) split;

    final Configuration conf = context.getConfiguration();
    try
    {
      //metadata = HadoopUtils.getMetadata(conf);
      Map<String, MrsImagePyramidMetadata> meta = HadoopUtils.getMetadata(context.getConfiguration());
      if (!meta.isEmpty())
      {
        MrsImagePyramidMetadata metadata =  meta.values().iterator().next();
        tilesize = metadata.getTilesize();
        classification = metadata.getClassification();
        nodata = metadata.getDefaultValueDouble(0);
      }
    }
    catch (ClassNotFoundException e)
    {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    if (tilesize < 0)
    {
      tilesize = conf.getInt("tilesize", -1);
      if (tilesize < 0)
      {
        throw new MrsImageException(
            "Error, no \"tilesize\" or \"metadata\" parameter in configuration, tilesize needs to be calculated & set before map/reduce");
      }
    }

    if (classification == null)
    {
      String cl = conf.get("classification", null);
      if (cl == null)
      {
        throw new MrsImageException(
            "Error, no \"classification\" or \"metadata\" parameter in configuration, classification needs to be calculated & set before map/reduce");
      }
      classification = Classification.valueOf(cl);
    }

    if (nodata == null)
    {
      String nd = conf.get("nodata", null);
      if (nd == null)
      {
        throw new MrsImageException(
            "Error, no \"nodata\" or \"metadata\" parameter in configuration, nodata needs to be calculated & set before map/reduce");
      }
      nodata = Double.parseDouble(nd);
    }


    zoomlevel = isplit.getZoomlevel();

    final String input = isplit.getFilename();
    log.info("processing: " + input);

    reader = GeotoolsRasterUtils.openImage(input);

    if (zoomlevel < 1)
    {
      try
      {
        zoomlevel = GeotoolsRasterUtils.calculateZoomlevel(reader, tilesize);
      }
      catch (final Exception e)
      {
        throw new IOException("Error calculating zoomlevel", e);
      }
    }

    if (reader != null)
    {
      currentTx = isplit.getStartTx();
      currentTy = isplit.getStartTy();

      endTx = isplit.getEndTx();
      endTy = isplit.getEndTy();

      final LongRectangle b = isplit.getImageBounds();
      minTx = b.getMinX();
      minTy = b.getMinY();
      maxTx = b.getMaxX();
      maxTy = b.getMaxY();

      currentTile = 0;
      totalTiles = isplit.getTotalTiles();

      geotoolsImage = GeotoolsRasterUtils.getImageFromReader(reader, "EPSG:4326");

      defaults = new double[geotoolsImage.getNumSampleDimensions()];
      Arrays.fill(defaults, nodata);

      extender = new BorderExtenderConstant(defaults);

//      int iiw = (int) geotoolsImage.getGridGeometry().getGridRange2D().getWidth();
//      int iih = (int) geotoolsImage.getGridGeometry().getGridRange2D().getHeight();
//
//
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
      imageBounds = new TMSUtils.Bounds(
          envelope.getMinimum(GeotoolsRasterUtils.LON_DIMENSION),
          envelope.getMinimum(GeotoolsRasterUtils.LAT_DIMENSION),
          envelope.getMaximum(GeotoolsRasterUtils.LON_DIMENSION),
          envelope.getMaximum(GeotoolsRasterUtils.LAT_DIMENSION));


      ih = (int) geotoolsImage.getGridGeometry().getGridRange2D().getHeight();
      iw = (int) geotoolsImage.getGridGeometry().getGridRange2D().getWidth();

      pw = imageBounds.width() / iw;
      ph = imageBounds.height() / ih;

      TMSUtils.Bounds tileBounds = TMSUtils.tileBounds(minTx, endTy, zoomlevel, tilesize);

      tw = tileBounds.width() / pw;
      th = tileBounds.height() / ph;


      tileBounds = TMSUtils.tileBounds(imageBounds, zoomlevel, tilesize);

      res = TMSUtils.resolution(zoomlevel, tilesize);

      DPx tp = latLonToPixels(tileBounds.n, tileBounds.w, zoomlevel, tilesize);
      DPx ip = latLonToPixels(imageBounds.n, imageBounds.w, zoomlevel, tilesize);
      DPx ep = latLonToPixels(imageBounds.s, imageBounds.e, zoomlevel, tilesize);

//      float xlatex = (float) (ip.px - tp.px - 0.5);
//      float xlatey = (float) (tp.py - ip.py - 0.5);
//
//      float scalex = (float)(ep.px - ip.px + 1.5) / (iw);
//      float scaley = (float)(ip.py - ep.py + 1.5) / (ih);

      float xlatex = (float) (ip.px - tp.px);
      float xlatey = (float) (tp.py - ip.py);

      float scalex = (float)(ep.px - ip.px) / (iw);
      float scaley = (float)(ip.py - ep.py) / (ih);


      // take care of categorical data
      Interpolation interp;
      if (classification == Classification.Categorical)
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


      PlanarImage pi = (PlanarImage)ScaleDescriptor.create(geotoolsImage.getRenderedImage(),
          scalex, scaley, xlatex, xlatey, interp, qualityHints);


      image = pi;

//      System.out.println(
//          "image: x: " + image.getMinX() + " y: " + image.getMinY() + " w: " + image.getWidth() + " h: " +
//              image.getHeight());


    }
  }

  private DPx latLonToPixels(double lat, double lon, int zoom, int tilesize)
  {
    return new DPx(((180.0 + lon) / res), ((90.0 + lat) / res));
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException
  {
    // long startTime = System.currentTimeMillis();

    if (currentTx > maxTx)
    {
      currentTx = minTx;
      currentTy++;
    }

    // one row too high...
    if (currentTy > endTy)
    {
      return false;
    }
    // on the end row, and past the last tx;
    else if (currentTy == endTy && currentTx > endTx)
    {
      return false;
    }

    final Raster raster = cutTile();

//    boolean categorical = (classification == Classification.Categorical);
//
//    final Raster r = GeotoolsRasterUtils.cutTile(geotoolsImage, currentTx, currentTy, zoomlevel,
//        tilesize, defaults, categorical);

    key = new TileIdWritable(TMSUtils.tileid(currentTx, currentTy, zoomlevel));
    value = RasterWritable.toWritable(raster);

    currentTx++;
    currentTile++;

    // long endTime = System.currentTimeMillis();
    // System.out.println("Tile read time: " + (endTime - startTime));
    return true;
  }

  static int cnt = 1;

  private Raster cutTile()
  {

    TMSUtils.Bounds tileBounds = TMSUtils.tileBounds(currentTx, currentTy, zoomlevel, tilesize);

    TMSUtils.Pixel tileUL = new TMSUtils.Pixel(
        (int) (((tileBounds.w - imageBounds.w) / pw) - 0.5),
        (int) (-((tileBounds.n - imageBounds.n) / ph) - 0.5));

    int dtx = (int) (currentTx - minTx);
    int dty = (int) (maxTy - currentTy);

//    int dpx = dtx * (int)tw;
//    int dpy = dty * (int)th;
//
//    int x = startpx + dpx;
//    int y = startpy + dpy;

    int x = dtx * tilesize;
    int y = dty * tilesize;

    WritableRaster tile = null;

    //System.out.println("cnt: " + cnt + " ulx: " + tileUL.px + " uly: " + tileUL.py + " llx: " + (int)(tileUL.px + tw - 1) + " lly: " + (int)(tileUL.py + th - 1));

    //Rectangle cropRect = new Rectangle((int) tileUL.px, (int) tileUL.py, (int) tw, (int) th);
    //Rectangle cropRect = new Rectangle(x, y, (int) tw, (int) th);
    Rectangle cropRect = new Rectangle(x, y, tilesize, tilesize);

    // crop, and fill the extra data with nodatas
    Raster cropped = image.getExtendedData(cropRect, extender).createTranslatedChild(0, 0);

    // The crop has the potential to make sample models sizes that aren't identical, to this will force them to all be the
    // same
    final SampleModel model = cropped.getSampleModel().createCompatibleSampleModel(tilesize, tilesize);
    tile = Raster.createWritableRaster(model, null);
    tile.setDataElements(0, 0, cropped);


//    System.out.println("***");
//    for (int i = 0; i < tile.getHeight(); i++)
//    {
//      for (int j = 0; j < tile.getWidth(); j++)
//      {
//        if (tile.getSample(i, j, 0) != nodata)
//        {
//          System.out.println(j + " " + i + " " + tile.getSample(i, j, 0));
//        }
//      }
//    }

//    System.out.println("cnt: " + cnt + " id: " + TMSUtils.tileid(currentTx, currentTy, zoomlevel) +
//        " tx: " + currentTx + " ty: " + currentTy +
//        " ulx: " + x + " uly: " + y +
//        " llx: " + (x + tilesize - 1) + " lly: " + (y + tilesize - 1) +
//        " tile: x: " + tile.getMinX() + " y: " + tile.getMinY() + " w: " + tile.getWidth() + " h: " + tile.getHeight());

//    File file = new File("/data/export/cropped" + cnt + ".tiff");
//    try
//    {
//      ImageUtils.writeImageToFile(file, ImageUtils.createImageWriter("image/tiff"), RasterUtils.makeBufferedImage(cropped));
//    }
//    catch (IOException e)
//    {
//      e.printStackTrace();
//    }

    //if (cnt == 3)
//      if (categorical)
//      {
//        tile = RasterUtils.scaleRasterNearest(cropped, tilesize, tilesize);
//      }
//      else
//      {
//        Number nodata = Double.NaN;
//        if (defaults != null)
//        {
//          nodata = defaults[0];
//        }
//
//        tile = RasterUtils.scaleRasterInterp(cropped, tilesize, tilesize, nodata);
//
//      }
//    else
//    {
//      tile = cropped.createCompatibleWritableRaster(tilesize, tilesize);
//      RasterUtils.fillWithNodata(tile, nodata.doubleValue());
//    }

//    WritableRaster t = RasterUtils.scaleRasterInterp(tile, (int)tw, (int)th, 0, true, true);
//
//    file = new File("/data/export/smaller" + cnt + ".tiff");
//    try
//    {
//      ImageUtils.writeImageToFile(file,ImageUtils.createImageWriter("image/tiff"),
//          RasterUtils.makeBufferedImage(t));
//    }
//    catch (IOException e)
//    {
//      e.printStackTrace();
//    }

//    file = new File("/data/export/scaled" + cnt + ".tiff");
//    try
//    {
//      //ImageUtils.writeImageToFile(file,ImageUtils.createImageWriter("image/tiff"), RasterUtils.makeBufferedImage(tile));
//      GeotoolsRasterUtils.saveLocalGeotiff("/data/export/tile" + cnt, tile, currentTx, currentTy, zoomlevel, tilesize, Double.NaN);
//      //GeotoolsRasterUtils.saveLocalGeotiff("/data/export/cropped-tile" + cnt, cropped, currentTx, currentTy, zoomlevel, tilesize, Double.NaN);
//    }
//    catch (IOException | FactoryException e)
//    {
//      e.printStackTrace();15/04/28 13:17:34 INFO format.IngestImageSplittingRecordReader: processing: hdfs://localhost:8020/user/tim.tisler/santiago-raw-aster/ASTGTM2_S33W071_dem.tif

//    }

    cnt++;


    return tile;
//    else
//    {
//      // edge tile
//      int ulx = Math.max((int) tileUL.px, 0);
//      int uly = Math.max((int) tileUL.py, 0);
//
//      int lrx = Math.min((int) (tileUL.px + tw), iw);
//      int lry = Math.min((int) (tileUL.py + th), ih);
//
//      int cw = (lrx - ulx) + 1;
//      int ch = (lry - uly) + 1;
//
//      Rectangle testRect = new Rectangle((int) tileUL.px, (int)tileUL.py, (int)tw, (int)th);
//      Raster test = image.getData(testRect).createTranslatedChild(0, 0);
//
//      test = RasterUtils.scaleRasterInterp(test, tilesize, tilesize, nodata, true, true);
//      try
//      {
//        GeotoolsRasterUtils.saveLocalGeotiff("/data/export/test" + cnt++, test, currentTx, currentTy, zoomlevel, tilesize, Double.NaN);
//      }
//      catch (IOException | FactoryException e)
//      {
//        e.printStackTrace();
//      }
//
//      Rectangle cropRect = new Rectangle(ulx, uly, cw, ch);
//      Raster cropped = image.getData(cropRect);
//
//      int dstHeight = (int) (tilesize * ((double)(ch) / th) + 0.5);
//      int dstWidth = (int) (tilesize * ((double)(cw) / tw) + 0.5);
//
//      Raster scaled;
//      if (categorical)
//      {
//        scaled = RasterUtils.scaleRasterNearest(cropped, dstWidth, dstHeight);
//      }
//      else
//      {
//        Number nodata = Double.NaN;
//        if (defaults != null)
//        {
//          nodata = defaults[0];
//        }
//
//        boolean includeRight = lrx == iw;
//        boolean includeBottom = lry == ih;
//
//        //scaled = RasterUtils.scaleRasterInterp(cropped, dstWidth, dstHeight, nodata, includeRight, includeBottom);
//        scaled = RasterUtils.scaleRasterInterp(cropped, dstWidth, dstHeight, nodata, true, true);
//      }
//
//      tile = cropped.createCompatibleWritableRaster(tilesize, tilesize);
//      RasterUtils.fillWithNodata(tile, nodata.doubleValue());
//
//      final Object data = scaled.getDataElements(scaled.getMinX(), scaled.getMinY(), scaled
//          .getWidth(), scaled.getHeight(), null);
//
//      TMSUtils.Pixel pixelUL = new TMSUtils.Pixel(
//          (int) (-(tileBounds.w - imageBounds.w) / res),
//          (int) ((tileBounds.n - imageBounds.n) / res));
//
//      int tx = Math.max((int) pixelUL.px, 0);
//      int ty = Math.max((int) pixelUL.py, 0);
//
//      if (tx == 91) tx = 93;
//      if (tx == 333) tx = 332;
//      System.out.println(" x: " + tx + " y: " + ty + " w: " + dstWidth + " h: " + dstHeight);
//      tile.setDataElements(tx, ty, dstWidth, dstHeight, data);
//    }
//
//    return tile;

  }

}
