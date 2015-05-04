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
import org.geotools.coverage.grid.GridCoordinates2D;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.io.AbstractGridCoverage2DReader;
import org.geotools.geometry.DirectPosition2D;
import org.geotools.geometry.GeneralEnvelope;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.image.MrsImageException;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.image.MrsImagePyramidMetadata.Classification;
import org.mrgeo.image.geotools.GeotoolsRasterUtils;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.TMSUtils;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.media.jai.*;
import javax.media.jai.operator.ScaleDescriptor;
import java.awt.*;
import java.awt.image.Raster;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class IngestImageSplittingRecordReader extends RecordReader<TileIdWritable, RasterWritable>
{
  private static Logger log = LoggerFactory.getLogger(IngestImageSplittingRecordReader.class);

  private long currentTx;
  private long currentTy;

  private long endTx;
  private long endTy;

  private long minTx;
  //private long minTy;

  private long maxTx;
  //private long maxTy;

  private int zoomlevel;

  private float currentTile; // used for progress ONLY
  private float totalTiles; // used for progress ONLY

  private Raster raster = null;

  private int tilesize = -1;
  private Classification classification = null;
  private Double nodata = null;
  double[] defaults = null;

  private TileIdWritable key = new TileIdWritable();

  private RasterWritable value;

  // for TESTING ONLY
  static int tilecnt = 0;



  public IngestImageSplittingRecordReader()
  {
//    HdfsImageInputStreamSpi.orderInputStreamProviders();

  }

  @Override
  public void close()
  {
    // no op
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

    AbstractGridCoverage2DReader reader = GeotoolsRasterUtils.openImage(input);
    try
    {
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
        //minTy = b.getMinY();
        maxTx = b.getMaxX();
        //maxTy = b.getMaxY();

        currentTile = 0;
        totalTiles = isplit.getTotalTiles();

        // open the geotools raster
        GridCoverage2D geotoolsImage = GeotoolsRasterUtils.getImageFromReader(reader, "EPSG:4326");

        // get nodata's
        defaults = new double[geotoolsImage.getNumSampleDimensions()];
        Arrays.fill(defaults, nodata);

        // calculate the raster width and height
        final int ih = (int) geotoolsImage.getGridGeometry().getGridRange2D().getHeight();
        final int iw = (int) geotoolsImage.getGridGeometry().getGridRange2D().getWidth();

        // this code will fill in the src raster with the corresponding cell id.  It's useful for testing
        // to make sure the scaling/cropping are getting all the data correctly
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


        // get the input envelope
        GeneralEnvelope envelope = (GeneralEnvelope) geotoolsImage.getEnvelope();
        CoordinateReferenceSystem crs = geotoolsImage.getCoordinateReferenceSystem2D();

        TMSUtils.Bounds imageBounds = new TMSUtils.Bounds(
            envelope.getMinimum(GeotoolsRasterUtils.LON_DIMENSION),
            envelope.getMinimum(GeotoolsRasterUtils.LAT_DIMENSION),
            envelope.getMaximum(GeotoolsRasterUtils.LON_DIMENSION),
            envelope.getMaximum(GeotoolsRasterUtils.LAT_DIMENSION));

        // calculate pixel h/w
        double pw = envelope.getSpan(GeotoolsRasterUtils.LON_DIMENSION) / iw;
        double ph = envelope.getSpan(GeotoolsRasterUtils.LON_DIMENSION) / ih;


        // calculate the lat/lon bounds of the input split ( tiles)
        TMSUtils.Bounds tiletl = TMSUtils.tileBounds(b.getMinX(), endTy, zoomlevel, tilesize);
        TMSUtils.Bounds tilelr = TMSUtils.tileBounds(b.getMaxX(), currentTy, zoomlevel, tilesize);

        TMSUtils.Bounds tileBounds = new TMSUtils.Bounds(tiletl.w, tilelr.s, tilelr.e, tiletl.n);

        // calculate the mrgeo pixel space for the input tile bounds
        DPx tilePxTl = DPx.latLonToPixels(tileBounds.n, tileBounds.w, zoomlevel, tilesize);
        //DPx tilePxLr = DPx.latLonToPixels(tileBounds.s, tileBounds.e, zoomlevel, tilesize);

        // final raster h/w
        int rasterw = (int)(b.getMaxX() - b.getMinX() + 1) * tilesize;
        int rasterh = (int)(endTy - currentTy + 1) * tilesize;


        try
        {
          GridGeometry2D grid = geotoolsImage.getGridGeometry();

          // calculate the source image (pixel) values for the tiles to read
          GridCoordinates2D tl = grid.worldToGrid(
              new DirectPosition2D(crs, tileBounds.w + pw / 2, tileBounds.n - ph / 2));
          GridCoordinates2D lr = grid.worldToGrid(
              new DirectPosition2D(crs, tileBounds.e + pw / 2, tileBounds.s - ph / 2));


          // image in mrgeo pixel space
          DPx imagePxTl = DPx.latLonToPixels(imageBounds.n, imageBounds.w, zoomlevel, tilesize);
          //DPx imagePxLr = DPx.latLonToPixels(imageBounds.s, imageBounds.e, zoomlevel, tilesize);

          float xlatex = (float)(imagePxTl.px - tilePxTl.px);
          float xlatey = (float)(tilePxTl.py - imagePxTl.py);

          float scalex = (float)(rasterw) / (lr.x - tl.x);
          float scaley = (float)(rasterh) / (lr.y - tl.y);

          Interpolation interp;
          if (classification != null && classification == Classification.Categorical)
          {
            interp = Interpolation.getInstance(Interpolation.INTERP_NEAREST);
          }
          else
          {
            interp = Interpolation.getInstance(Interpolation.INTERP_BILINEAR);
          }

          // crop the image to the input area
          GridCoverage2D geotoolsCropped = GeotoolsRasterUtils.crop(geotoolsImage, tileBounds.w, tileBounds.s,
              tileBounds.e, tileBounds.n);
          PlanarImage geotoolsPlanar = (PlanarImage) geotoolsCropped.getRenderedImage();

          // scale the image to mrgeo pixels, filling with nodata if needed
          RenderingHints qualityHints = new RenderingHints(RenderingHints.KEY_RENDERING,
              RenderingHints.VALUE_RENDER_QUALITY);
          qualityHints.put(JAI.KEY_BORDER_EXTENDER, BorderExtender.createInstance(BorderExtender.BORDER_COPY));

          BorderExtenderConstant extender = new BorderExtenderConstant(defaults);

          PlanarImage scaled =  ScaleDescriptor.create(geotoolsPlanar, scalex, scaley, xlatex, xlatey,
              interp, qualityHints);

          // make the final raster
          Rectangle imageRect = new Rectangle(0, 0, rasterw, rasterh);
          raster = scaled.getExtendedData(imageRect, extender).createTranslatedChild(0, 0);

          // this will save the pieces
//          try
//          {
//            File file = new File("/data/export/scaled-raw-" + cnt + ".tiff");
//            ImageUtils.writeImageToFile(file, ImageUtils.createImageWriter("raster/tiff"),
//                RasterUtils.makeBufferedImage(raster));
//
//            final GeneralEnvelope env = new GeneralEnvelope(new double[]{tileBounds.w, tileBounds.s},
//                new double[]{tileBounds.e, tileBounds.n});
//            env.setCoordinateReferenceSystem(geotoolsImage.getCoordinateReferenceSystem());
//            GeotoolsRasterUtils.saveLocalGeotiff("/data/export/src-" + cnt + ".tiff", geotoolsImage, nodata);
//            GeotoolsRasterUtils.saveLocalGeotiff("/data/export/scaled-" + cnt + ".tiff",
//                RasterUtils.makeRasterWritable(scaled.getData()), (GeneralEnvelope) geotoolsImage.getEnvelope(), nodata);
//            GeotoolsRasterUtils.saveLocalGeotiff("/data/export/final-" + cnt + ".tiff",
//                RasterUtils.makeRasterWritable(raster), env, nodata);
//
//            System.out.println("cropped: x: " + scaled.getMinX() + " y: " + scaled.getMinY() + " rasterw: " +
//                scaled.getWidth() + " rasterh: " + scaled.getHeight());
//            System.out.println("raster: x: " + raster.getMinX() + " y: " + raster.getMinY() + " rasterw: " +
//                raster.getWidth() + " rasterh: " + raster.getHeight());
//
//          }
//          catch (IOException e)
//          {
//            e.printStackTrace();
//          }

        }
        catch (TransformException e)
        {
          throw new IOException(e);
        }
      }
    }
    finally
    {
      try
      {
        GeotoolsRasterUtils.closeStreamFromReader(reader);
      }
      catch (Exception e)
      {
        throw new RuntimeException(e);
      }
    }
  }

  static int cnt = 1;

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



  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException
  {
    long startTime = System.currentTimeMillis();

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

    final Raster cropped = RasterUtils.crop(raster, currentTx, currentTy, minTx, endTy, tilesize);

    // save the tile...
//    try
//    {
//      GeotoolsRasterUtils.saveLocalGeotiff("/data/export/tile-" + cnt, cropped,
//          currentTx, currentTy, zoomlevel, tilesize, nodata);
//    }
//    catch (IOException | FactoryException e)
//    {
//      e.printStackTrace();
//    }

    if (cropped.getMinX() != 0 || cropped.getMinY() != 0 ||
        cropped.getWidth() != tilesize || cropped.getHeight() != tilesize)
    {
      throw new IOException("Tile not the right position/size: x"  + cropped.getMinX() + " y: " + cropped.getMinY() +
          " w: " + cropped.getWidth() + " h: " + cropped.getHeight());
    }

    key = new TileIdWritable(TMSUtils.tileid(currentTx, currentTy, zoomlevel));
    value = RasterWritable.toWritable(cropped);

    currentTx++;
    currentTile++;

    cnt++;

    long endTime = System.currentTimeMillis();
    System.out.println("Tile read time: " + (endTime - startTime));
    return true;
  }

}
