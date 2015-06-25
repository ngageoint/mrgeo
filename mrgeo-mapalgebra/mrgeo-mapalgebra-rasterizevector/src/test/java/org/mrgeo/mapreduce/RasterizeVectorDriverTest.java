package org.mrgeo.mapreduce;

import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.geometry.GeometryFactory;
import org.mrgeo.geometry.WritableGeometry;
import org.mrgeo.image.geotools.GeotoolsRasterUtils;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.TMSUtils;
import org.opengis.referencing.FactoryException;

import java.awt.image.Raster;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RasterizeVectorDriverTest extends LocalRunnerTest
{
  private static List<GeometryWritable> features;
  private static List<GeometryWritable> polygonWithInnerRing;
  private static List<GeometryWritable> multiPolygonsWithHolesFirst;
  private static List<GeometryWritable> multiPolygonsWithHolesLast;
  private static int tileSize = MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT;
  private static int zoom = 1;
  // The following coordinates must match the coordinates used in the POLYGON WKT definitions
  // used below for the features being painted.
  private static double minX1 = 12.0;
  private static double minY1 = 12.0;
  private static double maxX1 = 65.0;
  private static double maxY1 = 65.0;
  private static double minX2 = 14.0;
  private static double minY2 = 14.0;
  private static double maxX2 = 48.0;
  private static double maxY2 = 48.0;
  private static double minX3 = 10.0;
  private static double minY3 = 10.0;
  private static double maxX3 = 60.0;
  private static double maxY3 = 30.0;
  private static double minXinner1 = 20.0;
  private static double minYinner1 = 20.0;
  private static double maxXinner1 = 25.0;
  private static double maxYinner1 = 42.0;
  private static double minXinner2 = 28.0;
  private static double minYinner2 = 20.0;
  private static double maxXinner2 = 42.0;
  private static double maxYinner2 = 42.0;
  private static double minXOverlapping = 10;
  private static double minYOverlapping = 10;
  private static double maxXOverlapping = 60;
  private static double maxYOverlapping = 30;
  private static long tx = 1;
  private static long ty = 0;

  @BeforeClass
  public static void init() throws ParseException
  {
    features = new ArrayList<GeometryWritable>();
    polygonWithInnerRing = new ArrayList<GeometryWritable>();
    multiPolygonsWithHolesFirst = new ArrayList<GeometryWritable>();
    multiPolygonsWithHolesLast = new ArrayList<GeometryWritable>();

    final WKTReader reader = new WKTReader();

    String baseGeom = "POLYGON ((12 12,12 65,65 65,65 12,12 12))";
    WritableGeometry geom = GeometryFactory.fromJTS(reader.read(baseGeom));
    geom.setAttribute("population", Integer.toString(200));
    features.add(new GeometryWritable(geom));

    baseGeom = "POLYGON ((14 14,14 48,48 48,48 14,14 14))";
    geom = GeometryFactory.fromJTS(reader.read(baseGeom));
    geom.setAttribute("population", Integer.toString(300));
    features.add(new GeometryWritable(geom));

    // Construct a polygon with two inner rings (holes)
    String wktHoleyPolygon = "POLYGON ((14 14,14 48,48 48,48 14,14 14),(20 20, 20 42, 25 42, 25 20, 20 20),(28 20, 28 42, 42 42, 42 20, 28 20))";
    WritableGeometry holeyPolygon = GeometryFactory.fromJTS(reader.read(wktHoleyPolygon));
    holeyPolygon.setAttribute("population", Integer.toString(300));

    // Construct a polygon that overlaps the lower part of holeyPolygon
    String wktOverlappingPolygon = "POLYGON ((10 10,10 30,60 30,60 10,10 10))";
    WritableGeometry overlappingPolygon = GeometryFactory.fromJTS(reader.read(wktOverlappingPolygon));
    overlappingPolygon.setAttribute("population", Integer.toString(300));

    // Now build the feature lists for testing the rasterization
    polygonWithInnerRing.add(new GeometryWritable(holeyPolygon));

    multiPolygonsWithHolesFirst.add(new GeometryWritable(holeyPolygon));
    multiPolygonsWithHolesFirst.add(new GeometryWritable(overlappingPolygon));

    multiPolygonsWithHolesLast.add(new GeometryWritable(overlappingPolygon));
    multiPolygonsWithHolesLast.add(new GeometryWritable(holeyPolygon));
  }
  
  @Before
  public void setUp()
  {
    conf.setBoolean(RasterizeVectorDriver.TEST_REDUCER, true);
    conf.set(FeatureToTilesMapper.ZOOM, "1");
    conf.set(FeatureToTilesMapper.TILE_SIZE, MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT);
    conf.set(RasterizeVectorPainter.ZOOM, "1");
    conf.set(RasterizeVectorPainter.TILE_SIZE, MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT);
    conf.setBoolean("skip.stats", true);

  }


  private static void compareRasters(final Raster raster, final double first, final double second)
  {
    for (int x = 0; x < raster.getWidth(); x++)
    {
      for (int y = 0; y < raster.getHeight(); y++)
      {
        for (int band = 0; band < raster.getNumBands(); band++)
        {
          final double v = raster.getSampleDouble(x, y, band);

          if (x >= 34 && x <= 184)
          {
            if (y >= 71 && y <= 221)
            {
              if (x >= 40 && x <= 135)
              {
                if (y >= 119 && y <= 215)
                {
                  Assert.assertEquals("Unexpected value: px: " + x + " py: " + y, second, v,
                      0.0000001);
                  continue;
                }
                Assert
                    .assertEquals("Unexpected value: px: " + x + " py: " + y, first, v, 0.0000001);
              }
              continue;
            }
          }
          Assert.assertEquals("Unexpected value: px: " + x + " py: " + y + " (is not nan)",
              Double.NaN, v, 0.0000001);
        }
      }
    }
  }

  // Check the output when rasterizing the holey polygon
  private static void compareRastersForHoley(final Raster raster, final double maskValue)
  {
    try
    {
      GeotoolsRasterUtils.saveLocalGeotiff("/export/home/dave.johnson/out.tif", raster, tx, ty, zoom, tileSize, Double.NaN);
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    catch (FactoryException e)
    {
      System.out.println("Unable to save raster " + e);
    }
    TMSUtils.Pixel outerLL = TMSUtils.latLonToTilePixelUL(minY2, minX2, tx, ty, zoom, tileSize);
    TMSUtils.Pixel outerUR = TMSUtils.latLonToTilePixelUL(maxY2, maxX2, tx, ty, zoom, tileSize);
    TMSUtils.Pixel innerLL1 = TMSUtils.latLonToTilePixelUL(minYinner1, minXinner1, tx, ty, zoom, tileSize);
    TMSUtils.Pixel innerUR1 = TMSUtils.latLonToTilePixelUL(maxYinner1, maxXinner1, tx, ty, zoom, tileSize);
    TMSUtils.Pixel innerLL2 = TMSUtils.latLonToTilePixelUL(minYinner2, minXinner2, tx, ty, zoom, tileSize);
    TMSUtils.Pixel innerUR2 = TMSUtils.latLonToTilePixelUL(maxYinner2, maxXinner2, tx, ty, zoom, tileSize);
//    System.out.println("outerLL = " + outerLL.toString());
//    System.out.println("outerUR = " + outerUR.toString());
//    System.out.println("innerLL1 = " + innerLL1.toString());
//    System.out.println("innerUR1 = " + innerUR1.toString());
//    System.out.println("innerLL2 = " + innerLL2.toString());
//    System.out.println("innerUR2 = " + innerUR2.toString());
    // The following rectangles were generated after visually inspecting the generated raster
    // and the actual polygon in QGIS, It seems that the paint operation has some sort of
    // round-off when converting lat/lon coordinates to pixels, hence the need to increment
    // some coordinates by one and decrement others by one.
    LongRectangle outer = new LongRectangle(outerLL.px + 1, outerUR.py, outerUR.px - 1, outerLL.py - 1);
    LongRectangle inner1 = new LongRectangle(innerLL1.px + 1, innerUR1.py, innerUR1.px - 1, innerLL1.py - 1);
    LongRectangle inner2 = new LongRectangle(innerLL2.px, innerUR2.py, innerUR2.px - 1, innerLL2.py - 1);
    for (int x = 0; x < raster.getWidth(); x++)
    {
      for (int y = 0; y < raster.getHeight(); y++)
      {
        for (int band = 0; band < raster.getNumBands(); band++)
        {
          final double v = raster.getSampleDouble(x, y, band);

//          if (!Double.isNaN(v))
//          {
//            System.out.println(x + ", " + y + " = " + v);
//          }
          if (outer.contains(x, y))
          {
            if (inner1.contains(x, y))
            {
              Assert.assertTrue("Expected NaN for value at: px: " + x + " py: " + y + ", not " + v, Double.isNaN(v));
            }
            else if (inner2.contains(x, y))
            {
              Assert.assertTrue("Expected NaN for value at: px: " + x + " py: " + y + ", not " + v, Double.isNaN(v));
            }
            else
            {
              Assert.assertEquals("Unexpected value: px: " + x + " py: " + y, maskValue, v, 0.0000001);
            }
          }
          else
          {
            Assert.assertTrue("Expected NaN for value at: px: " + x + " py: " + y + ", not " + v, Double.isNaN(v));
          }
        }
      }
    }
  }

  private static void compareRastersForOverlappingHoleyFirst(final Raster raster, final double maskValue)
  {
    try
    {
      GeotoolsRasterUtils.saveLocalGeotiff("/export/home/dave.johnson/out.tif", raster, tx, ty, zoom, tileSize, Double.NaN);
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    catch (FactoryException e)
    {
      System.out.println("Unable to save raster " + e);
    }
    TMSUtils.Pixel outerLL = TMSUtils.latLonToTilePixelUL(minY2, minX2, tx, ty, zoom, tileSize);
    TMSUtils.Pixel outerUR = TMSUtils.latLonToTilePixelUL(maxY2, maxX2, tx, ty, zoom, tileSize);
    TMSUtils.Pixel innerLL1 = TMSUtils.latLonToTilePixelUL(minYinner1, minXinner1, tx, ty, zoom, tileSize);
    TMSUtils.Pixel innerUR1 = TMSUtils.latLonToTilePixelUL(maxYinner1, maxXinner1, tx, ty, zoom, tileSize);
    TMSUtils.Pixel innerLL2 = TMSUtils.latLonToTilePixelUL(minYinner2, minXinner2, tx, ty, zoom, tileSize);
    TMSUtils.Pixel innerUR2 = TMSUtils.latLonToTilePixelUL(maxYinner2, maxXinner2, tx, ty, zoom, tileSize);
    TMSUtils.Pixel secondFeatureLL = TMSUtils.latLonToTilePixelUL(minY3, minX3, tx, ty, zoom, tileSize);
    TMSUtils.Pixel secondFeatureUR = TMSUtils.latLonToTilePixelUL(maxY3, maxX3, tx, ty, zoom, tileSize);
//    System.out.println("outerLL = " + outerLL.toString());
//    System.out.println("outerUR = " + outerUR.toString());
//    System.out.println("innerLL1 = " + innerLL1.toString());
//    System.out.println("innerUR1 = " + innerUR1.toString());
//    System.out.println("innerLL2 = " + innerLL2.toString());
//    System.out.println("innerUR2 = " + innerUR2.toString());
//    System.out.println("secondLL = " + secondFeatureLL.toString());
//    System.out.println("secondUR = " + secondFeatureUR.toString());
    // The following rectangles were generated after visually inspecting the generated raster
    // and the actual polygon in QGIS, It seems that the paint operation has some sort of
    // round-off when converting lat/lon coordinates to pixels, hence the need to increment
    // some coordinates by one and decrement others by one.
    LongRectangle outer = new LongRectangle(outerLL.px + 1, outerUR.py, outerUR.px - 1, outerLL.py - 1);
    LongRectangle inner1 = new LongRectangle(innerLL1.px + 1, innerUR1.py, innerUR1.px - 1, innerLL1.py - 1);
    LongRectangle inner2 = new LongRectangle(innerLL2.px, innerUR2.py, innerUR2.px - 1, innerLL2.py - 1);
    LongRectangle secondFeature = new LongRectangle(secondFeatureLL.px, secondFeatureUR.py, secondFeatureUR.px - 1, secondFeatureLL.py - 1);
    for (int x = 0; x < raster.getWidth(); x++)
    {
      for (int y = 0; y < raster.getHeight(); y++)
      {
        for (int band = 0; band < raster.getNumBands(); band++)
        {
          final double v = raster.getSampleDouble(x, y, band);

//          if (!Double.isNaN(v))
//          {
//            System.out.println(x + ", " + y + " = " + v);
//          }
          if (outer.contains(x, y))
          {
            // Because the holey polygon is painted first, and the overlap second, any pixel
            // within the overlap should be set to the maskValue.
            if (secondFeature.contains(x, y))
            {
              Assert.assertEquals("Unexpected value: px: " + x + " py: " + y, maskValue, v, 0.0000001);
            }
            else if (inner1.contains(x, y))
            {
              Assert.assertTrue("Expected NaN for value at: px: " + x + " py: " + y + ", not " + v, Double.isNaN(v));
            }
            else if (inner2.contains(x, y))
            {
              Assert.assertTrue("Expected NaN for value at: px: " + x + " py: " + y + ", not " + v, Double.isNaN(v));
            }
            else
            {
              Assert.assertEquals("Unexpected value: px: " + x + " py: " + y, maskValue, v, 0.0000001);
            }
          }
          else
          {
            // Some pixels are outside of the outer polygon, but still inside the
            // overlap polygon, so they should be set to the maskValue.
            if (secondFeature.contains(x, y))
            {
              Assert.assertEquals("Unexpected value: px: " + x + " py: " + y, maskValue, v, 0.0000001);
            }
            else
            {
              Assert.assertTrue("Expected NaN for value at: px: " + x + " py: " + y + ", not " + v, Double.isNaN(v));
            }
          }
        }
      }
    }
  }

  private static void compareRastersForOverlappingHoleyLast(final Raster raster, final double maskValue)
  {
    try
    {
      GeotoolsRasterUtils.saveLocalGeotiff("/export/home/dave.johnson/out.tif", raster, tx, ty, zoom, tileSize, Double.NaN);
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    catch (FactoryException e)
    {
      System.out.println("Unable to save raster " + e);
    }
    TMSUtils.Pixel outerLL = TMSUtils.latLonToTilePixelUL(minY2, minX2, tx, ty, zoom, tileSize);
    TMSUtils.Pixel outerUR = TMSUtils.latLonToTilePixelUL(maxY2, maxX2, tx, ty, zoom, tileSize);
    TMSUtils.Pixel innerLL1 = TMSUtils.latLonToTilePixelUL(minYinner1, minXinner1, tx, ty, zoom, tileSize);
    TMSUtils.Pixel innerUR1 = TMSUtils.latLonToTilePixelUL(maxYinner1, maxXinner1, tx, ty, zoom, tileSize);
    TMSUtils.Pixel innerLL2 = TMSUtils.latLonToTilePixelUL(minYinner2, minXinner2, tx, ty, zoom, tileSize);
    TMSUtils.Pixel innerUR2 = TMSUtils.latLonToTilePixelUL(maxYinner2, maxXinner2, tx, ty, zoom, tileSize);
    TMSUtils.Pixel secondFeatureLL = TMSUtils.latLonToTilePixelUL(minY3, minX3, tx, ty, zoom, tileSize);
    TMSUtils.Pixel secondFeatureUR = TMSUtils.latLonToTilePixelUL(maxY3, maxX3, tx, ty, zoom, tileSize);
//    System.out.println("outerLL = " + outerLL.toString());
//    System.out.println("outerUR = " + outerUR.toString());
//    System.out.println("innerLL1 = " + innerLL1.toString());
//    System.out.println("innerUR1 = " + innerUR1.toString());
//    System.out.println("innerLL2 = " + innerLL2.toString());
//    System.out.println("innerUR2 = " + innerUR2.toString());
//    System.out.println("secondLL = " + secondFeatureLL.toString());
//    System.out.println("secondUR = " + secondFeatureUR.toString());
    // The following rectangles were generated after visually inspecting the generated raster
    // and the actual polygon in QGIS, It seems that the paint operation has some sort of
    // round-off when converting lat/lon coordinates to pixels, hence the need to increment
    // some coordinates by one and decrement others by one.
    LongRectangle outer = new LongRectangle(outerLL.px + 1, outerUR.py, outerUR.px - 1, outerLL.py - 1);
    LongRectangle inner1 = new LongRectangle(innerLL1.px + 1, innerUR1.py, innerUR1.px - 1, innerLL1.py - 1);
    LongRectangle inner2 = new LongRectangle(innerLL2.px, innerUR2.py, innerUR2.px - 1, innerLL2.py - 1);
    LongRectangle secondFeature = new LongRectangle(secondFeatureLL.px, secondFeatureUR.py, secondFeatureUR.px - 1, secondFeatureLL.py - 1);
    for (int x = 0; x < raster.getWidth(); x++)
    {
      for (int y = 0; y < raster.getHeight(); y++)
      {
        for (int band = 0; band < raster.getNumBands(); band++)
        {
          final double v = raster.getSampleDouble(x, y, band);

//          if (!Double.isNaN(v))
//          {
//            System.out.println(x + ", " + y + " = " + v);
//          }
          if (outer.contains(x, y))
          {
            // Because the holey polygon is painted last, and the overlap first, any pixel
            // within the holes of the polygon should be set to NaN.
            if (inner1.contains(x, y))
            {
              Assert.assertTrue("Expected NaN for value at: px: " + x + " py: " + y + ", not " + v, Double.isNaN(v));
            }
            else if (inner2.contains(x, y))
            {
              Assert.assertTrue("Expected NaN for value at: px: " + x + " py: " + y + ", not " + v, Double.isNaN(v));
            }
            else
            {
              Assert.assertEquals("Unexpected value: px: " + x + " py: " + y, maskValue, v, 0.0000001);
            }
          }
          else
          {
            // Some pixels are outside of the outer polygon, but still inside the
            // overlap polygon, so they should be set to the maskValue.
            if (secondFeature.contains(x, y))
            {
              Assert.assertEquals("Unexpected value: px: " + x + " py: " + y, maskValue, v, 0.0000001);
            }
            else
            {
              Assert.assertTrue("Expected NaN for value at: px: " + x + " py: " + y + ", not " + v, Double.isNaN(v));
            }
          }
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void reducerLast() throws Exception
  {
    final String aggregationType = RasterizeVectorPainter.AggregationType.LAST.toString();
    final String valueColumn = "population";

    conf.set(RasterizeVectorPainter.AGGREGATION_TYPE, aggregationType);
    conf.set(RasterizeVectorPainter.VALUE_COLUMN, valueColumn);

    final long tileId = 1;
    final ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable> driver = new ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable>()
        .withConfiguration(conf).withReducer(new RasterizeVectorDriver.PaintReduce())
        .withInputKey(new TileIdWritable(tileId)).withInputValues(features);
    final java.util.List<Pair<TileIdWritable, RasterWritable>> l = driver.run();
    final Raster raster = RasterWritable.toRaster(l.get(0).getSecond());

    compareRasters(raster, 200.0, 200.0);
  }

  @Test
  @Category(UnitTest.class)
  public void reducerLastNoValueColumn() throws Exception
  {
    final String aggregationType = RasterizeVectorPainter.AggregationType.LAST.toString();

    conf.set(RasterizeVectorPainter.AGGREGATION_TYPE, aggregationType);

    final long tileId = 1;
    final ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable> driver = new ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable>()
        .withConfiguration(conf).withReducer(new RasterizeVectorDriver.PaintReduce())
        .withInputKey(new TileIdWritable(tileId)).withInputValues(features);
    final java.util.List<Pair<TileIdWritable, RasterWritable>> l = driver.run();
    final Raster raster = RasterWritable.toRaster(l.get(0).getSecond());

    compareRasters(raster, 1.0, 1.0);
  }

  @Test
  @Category(UnitTest.class)
  public void reducerMask() throws Exception
  {
    final String aggregationType = RasterizeVectorPainter.AggregationType.MASK.toString();

    conf.set(RasterizeVectorPainter.AGGREGATION_TYPE, aggregationType);

    final long tileId = 1;
    final ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable> driver = new ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable>()
        .withConfiguration(conf).withReducer(new RasterizeVectorDriver.PaintReduce())
        .withInputKey(new TileIdWritable(tileId)).withInputValues(features);
    final java.util.List<Pair<TileIdWritable, RasterWritable>> l = driver.run();
    final Raster raster = RasterWritable.toRaster(l.get(0).getSecond());

    compareRasters(raster, 0.0, 0.0);
  }

  @Test
  @Category(UnitTest.class)
  public void reducerMaskWithInnerRings() throws Exception
  {
    final String aggregationType = RasterizeVectorPainter.AggregationType.MASK.toString();

    conf.set(RasterizeVectorPainter.AGGREGATION_TYPE, aggregationType);

    final long tileId = 1;
    final ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable> driver = new ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable>()
            .withConfiguration(conf).withReducer(new RasterizeVectorDriver.PaintReduce())
            .withInputKey(new TileIdWritable(tileId)).withInputValues(polygonWithInnerRing);
    final java.util.List<Pair<TileIdWritable, RasterWritable>> l = driver.run();
    final Raster raster = RasterWritable.toRaster(l.get(0).getSecond());

    compareRastersForHoley(raster, 0.0);
  }

  @Test
  @Category(UnitTest.class)
  public void reducerMaskOverlappingHoleyFirst() throws Exception
  {
    final String aggregationType = RasterizeVectorPainter.AggregationType.MASK.toString();

    conf.set(RasterizeVectorPainter.AGGREGATION_TYPE, aggregationType);

    final long tileId = 1;
    final ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable> driver = new ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable>()
            .withConfiguration(conf).withReducer(new RasterizeVectorDriver.PaintReduce())
            .withInputKey(new TileIdWritable(tileId)).withInputValues(multiPolygonsWithHolesFirst);
    final java.util.List<Pair<TileIdWritable, RasterWritable>> l = driver.run();
    final Raster raster = RasterWritable.toRaster(l.get(0).getSecond());

    compareRastersForOverlappingHoleyFirst(raster, 0.0);
  }

  @Test
  @Category(UnitTest.class)
  public void reducerMaskOverlappingHoleyLast() throws Exception
  {
    final String aggregationType = RasterizeVectorPainter.AggregationType.MASK.toString();

    conf.set(RasterizeVectorPainter.AGGREGATION_TYPE, aggregationType);

    final long tileId = 1;
    final ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable> driver = new ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable>()
            .withConfiguration(conf).withReducer(new RasterizeVectorDriver.PaintReduce())
            .withInputKey(new TileIdWritable(tileId)).withInputValues(multiPolygonsWithHolesLast);
    final java.util.List<Pair<TileIdWritable, RasterWritable>> l = driver.run();
    final Raster raster = RasterWritable.toRaster(l.get(0).getSecond());

    compareRastersForOverlappingHoleyLast(raster, 0.0);
  }

  // This should not be any different than the normal mask, testing for completeness...
  @Test
  @Category(UnitTest.class)
  public void reducerMaskWithValueColumn() throws Exception
  {
    final String valueColumn = "population";
    final String aggregationType = RasterizeVectorPainter.AggregationType.MASK.toString();

    conf.set(RasterizeVectorPainter.AGGREGATION_TYPE, aggregationType);
    conf.set(RasterizeVectorPainter.VALUE_COLUMN, valueColumn);

    final long tileId = 1;
    final ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable> driver = new ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable>()
        .withConfiguration(conf).withReducer(new RasterizeVectorDriver.PaintReduce())
        .withInputKey(new TileIdWritable(tileId)).withInputValues(features);
    final java.util.List<Pair<TileIdWritable, RasterWritable>> l = driver.run();
    final Raster raster = RasterWritable.toRaster(l.get(0).getSecond());

    compareRasters(raster, 0.0, 0.0);
  }

  @Test(expected = IllegalArgumentException.class)
  @Category(UnitTest.class)
  public void reducerNoTileSize() throws IOException
  {
    final ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable> driver = new ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable>()
        .withConfiguration(conf).withReducer(new RasterizeVectorDriver.PaintReduce())
        .withInputKey(new TileIdWritable(1)).withInputValues(features);
    driver.run();
  }

  @Test(expected = IllegalArgumentException.class)
  @Category(UnitTest.class)
  public void reducerNoZoom() throws IOException
  {
    final ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable> driver = new ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable>()
        .withConfiguration(conf).withReducer(new RasterizeVectorDriver.PaintReduce())
        .withInputKey(new TileIdWritable(1)).withInputValues(features);
    driver.run();
  }

  @Test
  @Category(UnitTest.class)
  public void reducerSum() throws Exception
  {
    final String aggregationType = RasterizeVectorPainter.AggregationType.SUM.toString();

    conf.set(RasterizeVectorPainter.AGGREGATION_TYPE, aggregationType);

    final long tileId = 1;
    final ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable> driver = new ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable>()
        .withConfiguration(conf).withReducer(new RasterizeVectorDriver.PaintReduce())
        .withInputKey(new TileIdWritable(tileId)).withInputValues(features);
    final java.util.List<Pair<TileIdWritable, RasterWritable>> l = driver.run();
    final Raster raster = RasterWritable.toRaster(l.get(0).getSecond());

    compareRasters(raster, 1.0, 2.0);
  }

  @Test
  @Category(UnitTest.class)
  public void reducerSumWithValueColumn() throws Exception
  {
    final String valueColumn = "population";
    final String aggregationType = RasterizeVectorPainter.AggregationType.SUM.toString();

    conf.set(RasterizeVectorPainter.AGGREGATION_TYPE, aggregationType);
    conf.set(RasterizeVectorPainter.VALUE_COLUMN, valueColumn);

    final long tileId = 1;
    final ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable> driver =
        new ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable>()
            .withConfiguration(conf).withReducer(new RasterizeVectorDriver.PaintReduce())
            .withInputKey(new TileIdWritable(tileId)).withInputValues(features);
    final java.util.List<Pair<TileIdWritable, RasterWritable>> l = driver.run();
    final Raster raster = RasterWritable.toRaster(l.get(0).getSecond());

    compareRasters(raster, 200.0, 500.0);
  }
  
  @Test
  @Category(UnitTest.class)
  public void reducerAverage() throws Exception
  {
    final String aggregationType = RasterizeVectorPainter.AggregationType.AVERAGE.toString();

    conf.set(RasterizeVectorPainter.AGGREGATION_TYPE, aggregationType);

    final long tileId = 1;
    final ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable> driver = new ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable>()
        .withConfiguration(conf).withReducer(new RasterizeVectorDriver.PaintReduce())
        .withInputKey(new TileIdWritable(tileId)).withInputValues(features);
    final java.util.List<Pair<TileIdWritable, RasterWritable>> l = driver.run();
    final Raster raster = RasterWritable.toRaster(l.get(0).getSecond());

    compareRasters(raster, 1.0, 1.0);
  }

  @Test
  @Category(UnitTest.class)
  public void reducerAverageWithValueColumn() throws Exception
  {
    final String valueColumn = "population";
    final String aggregationType = RasterizeVectorPainter.AggregationType.AVERAGE.toString();

    conf.set(RasterizeVectorPainter.AGGREGATION_TYPE, aggregationType);
    conf.set(RasterizeVectorPainter.VALUE_COLUMN, valueColumn);

    final long tileId = 1;
    final ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable> driver = new ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable>()
        .withConfiguration(conf).withReducer(new RasterizeVectorDriver.PaintReduce())
        .withInputKey(new TileIdWritable(tileId)).withInputValues(features);
    final java.util.List<Pair<TileIdWritable, RasterWritable>> l = driver.run();
    final Raster raster = RasterWritable.toRaster(l.get(0).getSecond());

    compareRasters(raster, 200.0, 250.0);
  }
  
  @Test
  @Category(UnitTest.class)
  public void reducerMin() throws Exception
  {
    final String aggregationType = RasterizeVectorPainter.AggregationType.MIN.toString();

    conf.set(RasterizeVectorPainter.AGGREGATION_TYPE, aggregationType);

    final long tileId = 1;
    final ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable> driver = new ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable>()
        .withConfiguration(conf).withReducer(new RasterizeVectorDriver.PaintReduce())
        .withInputKey(new TileIdWritable(tileId)).withInputValues(features);
    final java.util.List<Pair<TileIdWritable, RasterWritable>> l = driver.run();
    final Raster raster = RasterWritable.toRaster(l.get(0).getSecond());

    compareRasters(raster, 1.0, 1.0);
  }

  @Test
  @Category(UnitTest.class)
  public void reducerMinWithValueColumn() throws Exception
  {
    final String valueColumn = "population";
    final String aggregationType = RasterizeVectorPainter.AggregationType.MIN.toString();

    conf.set(RasterizeVectorPainter.AGGREGATION_TYPE, aggregationType);
    conf.set(RasterizeVectorPainter.VALUE_COLUMN, valueColumn);

    final long tileId = 1;
    final ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable> driver = new ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable>()
        .withConfiguration(conf).withReducer(new RasterizeVectorDriver.PaintReduce())
        .withInputKey(new TileIdWritable(tileId)).withInputValues(features);
    final java.util.List<Pair<TileIdWritable, RasterWritable>> l = driver.run();
    final Raster raster = RasterWritable.toRaster(l.get(0).getSecond());

    compareRasters(raster, 200.0, 200.0);
  }
  @Test
  @Category(UnitTest.class)
  public void reducerMax() throws Exception
  {
    final String aggregationType = RasterizeVectorPainter.AggregationType.MAX.toString();

    conf.set(RasterizeVectorPainter.AGGREGATION_TYPE, aggregationType);

    final long tileId = 1;
    final ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable> driver = new ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable>()
        .withConfiguration(conf).withReducer(new RasterizeVectorDriver.PaintReduce())
        .withInputKey(new TileIdWritable(tileId)).withInputValues(features);
    final java.util.List<Pair<TileIdWritable, RasterWritable>> l = driver.run();
    final Raster raster = RasterWritable.toRaster(l.get(0).getSecond());

    compareRasters(raster, 1.0, 1.0);
  }

  @Test
  @Category(UnitTest.class)
  public void reducerMaxWithValueColumn() throws Exception
  {
    final String valueColumn = "population";
    final String aggregationType = RasterizeVectorPainter.AggregationType.MAX.toString();

    conf.set(RasterizeVectorPainter.AGGREGATION_TYPE, aggregationType);
    conf.set(RasterizeVectorPainter.VALUE_COLUMN, valueColumn);

    final long tileId = 1;
    final ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable> driver = new ReduceDriver<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable>()
        .withConfiguration(conf).withReducer(new RasterizeVectorDriver.PaintReduce())
        .withInputKey(new TileIdWritable(tileId)).withInputValues(features);
    final java.util.List<Pair<TileIdWritable, RasterWritable>> l = driver.run();
    final Raster raster = RasterWritable.toRaster(l.get(0).getSecond());

    compareRasters(raster, 200.0, 300.0);
  }

}