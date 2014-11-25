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
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.GeometryFactory;
import org.mrgeo.geometry.WritableGeometry;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.LocalRunnerTest;

import java.awt.image.Raster;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RasterizeVectorDriverTest extends LocalRunnerTest
{
  private static List<GeometryWritable> features;

  @BeforeClass
  public static void init() throws ParseException
  {
    features = new ArrayList<GeometryWritable>();

    final WKTReader reader = new WKTReader();

    String baseGeom = "POLYGON ((12 12,12 65,65 65,65 12,12 12))";
    WritableGeometry geom = GeometryFactory.fromJTS(reader.read(baseGeom));
    geom.setAttribute("population", Integer.toString(200));
    features.add(new GeometryWritable(geom));

    baseGeom = "POLYGON ((14 14,14 48,48 48,48 14,14 14))";
    geom = GeometryFactory.fromJTS(reader.read(baseGeom));
    geom.setAttribute("population", Integer.toString(300));
    features.add(new GeometryWritable(geom));
  }
  
  @Before
  public void setUp()
  {
    conf.setBoolean(RasterizeVectorDriver.TEST_REDUCER, true);
    conf.set(FeatureToTilesMapper.ZOOM, "1");
    conf.set(FeatureToTilesMapper.TILE_SIZE, "512");
    conf.set(RasterizeVectorPainter.ZOOM, "1");
    conf.set(RasterizeVectorPainter.TILE_SIZE, "512");
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