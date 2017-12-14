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

package org.mrgeo.data.vector.mbvectortiles;

import com.almworks.sqlite4java.SQLiteException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mrgeo.geometry.GeometryFactory;
import org.mrgeo.geometry.LinearRing;
import org.mrgeo.geometry.Point;
import org.mrgeo.geometry.Polygon;
import org.mrgeo.test.TestUtils;

import java.io.File;
import java.io.IOException;

public class MbVectorTilesRecordReaderTest
{
  private static double POINT_EPSILON = 1e-7;
  private static String input;

  private MbVectorTilesInputFormat getInputFormatForWaterZoom14(int tilesPerPartition)
  {
    String[] layers = { "water" };
    MbVectorTilesSettings dbSettings = new MbVectorTilesSettings(
            "/home/dave.johnson/Downloads/2017-07-03_new-zealand_wellington.mbtiles",
            layers,
            6,
            tilesPerPartition,
            null

    );
    MbVectorTilesInputFormat inputFormat = new MbVectorTilesInputFormat(dbSettings);
    return inputFormat;
  }

  @BeforeClass
  public static void init()
  {
    input = TestUtils.composeInputDir(MbVectorTilesRecordReaderTest.class);
  }

  @Test
  public void testZoom0() throws IOException, SQLiteException, InterruptedException
  {
    Point[] poly1 = new Point[] {
            GeometryFactory.createPoint(-97.910156250, 83.500294576),
            GeometryFactory.createPoint(-73.740234375, -7.972197714),
            GeometryFactory.createPoint(-139.042968750, 12.297068293),
            GeometryFactory.createPoint(-97.910156250, 83.500294576)
    };
    Point[] poly2 = new Point[] {
            GeometryFactory.createPoint(-24.169921875, 61.354613585),
            GeometryFactory.createPoint(-49.218750000, 28.613459424),
            GeometryFactory.createPoint(-47.373046875, 52.482780222),
            GeometryFactory.createPoint(-24.169921875, 61.354613585)
    };
    Point[] poly3 = new Point[] {
            GeometryFactory.createPoint(-26.806640625, 22.024545601),
            GeometryFactory.createPoint(-1.845703125, 2.547987871),
            GeometryFactory.createPoint(-33.925781250, -15.114552872),
            GeometryFactory.createPoint(-51.591796875, -45.890008159),
            GeometryFactory.createPoint(-74.355468750, -19.394067895),
            GeometryFactory.createPoint(-44.560546875, -3.864254616),
            GeometryFactory.createPoint(-66.005859375, 6.489983333),
            GeometryFactory.createPoint(-26.806640625, 22.024545601)
    };
    Point[] poly4 = new Point[]{
            GeometryFactory.createPoint(0.878906250, 33.211116472),
            GeometryFactory.createPoint(77.607421875, 14.859850401),
            GeometryFactory.createPoint(15.908203125, -35.101934057),
            GeometryFactory.createPoint(0.878906250, 33.211116472)
    };
    Point[][] polygons = {
            poly1,
            poly2,
            poly3,
            poly4
    };

    File dbPath = new File(input, "simple-triangles-z0.mbtiles");
    MbVectorTilesSettings dbSettings = new MbVectorTilesSettings(dbPath.getAbsolutePath(), new String[] { "simple-triangles"}, 0, 1, null);
    MbVectorTilesRecordReader reader = new MbVectorTilesRecordReader(dbSettings);
    MbVectorTilesInputSplit split = new MbVectorTilesInputSplit(-1, -1, dbSettings.getZoom());
    reader.initialize(split, null);
    int index = 0;
    while (reader.nextKeyValue()) {
      org.mrgeo.geometry.Geometry g = reader.getCurrentValue();
      Assert.assertTrue(g instanceof Polygon);
      Polygon polygon = (Polygon)g;
      Point[] expectedPoly = polygons[index];
      LinearRing ring = polygon.getExteriorRing();
      // Note: the following code assumes that the polygon from the
      // MB tiles reader will be returned with the points in the same
      // order every time. If that turns out not to be true, then we
      // will need to make the polygon comparison smarter.
      Assert.assertEquals(expectedPoly.length, ring.getNumPoints());
      for (int i=0; i < ring.getNumPoints(); i++) {
        Point actualPoint = ring.getPoint(i);
        Point expectedPoint = expectedPoly[i];
        Assert.assertEquals(expectedPoint.getX(), actualPoint.getX(), POINT_EPSILON);
        Assert.assertEquals(expectedPoint.getY(), actualPoint.getY(), POINT_EPSILON);
      }
      index++;
    }
  }

  @Test
  public void testZoom2() throws IOException, SQLiteException, InterruptedException
  {
    Point[][] polygons = {
            new Point[] {
                    GeometryFactory.createPoint(-88.242187500, 1.757536811),
                    GeometryFactory.createPoint(-88.242187500, -3.447624667),
                    GeometryFactory.createPoint(-90.000000000, -2.899152699),
                    GeometryFactory.createPoint(-99.404296875, 0.000000000),
                    GeometryFactory.createPoint(-105.073242188, 1.757536811),
                    GeometryFactory.createPoint(-88.242187500, 1.757536811)
            },
            new Point[] {
                    GeometryFactory.createPoint(-88.242187500, 67.204032343),
                    GeometryFactory.createPoint(-88.242187500, -1.757536811),
                    GeometryFactory.createPoint(-93.757324219, -1.757536811),
                    GeometryFactory.createPoint(-99.404296875, 0.000000000),
                    GeometryFactory.createPoint(-139.042968750, 12.232654837),
                    GeometryFactory.createPoint(-118.037109375, 66.513260443),
                    GeometryFactory.createPoint(-117.553710938, 67.204032343),
                    GeometryFactory.createPoint(-88.242187500, 67.204032343)
            },
            new Point[] {
                    GeometryFactory.createPoint(-97.910156250, 83.497806839),
                    GeometryFactory.createPoint(-90.000000000, 72.777081264),
                    GeometryFactory.createPoint(-88.242187500, 68.664550672),
                    GeometryFactory.createPoint(-88.242187500, 65.802776393),
                    GeometryFactory.createPoint(-118.498535156, 65.802776393),
                    GeometryFactory.createPoint(-118.037109375, 66.513260443),
                    GeometryFactory.createPoint(-97.910156250, 83.497806839)
            },
            new Point[] {
                    GeometryFactory.createPoint(-75.058593750, 1.757536811),
                    GeometryFactory.createPoint(-74.794921875, 0.000000000),
                    GeometryFactory.createPoint(-73.674316406, -7.972197714),
                    GeometryFactory.createPoint(-90.000000000, -2.899152699),
                    GeometryFactory.createPoint(-91.757812500, -2.372368709),
                    GeometryFactory.createPoint(-91.757812500, 1.757536811),
                    GeometryFactory.createPoint(-75.058593750, 1.757536811)
            },
            new Point[] {
                    GeometryFactory.createPoint(-3.164062500, 1.757536811),
                    GeometryFactory.createPoint(-6.328125000, 0.000000000),
                    GeometryFactory.createPoint(-33.881835938, -15.178180946),
                    GeometryFactory.createPoint(-51.591796875, -45.935870621),
                    GeometryFactory.createPoint(-74.355468750, -19.394067895),
                    GeometryFactory.createPoint(-44.516601563, -3.908098882),
                    GeometryFactory.createPoint(-52.580566406, 0.000000000),
                    GeometryFactory.createPoint(-56.206054688, 1.757536811),
                    GeometryFactory.createPoint(-3.164062500, 1.757536811)
            },
            new Point[] {
                    GeometryFactory.createPoint(-87.714843750, 67.204032343),
                    GeometryFactory.createPoint(-87.451171875, 66.513260443),
                    GeometryFactory.createPoint(-74.794921875, 0.000000000),
                    GeometryFactory.createPoint(-74.553222656, -1.757536811),
                    GeometryFactory.createPoint(-91.757812500, -1.757536811),
                    GeometryFactory.createPoint(-91.757812500, 67.204032343),
                    GeometryFactory.createPoint(-87.714843750, 67.204032343)
            },
            new Point[] {
                    GeometryFactory.createPoint(-24.169921875, 61.333539673),
                    GeometryFactory.createPoint(-49.152832031, 28.594168506),
                    GeometryFactory.createPoint(-47.373046875, 52.482780222),
                    GeometryFactory.createPoint(-24.169921875, 61.333539673)
            },
            new Point[] {
                    GeometryFactory.createPoint(-26.806640625, 21.963424937),
                    GeometryFactory.createPoint(-1.823730469, 2.504085262),
                    GeometryFactory.createPoint(-6.328125000, 0.000000000),
                    GeometryFactory.createPoint(-9.470214844, -1.757536811),
                    GeometryFactory.createPoint(-48.955078125, -1.757536811),
                    GeometryFactory.createPoint(-52.580566406, 0.000000000),
                    GeometryFactory.createPoint(-65.961914063, 6.489983333),
                    GeometryFactory.createPoint(-26.806640625, 21.963424937)
            },
            new Point[] {
                    GeometryFactory.createPoint(0.900878906, 33.174341551),
                    GeometryFactory.createPoint(1.757812500, 32.990235560),
                    GeometryFactory.createPoint(1.757812500, 29.688052750),
                    GeometryFactory.createPoint(0.900878906, 33.174341551)
            },
            new Point[] {
                    GeometryFactory.createPoint(-91.757812500, 76.116621684),
                    GeometryFactory.createPoint(-90.000000000, 72.777081264),
                    GeometryFactory.createPoint(-87.451171875, 66.513260443),
                    GeometryFactory.createPoint(-87.209472656, 65.802776393),
                    GeometryFactory.createPoint(-91.757812500, 65.802776393),
                    GeometryFactory.createPoint(-91.757812500, 76.116621684)
            },
            new Point[] {
                    GeometryFactory.createPoint(62.160644531, 1.757536811),
                    GeometryFactory.createPoint(60.095214844, 0.000000000),
                    GeometryFactory.createPoint(15.974121094, -35.155845702),
                    GeometryFactory.createPoint(8.173828125, 0.000000000),
                    GeometryFactory.createPoint(7.822265625, 1.757536811),
                    GeometryFactory.createPoint(62.160644531, 1.757536811)
            },
            new Point[] {
                    GeometryFactory.createPoint(0.900878906, 33.174341551),
                    GeometryFactory.createPoint(77.673339844, 14.817370620),
                    GeometryFactory.createPoint(60.095214844, 0.000000000),
                    GeometryFactory.createPoint(58.029785156, -1.757536811),
                    GeometryFactory.createPoint(8.547363281, -1.757536811),
                    GeometryFactory.createPoint(8.173828125, 0.000000000),
                    GeometryFactory.createPoint(0.900878906, 33.174341551)
            }
    };

    File dbPath = new File(input, "simple-triangles-z2.mbtiles");
    MbVectorTilesSettings dbSettings = new MbVectorTilesSettings(dbPath.getAbsolutePath(), new String[] { "simple-triangles"}, 2, 1, null);
    MbVectorTilesRecordReader reader = new MbVectorTilesRecordReader(dbSettings);
    MbVectorTilesInputSplit split = new MbVectorTilesInputSplit(-1, -1, dbSettings.getZoom());
    reader.initialize(split, null);
    int index = 0;
    while (reader.nextKeyValue()) {
      org.mrgeo.geometry.Geometry g = reader.getCurrentValue();
      System.out.println(g.toString());
      Assert.assertTrue(g instanceof Polygon);
      Polygon polygon = (Polygon)g;
      Point[] expectedPoly = polygons[index];
      LinearRing ring = polygon.getExteriorRing();
      // Note: the following code assumes that the polygon from the
      // MB tiles reader will be returned with the points in the same
      // order every time. If that turns out not to be true, then we
      // will need to make the polygon comparison smarter.
      Assert.assertEquals(expectedPoly.length, ring.getNumPoints());
      for (int i=0; i < ring.getNumPoints(); i++) {
        Point actualPoint = ring.getPoint(i);
        Point expectedPoint = expectedPoly[i];
        Assert.assertEquals(expectedPoint.getX(), actualPoint.getX(), POINT_EPSILON);
        Assert.assertEquals(expectedPoint.getY(), actualPoint.getY(), POINT_EPSILON);
      }
      index++;
    }
  }
}