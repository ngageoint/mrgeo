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

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.mrgeo.data.vector.FeatureIdWritable;
import org.mrgeo.geometry.*;
import org.mrgeo.mapbox.vector.tile.VectorTile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class MbVectorTilesRecordReader extends RecordReader<FeatureIdWritable, Geometry>
{
  private static final int maxAllowableZoom = 32;

  private MbVectorTilesSettings dbSettings;
  private long offset;
  private long limit;
  private int zoomLevel;
  private long currIndex;
  private SQLiteConnection conn;
  private SQLiteStatement tileStmt;
//  private double left;
//  private double bottom;
  private WritableGeometry currGeom;
  private FeatureIdWritable currKey = new FeatureIdWritable();
  private Iterator<VectorTile.Tile.Layer> layerIter = null;
  private Iterator<VectorTile.Tile.Feature> featureIter = null;
  private VectorTile.Tile.Layer currLayer;
  private long tileColumn = -1;
  private long tileRow = -1;

  public MbVectorTilesRecordReader(MbVectorTilesSettings dbSettings)
  {
    this.dbSettings = dbSettings;
  }

//  @SuppressFBWarnings(value = {"SQL_INJECTION_JDBC", "SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING"}, justification = "User supplied queries are a requirement")
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
  {
    if (!(split instanceof MbVectorTilesInputSplit)) {
      throw new IOException("Expected an instance of PgInputSplit");
    }
    offset = ((MbVectorTilesInputSplit) split).getOffset();
    limit = ((MbVectorTilesInputSplit) split).getLimit();
    zoomLevel = ((MbVectorTilesInputSplit) split).getZoomLevel();
    currIndex = (offset < 0) ? 0 : offset - 1;
    try
    {
      conn = MbVectorTilesDataProvider.getDbConnection(dbSettings,
              context.getConfiguration());
      // If the offset is < 0, then there is only one partition, so no need
      // for a limit query.
      String query ="SELECT tile_column, tile_row, tile_data FROM tiles WHERE zoom_level=? order by zoom_level, tile_column, tile_row";
      if (offset >= 0 && limit >= 0) {
        query = query + " LIMIT ? OFFSET ?";
      }
      tileStmt = conn.prepare(query);
      tileStmt.bind(1, zoomLevel);
      if (offset >= 0 && limit >= 0) {
        tileStmt.bind(2, limit);
        tileStmt.bind(3, offset);
      }
    }
    catch (SQLiteException e)
    {
      throw new IOException("Could not open database.", e);
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException
  {
    // Loops until it finds the next geometry. If the feature iterator has
    // been created and has a feature available, process it. If not, then
    // see if the layer iterator has been created and has an available layer
    // to process. If not, then see if there is another tile record
    // available to process
    while (true) {
      if (featureIter != null && featureIter.hasNext() && currLayer != null) {
        // We got a feature, convert it to MrGeo Geometry and return
        currGeom = geometryFromFeature(featureIter.next(), tileColumn, tileRow, currLayer);
        currIndex++;
        return true;
      } else {
        // We're done processing features in the current layer, see if
        // there are any more layers in the current tile.
        featureIter = null;
        if (layerIter != null && layerIter.hasNext()) {
          currLayer = layerIter.next();
          if (currLayer != null) {
            featureIter = currLayer.getFeaturesList().iterator();
            // We got another layer in the current tile, continue so we can
            // process the features in this layer.
          }
        }
        else {
          // There are no layers remaining in the current tile. See if there
          // are more tiles to process.
          layerIter = null;
          try {
            if (tileStmt == null || !tileStmt.step()) {
              return false;
            }
            // Read the blob for the tile and decode the protobuf according to
            // the vector tiles spec. When converting to Geometry, I'll have to
            // re-project from the mercator (in the vector tiles) to WGS-84 that
            // we use in our Geometry. I also need to grab all of the attribute
            // values from the feature.
            tileColumn = tileStmt.columnLong(0);
            tileRow = tileStmt.columnLong(1);
            // The following was taken from tippecanoe
            tileRow = (1L << zoomLevel) - 1 - tileRow;
//            System.out.println("Processing tile x = " + tileColumn + ", y = " + tileRow);
            byte[] rawTileData = tileStmt.columnBlob(2);
            byte[] tileData = null;
            if (isGZIPStream(rawTileData)) {
              tileData = gunzip(rawTileData);
            }
            else {
              tileData = rawTileData;
            }
            VectorTile.Tile tile = VectorTile.Tile.parseFrom(tileData);
            layerIter = tile.getLayersList().iterator();

            // Compute spatial characteristics of this tile and store the
            // results (used later to compute points for geometries).
            // calculate bounds
//            Bounds wldwgs84 = new Bounds(-180.0, -85.06, 180.0, 85.06);
////            Bounds wldwgs84 = Bounds.WORLD;
//
////            SpatialReference sourceSrs = new SpatialReference();
////            sourceSrs.SetFromUserInput("EPSG:4326");
////            SpatialReference destSrs = new SpatialReference();
////            destSrs.SetFromUserInput("EPSG:3857");
////            CoordinateTransformation tx = new CoordinateTransformation(sourceSrs, destSrs);
////            double[] leftTop = tx.TransformPoint(wldwgs84.w, wldwgs84.n);
////            double[] rightBottom = tx.TransformPoint(wldwgs84.e, wldwgs84.s);
////            double l = leftTop[0];
////            double t = leftTop[1];
////            double r = rightBottom[0];
////            double b = rightBottom[1];
//            double l = -180.0;
//            double t = 90.0;
//            double r = 180.0;
//            double b = -90.0;
//
//            Bounds bounds = new Bounds(l, b, r, t);
////            Bounds bounds = new Bounds(l, l, -l, -l);
//
//            double w = bounds.width();
//            double h = bounds.height();
//
//            double scale = Math.pow(2, zoomLevel);
//
//            double tw = w / scale;
//            double th = h / scale;
//
//            bottom = bounds.s + (tileRow * th);
//            left = bounds.w + (tileColumn * tw);
            // Continue processing against the new layerIter
          }
          catch(SQLiteException e) {
            throw new IOException("Error getting next key/value pair", e);
          }
        }
      }
    }
  }

  public static boolean isGZIPStream(byte[] bytes)
  {
    return bytes[0] == (byte) GZIPInputStream.GZIP_MAGIC
            && bytes[1] == (byte) (GZIPInputStream.GZIP_MAGIC >>> 8);
  }

  private static byte[] gunzip(byte[] bytes) throws IOException
  {
    GZIPInputStream is = new GZIPInputStream(new ByteArrayInputStream(bytes));
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    byte[] buf = new byte[1024];
    int count = is.read(buf, 0, buf.length);
    while (count >= 0) {
      os.write(buf, 0, count);
      count = is.read(buf, 0, buf.length);
    }
    os.close();
    is.close();
    return os.toByteArray();
  }

//  private static byte[] gzip(byte[] input) throws Exception
//  {
//    ByteArrayOutputStream bos = new ByteArrayOutputStream();
//    GZIPOutputStream gzip = new GZIPOutputStream(bos);
//    gzip.write(input);
//    gzip.close();
//    return bos.toByteArray();
//  }

//  public static byte[] decompress(byte[] data) throws IOException, DataFormatException
//  {
//    Inflater inflater = new Inflater(true);
//    inflater.setInput(data);
//    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
//    try {
//      byte[] buffer = new byte[1024];
//      while (!inflater.finished()) {
//        int count = inflater.inflate(buffer);
//        outputStream.write(buffer, 0, count);
//      }
//    }
//    finally {
//      outputStream.close();
//    }
//    return outputStream.toByteArray();
//  }

  private double tile2lon(long x, int zoom)
  {
    long n = 1L << zoom;
    return 360.0 * x / n - 180.0;
  }

  private double tile2lat(long y, int zoom)
  {
    long n = 1L << zoom;
  	return Math.atan(Math.sinh(Math.PI* (1 - 2.0 * y / n))) * 180.0 / Math.PI;
  }

  private WritableGeometry geometryFromFeature(VectorTile.Tile.Feature feature,
                                               long tilex,
                                               long tiley,
                                               VectorTile.Tile.Layer layer)
  {
    if (!feature.hasType()) {
      return null;
    }

    int cursorx = 0;
    int cursory = 0;
    List<Point> points = new ArrayList<Point>();
    int ndx = 0;
//    final int maxAllowableZoom = 32;
    final int extent = layer.getExtent();
    List<List<Point>> geometries = new ArrayList<List<Point>>();
    while (ndx < feature.getGeometryCount()) {
      int f = feature.getGeometry(ndx);
      ndx++;
      int cid = f & 0x07;
      int count = f >> 3;

      for (int c = 0; c < count; c++) {
        if (cid == 1) {
          // MoveTo
          int dx_bitwise_result = (feature.getGeometry(ndx) >> 1) ^ (-(feature.getGeometry(ndx) & 1));
          cursorx += dx_bitwise_result;
          ndx++;
          int dy_bitwise_result = (feature.getGeometry(ndx) >> 1) ^ (-(feature.getGeometry(ndx) & 1));
          cursory += dy_bitwise_result;
//          System.out.println("px = " + cursorx);
//          System.out.println("py = " + cursory);
          ndx++;

          points = new ArrayList<Point>();
          Point pt = computePoint(tilex, tiley, extent, cursorx, cursory);
          points.add(pt);
        }
        else if (cid == 2) {
          // LineTo
          int dx_bitwise_result = (feature.getGeometry(ndx) >> 1) ^ (-(feature.getGeometry(ndx) & 1));
          cursorx += dx_bitwise_result;
          ndx++;
          int dy_bitwise_result = (feature.getGeometry(ndx) >> 1) ^ (-(feature.getGeometry(ndx) & 1));
          cursory += dy_bitwise_result;
          ndx++;

          Point pt = computePoint(tilex, tiley, extent, cursorx, cursory);
          points.add(pt);
        }
        else if (cid == 7) {
          // ClosePath
          points.add(points.get(0));
          geometries.add(points);
          points = new ArrayList<Point>();
        }
      }
    }
    if (!points.isEmpty()) {
      geometries.add(points);
    }

    WritableGeometry geom = null;
    if (feature.getType() == VectorTile.Tile.GeomType.POLYGON) {
      // The first geometry from the vector tile is the outer ring of
      // the polygon, and each subsequent geometry is an inner ring
      WritablePolygon polygon = GeometryFactory.createPolygon(geometries.get(0));
      geom = polygon;
      for (int i=1; i < geometries.size(); i++) {
        WritableLinearRing ring = GeometryFactory.createLinearRing();
        ring.setPoints(geometries.get(i));
        polygon.addInteriorRing(ring);
      }
    }
    else if (feature.getType() == VectorTile.Tile.GeomType.LINESTRING) {
      WritableLineString lineString = GeometryFactory.createLineString();
      geom = lineString;
      lineString.setPoints(geometries.get(0));
    }
    else if (feature.getType() == VectorTile.Tile.GeomType.POINT) {
      Point p = geometries.get(0).get(0);
      geom = GeometryFactory.createPoint(p.getX(), p.getY());
    }

    // Assign attributes to Geometry
    Map<String, String> tags = new HashMap<String, String>();
    if (geom != null) {
      boolean isKey = true;
      String tagKey = "";
      for (int tagIndex : feature.getTagsList()) {
        if (isKey) {
          tagKey = layer.getKeys(tagIndex);
        }
        else {
          VectorTile.Tile.Value v = layer.getValues(tagIndex);
          String value = null;
          if (v.hasBoolValue()) {
            value = Boolean.toString(v.getBoolValue());
          }
          else if (v.hasDoubleValue()) {
            value = Double.toString(v.getDoubleValue());
          }
          else if (v.hasFloatValue()) {
            value = Float.toString(v.getFloatValue());
          }
          else if (v.hasIntValue()) {
            value = Long.toString(v.getIntValue());
          }
          else if (v.hasSintValue()) {
            value = Long.toString(v.getSintValue());
          }
          else if (v.hasStringValue()) {
            value = v.getStringValue();
          }
          else if (v.hasUintValue()) {
            value = Long.toString(v.getUintValue());
          }
          if (value != null) {
            tags.put(tagKey, value);
          }
        }
        isKey = !isKey;
      }
    }
    if (geom != null) {
      geom.setAttributes(tags);
    }

    return geom;
  }

  private Point computePoint(long tilex, long tiley, int extent, int cursorx, int cursory)
  {
    // From tippecanoe converting to GeoJSON (in write_json.cpp)
    long scale = 1L << (maxAllowableZoom - zoomLevel);
    long wx = scale * tilex + (scale / extent) * cursorx;
    long wy = scale * tiley + (scale / extent) * cursory;
//    System.out.println("wx = " + wx);
//    System.out.println("wy = " + wy);

    double lat = tile2lat(wy, maxAllowableZoom);
    double lon = tile2lon(wx, maxAllowableZoom);
    return GeometryFactory.createPoint(lon, lat);
  }

  @Override
  public FeatureIdWritable getCurrentKey() throws IOException, InterruptedException
  {
    currKey.set(currIndex);
    return currKey;
  }

  @SuppressFBWarnings(value = "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE", justification = "Null return value is ok")
  @Override
  public Geometry getCurrentValue() throws IOException, InterruptedException
  {
    return currGeom;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException
  {
    return (float)(currIndex + 1 - offset) / (float)limit;
  }

  @Override
  public void close() throws IOException
  {
    if (tileStmt != null) {
      tileStmt.dispose();
      tileStmt = null;
    }
    if (conn != null) {
      conn.dispose();
      conn = null;
    }
  }
}
