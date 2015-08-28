/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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

package org.mrgeo.rasterops;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.mrgeo.format.CsvOutputFormat;
import org.mrgeo.geometry.GeometryFactory;
import org.mrgeo.geometry.WritablePoint;
import org.mrgeo.mapalgebra.MapAlgebraExecutioner;
import org.mrgeo.mapalgebra.MapOpHadoop;
import org.mrgeo.progress.Progress;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.TMSUtils.LatLon;
import org.mrgeo.utils.TMSUtils.Pixel;

import java.io.IOException;

public class RandomSamplesToTsv implements RandomSampler.RandomSamplerListener
{
  private int count = 1;
  private String newColumnName;
  private String newColumnValue;
  private boolean assignNewColumn = false;
  private Path outputPath;
  private Path outputColumns;
  private CsvOutputFormat.CsvRecordWriter writer;

  public RandomSamplesToTsv(String newColumnName, String newColumnValue,
      Path outputPath, Path outputColumns)
  {
    this.newColumnName = newColumnName;
    this.newColumnValue = newColumnValue;
    this.outputPath = outputPath;
    this.outputColumns = outputColumns;
  }

  @Override
  public void beginningSamples() throws IOException
  {
    if ((newColumnName != null) && (newColumnName.length() > 0) &&
        (newColumnValue != null))
    {
      assignNewColumn = true;
    }

    writer = new CsvOutputFormat.CsvRecordWriter(outputColumns,
        outputPath);
  }

  @Override
  public void completedSamples() throws IOException
  {
    writer.close(null);
  }

  @Override
  public void reportPoint(Pixel pixel, LatLon location) throws IOException
  {
    WritablePoint featurePoint = GeometryFactory.createPoint(location.lon, location.lat);
    if (assignNewColumn)
    {
      featurePoint.setAttribute(newColumnName, newColumnValue);
    }
    LongWritable outputKey = new LongWritable(count);
    writer.write(outputKey, featurePoint);
  }

  /**
   * Given a raster input, this method randomly samples the specified number of
   * points from that raster in pixel space. It converts those points to WGS84
   * lat/long coordinates and writes each of those points to the specified
   * output TSV file. It does not include the pixel values form the original
   * raster in the output.
   *
   * It is assumed that the caller will pass a reasonable sample count for the
   * input raster. If the sample count is larger than the number of pixels
   * available, it will throw an IllegalArgumentException.
   *
   * @param p Object to report progress to
   * @param mapOp The input raster map op
   * @param sampleCount The number of random sample points to return
   * @param outputPath The path the .tsv file for the results
   * @param outputColumns The path to the .tsv.columns file for the results
   * @throws IOException
   */
  public static void writeRandomPointsToTsv(Progress p, MapOpHadoop mapOp, int sampleCount,
      String newColumnName, String newColumnValue,
      Path outputPath, Path outputColumns) throws IOException, IllegalArgumentException
  {
    int zoomLevel = MapAlgebraExecutioner.calculateMaximumZoomlevel(mapOp);
    Bounds bounds = MapAlgebraExecutioner.calculateBounds(mapOp);
    int tileSize = MapAlgebraExecutioner.calculateTileSize(mapOp);
    writeRandomPointsToTsv(p, bounds, zoomLevel, tileSize, sampleCount,
        newColumnName, newColumnValue, outputPath, outputColumns);
  }

  /**
   * Given a bounds, zoom and tile size, this method randomly samples the specified number of
   * points from that region in pixel space. It converts those points to WGS84
   * lat/long coordinates and writes each of those points to the specified
   * output TSV file. It does not include the pixel values form the original
   * raster in the output.
   *
   * It is assumed that the caller will pass a reasonable sample count for the
   * input raster. If the sample count is larger than the number of pixels
   * available, it will throw an IllegalArgumentException.
   *
   * @throws IOException
   */
  public static void writeRandomPointsToTsv(Progress p, Bounds bounds, int zoomLevel, int tileSize,
      long sampleCount, String newColumnName, String newColumnValue,
      Path outputPath, Path outputColumns) throws IOException, IllegalArgumentException
  {
    RandomSamplesToTsv listener = new RandomSamplesToTsv(newColumnName, newColumnValue, outputPath, outputColumns);
    RandomSampler rs = new RandomSampler();
    rs.generateRandomPoints(p, bounds, zoomLevel, tileSize, sampleCount, listener);
  }
}
