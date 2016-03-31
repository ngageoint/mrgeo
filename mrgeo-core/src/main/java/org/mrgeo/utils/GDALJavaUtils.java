/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.utils;

import org.gdal.gdal.Dataset;
import scala.util.Left;
import scala.util.Right;

import java.awt.image.Raster;
import java.io.OutputStream;

public class GDALJavaUtils
{

public static void saveRaster(Raster raster, String filename)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      null,
      Double.NEGATIVE_INFINITY,
      "GTiff",
      new String[]{}
  );
}

public static void saveRaster(Raster raster, String filename, String format)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      null,
      Double.NEGATIVE_INFINITY,
      format,
      new String[]{}
  );
}

public static void saveRaster(Raster raster, String filename, double nodata)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      null,
      nodata,
      "GTiff",
      new String[]{}
  );
}

public static void saveRaster(Raster raster, String filename, double nodata, String format)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      null,
      nodata,
      format,
      new String[]{}
  );
}

public static void saveRaster(Raster raster, String filename, String[] options)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      null,
      Double.NEGATIVE_INFINITY,
      "GTiff",
      options
  );
}

public static void saveRaster(Raster raster, String filename, String format, String[] options)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      null,
      Double.NEGATIVE_INFINITY,
      format,
      options
  );
}

public static void saveRaster(Raster raster, String filename, double nodata, String[] options)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      null,
      nodata,
      "GTiff",
      options
  );
}

public static void saveRaster(Raster raster, String filename, double nodata, String format, String[] options)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      null,
      nodata,
      format,
      options
  );
}

public static void saveRaster(Dataset dataset, String filename)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      null,
      Double.NEGATIVE_INFINITY,
      "GTiff",
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, String filename, String format)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      null,
      Double.NEGATIVE_INFINITY,
      format,
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, String filename, double nodata)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      null,
      nodata,
      "GTiff",
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, String filename, double nodata, String format)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      null,
      nodata,
      format,
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, String filename, String[] options)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      null,
      Double.NEGATIVE_INFINITY,
      "GTiff",
      options
  );
}

public static void saveRaster(Dataset dataset, String filename, String format, String[] options)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      null,
      Double.NEGATIVE_INFINITY,
      format,
      options
  );
}

public static void saveRaster(Dataset dataset, String filename, double nodata, String[] options)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      null,
      nodata,
      "GTiff",
      options
  );
}

public static void saveRaster(Dataset dataset, String filename, double nodata, String format, String[] options)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      null,
      nodata,
      format,
      options
  );
}


public static void saveRaster(Raster raster, OutputStream stream)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      null,
      Double.NEGATIVE_INFINITY,
      "GTiff",
      new String[]{}
  );
}

public static void saveRaster(Raster raster, OutputStream stream, String format)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      null,
      Double.NEGATIVE_INFINITY,
      format,
      new String[]{}
  );
}

public static void saveRaster(Raster raster, OutputStream stream, double nodata)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      null,
      nodata,
      "GTiff",
      new String[]{}
  );
}

public static void saveRaster(Raster raster, OutputStream stream, double nodata, String format)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      null,
      nodata,
      format,
      new String[]{}
  );
}

public static void saveRaster(Raster raster, OutputStream stream, String[] options)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      null,
      Double.NEGATIVE_INFINITY,
      "GTiff",
      options
  );
}

public static void saveRaster(Raster raster, OutputStream stream, String format, String[] options)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      null,
      Double.NEGATIVE_INFINITY,
      format,
      options
  );
}

public static void saveRaster(Raster raster, OutputStream stream, double nodata, String[] options)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      null,
      nodata,
      "GTiff",
      options
  );
}

public static void saveRaster(Raster raster, OutputStream stream, double nodata, String format, String[] options)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      null,
      nodata,
      format,
      options
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      null,
      Double.NEGATIVE_INFINITY,
      "GTiff",
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, String format)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      null,
      Double.NEGATIVE_INFINITY,
      format,
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, double nodata)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      null,
      nodata,
      "GTiff",
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, double nodata, String format)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      null,
      nodata,
      format,
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, String[] options)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      null,
      Double.NEGATIVE_INFINITY,
      "GTiff",
      options
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, String format, String[] options)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      null,
      Double.NEGATIVE_INFINITY,
      format,
      options
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, double nodata, String[] options)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      null,
      nodata,
      "GTiff",
      options
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, double nodata, String format, String[] options)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      null,
      nodata,
      format,
      options
  );
}

public static void saveRasterTile(Raster raster, String filename, long tx, long ty, int zoom)
{
  GDALUtils.saveRasterTile(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      tx, ty, zoom,
      Double.NEGATIVE_INFINITY,
      "GTiff",
      new String[]{}
  );
}

public static void saveRasterTile(Raster raster, String filename, long tx, long ty, int zoom, String format)
{
  GDALUtils.saveRasterTile(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      tx, ty, zoom,
      Double.NEGATIVE_INFINITY,
      format,
      new String[]{}
  );
}

public static void saveRasterTile(Raster raster, String filename, long tx, long ty, int zoom, double nodata)
{
  GDALUtils.saveRasterTile(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      tx, ty, zoom,
      nodata,
      "GTiff",
      new String[]{}
  );
}

public static void saveRasterTile(Raster raster, String filename, long tx, long ty, int zoom, double nodata, String format)
{
  GDALUtils.saveRasterTile(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      tx, ty, zoom,
      nodata,
      format,
      new String[]{}
  );
}

public static void saveRasterTile(Raster raster, String filename, long tx, long ty, int zoom, String[] options)
{
  GDALUtils.saveRasterTile(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      tx, ty, zoom,
      Double.NEGATIVE_INFINITY,
      "GTiff",
      options
  );
}

public static void saveRasterTile(Raster raster, String filename, long tx, long ty, int zoom, String format, String[] options)
{
  GDALUtils.saveRasterTile(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      tx, ty, zoom,
      Double.NEGATIVE_INFINITY,
      format,
      options
  );
}

public static void saveRasterTile(Raster raster, String filename, long tx, long ty, int zoom, double nodata, String[] options)
{
  GDALUtils.saveRasterTile(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      tx, ty, zoom,
      nodata,
      "GTiff",
      options
  );
}

public static void saveRasterTile(Raster raster, String filename, long tx, long ty, int zoom, double nodata, String format, String[] options)
{
  GDALUtils.saveRasterTile(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      tx, ty, zoom,
      nodata,
      format,
      options
  );
}

public static void saveRasterTile(Dataset dataset, String filename, long tx, long ty, int zoom)
{
  GDALUtils.saveRasterTile(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      tx, ty, zoom,
      Double.NEGATIVE_INFINITY,
      "GTiff",
      new String[]{}
  );
}

public static void saveRasterTile(Dataset dataset, String filename, long tx, long ty, int zoom, String format)
{
  GDALUtils.saveRasterTile(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      tx, ty, zoom,
      Double.NEGATIVE_INFINITY,
      format,
      new String[]{}
  );
}

public static void saveRasterTile(Dataset dataset, String filename, long tx, long ty, int zoom, double nodata)
{
  GDALUtils.saveRasterTile(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      tx, ty, zoom,
      nodata,
      "GTiff",
      new String[]{}
  );
}

public static void saveRasterTile(Dataset dataset, String filename, long tx, long ty, int zoom, double nodata, String format)
{
  GDALUtils.saveRasterTile(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      tx, ty, zoom,
      nodata,
      format,
      new String[]{}
  );
}

public static void saveRasterTile(Dataset dataset, String filename, long tx, long ty, int zoom, String[] options)
{
  GDALUtils.saveRasterTile(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      tx, ty, zoom,
      Double.NEGATIVE_INFINITY,
      "GTiff",
      options
  );
}

public static void saveRasterTile(Dataset dataset, String filename, long tx, long ty, int zoom, String format, String[] options)
{
  GDALUtils.saveRasterTile(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      tx, ty, zoom,
      Double.NEGATIVE_INFINITY,
      format,
      options
  );
}

public static void saveRasterTile(Dataset dataset, String filename, long tx, long ty, int zoom, double nodata, String[] options)
{
  GDALUtils.saveRasterTile(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      tx, ty, zoom,
      nodata,
      "GTiff",
      options
  );
}

public static void saveRasterTile(Dataset dataset, String filename, long tx, long ty, int zoom, double nodata, String format, String[] options)
{
  GDALUtils.saveRasterTile(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      tx, ty, zoom,
      nodata,
      format,
      options
  );
}

public static void saveRasterTile(Raster raster, OutputStream stream, long tx, long ty, int zoom)
{
  GDALUtils.saveRasterTile(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      tx, ty, zoom,
      Double.NEGATIVE_INFINITY,
      "GTiff",
      new String[]{}
  );
}

public static void saveRasterTile(Raster raster, OutputStream stream, long tx, long ty, int zoom, String format)
{
  GDALUtils.saveRasterTile(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      tx, ty, zoom,
      Double.NEGATIVE_INFINITY,
      format,
      new String[]{}
  );
}

public static void saveRasterTile(Raster raster, OutputStream stream, long tx, long ty, int zoom, double nodata)
{
  GDALUtils.saveRasterTile(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      tx, ty, zoom,
      nodata,
      "GTiff",
      new String[]{}
  );
}

public static void saveRasterTile(Raster raster, OutputStream stream, long tx, long ty, int zoom, double nodata, String format)
{
  GDALUtils.saveRasterTile(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      tx, ty, zoom,
      nodata,
      format,
      new String[]{}
  );
}

public static void saveRasterTile(Raster raster, OutputStream stream, long tx, long ty, int zoom, String[] options)
{
  GDALUtils.saveRasterTile(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      tx, ty, zoom,
      Double.NEGATIVE_INFINITY,
      "GTiff",
      options
  );
}

public static void saveRasterTile(Raster raster, OutputStream stream, long tx, long ty, int zoom, String format, String[] options)
{
  GDALUtils.saveRasterTile(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      tx, ty, zoom,
      Double.NEGATIVE_INFINITY,
      format,
      options
  );
}

public static void saveRasterTile(Raster raster, OutputStream stream, long tx, long ty, int zoom, double nodata, String[] options)
{
  GDALUtils.saveRasterTile(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      tx, ty, zoom,
      nodata,
      "GTiff",
      options
  );
}

public static void saveRasterTile(Raster raster, OutputStream stream, long tx, long ty, int zoom, double nodata, String format, String[] options)
{
  GDALUtils.saveRasterTile(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      tx, ty, zoom,
      nodata,
      format,
      options
  );
}

public static void saveRasterTile(Dataset dataset, OutputStream stream, long tx, long ty, int zoom)
{
  GDALUtils.saveRasterTile(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      tx, ty, zoom,
      Double.NEGATIVE_INFINITY,
      "GTiff",
      new String[]{}
  );
}

public static void saveRasterTile(Dataset dataset, OutputStream stream, long tx, long ty, int zoom, String format)
{
  GDALUtils.saveRasterTile(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      tx, ty, zoom,
      Double.NEGATIVE_INFINITY,
      format,
      new String[]{}
  );
}

public static void saveRasterTile(Dataset dataset, OutputStream stream, long tx, long ty, int zoom, double nodata)
{
  GDALUtils.saveRasterTile(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      tx, ty, zoom,
      nodata,
      "GTiff",
      new String[]{}
  );
}

public static void saveRasterTile(Dataset dataset, OutputStream stream, long tx, long ty, int zoom, double nodata, String format)
{
  GDALUtils.saveRasterTile(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      tx, ty, zoom,
      nodata,
      format,
      new String[]{}
  );
}

public static void saveRasterTile(Dataset dataset, OutputStream stream, long tx, long ty, int zoom, String[] options)
{
  GDALUtils.saveRasterTile(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      tx, ty, zoom,
      Double.NEGATIVE_INFINITY,
      "GTiff",
      options
  );
}

public static void saveRasterTile(Dataset dataset, OutputStream stream, long tx, long ty, int zoom, String format, String[] options)
{
  GDALUtils.saveRasterTile(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      tx, ty, zoom,
      Double.NEGATIVE_INFINITY,
      format,
      options
  );
}

public static void saveRasterTile(Dataset dataset, OutputStream stream, long tx, long ty, int zoom, double nodata, String[] options)
{
  GDALUtils.saveRasterTile(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      tx, ty, zoom,
      nodata,
      "GTiff",
      options
  );
}

public static void saveRasterTile(Dataset dataset, OutputStream stream, long tx, long ty, int zoom, double nodata, String format, String[] options)
{
  GDALUtils.saveRasterTile(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      tx, ty, zoom,
      nodata,
      format,
      options
  );
}

public static void saveRaster(Raster raster, String filename, Bounds bounds)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      "GTiff",
      new String[]{}
  );
}

public static void saveRaster(Raster raster, String filename, Bounds bounds, String format)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      format,
      new String[]{}
  );
}

public static void saveRaster(Raster raster, String filename, Bounds bounds, double nodata)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      "GTiff",
      new String[]{}
  );
}

public static void saveRaster(Raster raster, String filename, Bounds bounds, double nodata, String format)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      format,
      new String[]{}
  );
}

public static void saveRaster(Raster raster, String filename, Bounds bounds, String[] options)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      "GTiff",
      options
  );
}

public static void saveRaster(Raster raster, String filename, Bounds bounds, String format, String[] options)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      format,
      options
  );
}

public static void saveRaster(Raster raster, String filename, Bounds bounds, double nodata, String[] options)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      "GTiff",
      options
  );
}

public static void saveRaster(Raster raster, String filename, Bounds bounds, double nodata, String format, String[] options)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      format,
      options
  );
}

public static void saveRaster(Dataset dataset, String filename, Bounds bounds)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      "GTiff",
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, String filename, Bounds bounds, String format)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      format,
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, String filename, Bounds bounds, double nodata)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      "GTiff",
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, String filename, Bounds bounds, double nodata, String format)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      format,
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, String filename, Bounds bounds, String[] options)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      "GTiff",
      options
  );
}

public static void saveRaster(Dataset dataset, String filename, Bounds bounds, String format, String[] options)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      format,
      options
  );
}

public static void saveRaster(Dataset dataset, String filename, Bounds bounds, double nodata, String[] options)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      "GTiff",
      options
  );
}

public static void saveRaster(Dataset dataset, String filename, Bounds bounds, double nodata, String format, String[] options)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      format,
      options
  );
}


public static void saveRaster(Raster raster, OutputStream stream, Bounds bounds)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      "GTiff",
      new String[]{}
  );
}

public static void saveRaster(Raster raster, OutputStream stream, Bounds bounds, String format)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      format,
      new String[]{}
  );
}

public static void saveRaster(Raster raster, OutputStream stream, Bounds bounds, double nodata)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      "GTiff",
      new String[]{}
  );
}

public static void saveRaster(Raster raster, OutputStream stream, Bounds bounds, double nodata, String format)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      format,
      new String[]{}
  );
}

public static void saveRaster(Raster raster, OutputStream stream, Bounds bounds, String[] options)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      "GTiff",
      options
  );
}

public static void saveRaster(Raster raster, OutputStream stream, Bounds bounds, String format, String[] options)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      format,
      options
  );
}

public static void saveRaster(Raster raster, OutputStream stream, Bounds bounds, double nodata, String[] options)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      "GTiff",
      options
  );
}

public static void saveRaster(Raster raster, OutputStream stream, Bounds bounds, double nodata, String format, String[] options)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      format,
      options
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, Bounds bounds)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      "GTiff",
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, Bounds bounds, String format)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      format,
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, Bounds bounds, double nodata)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      "GTiff",
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, Bounds bounds, double nodata, String format)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      format,
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, Bounds bounds, String[] options)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      "GTiff",
      options
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, Bounds bounds, String format, String[] options)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      format,
      options
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, Bounds bounds, double nodata, String[] options)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      "GTiff",
      options
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, Bounds bounds, double nodata, String format, String[] options)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      new Left<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      format,
      options
  );
}




public static void saveRaster(Raster raster, String filename, TMSUtils.Bounds bounds)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      "GTiff",
      new String[]{}
  );
}

public static void saveRaster(Raster raster, String filename, TMSUtils.Bounds bounds, String format)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      format,
      new String[]{}
  );
}

public static void saveRaster(Raster raster, String filename, TMSUtils.Bounds bounds, double nodata)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      "GTiff",
      new String[]{}
  );
}

public static void saveRaster(Raster raster, String filename, TMSUtils.Bounds bounds, double nodata, String format)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      format,
      new String[]{}
  );
}

public static void saveRaster(Raster raster, String filename, TMSUtils.Bounds bounds, String[] options)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      "GTiff",
      options
  );
}

public static void saveRaster(Raster raster, String filename, TMSUtils.Bounds bounds, String format, String[] options)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      format,
      options
  );
}

public static void saveRaster(Raster raster, String filename, TMSUtils.Bounds bounds, double nodata, String[] options)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      "GTiff",
      options
  );
}

public static void saveRaster(Raster raster, String filename, TMSUtils.Bounds bounds, double nodata, String format, String[] options)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Left<String, OutputStream>(filename),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      format,
      options
  );
}

public static void saveRaster(Dataset dataset, String filename, TMSUtils.Bounds bounds)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      "GTiff",
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, String filename, TMSUtils.Bounds bounds, String format)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      format,
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, String filename, TMSUtils.Bounds bounds, double nodata)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      "GTiff",
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, String filename, TMSUtils.Bounds bounds, double nodata, String format)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      format,
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, String filename, TMSUtils.Bounds bounds, String[] options)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      "GTiff",
      options
  );
}

public static void saveRaster(Dataset dataset, String filename, TMSUtils.Bounds bounds, String format, String[] options)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      format,
      options
  );
}

public static void saveRaster(Dataset dataset, String filename, TMSUtils.Bounds bounds, double nodata, String[] options)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      "GTiff",
      options
  );
}

public static void saveRaster(Dataset dataset, String filename, TMSUtils.Bounds bounds, double nodata, String format, String[] options)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Left<String, OutputStream>(filename),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      format,
      options
  );
}


public static void saveRaster(Raster raster, OutputStream stream, TMSUtils.Bounds bounds)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      "GTiff",
      new String[]{}
  );
}

public static void saveRaster(Raster raster, OutputStream stream, TMSUtils.Bounds bounds, String format)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      format,
      new String[]{}
  );
}

public static void saveRaster(Raster raster, OutputStream stream, TMSUtils.Bounds bounds, double nodata)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      "GTiff",
      new String[]{}
  );
}

public static void saveRaster(Raster raster, OutputStream stream, TMSUtils.Bounds bounds, double nodata, String format)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      format,
      new String[]{}
  );
}

public static void saveRaster(Raster raster, OutputStream stream, TMSUtils.Bounds bounds, String[] options)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      "GTiff",
      options
  );
}

public static void saveRaster(Raster raster, OutputStream stream, TMSUtils.Bounds bounds, String format, String[] options)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      format,
      options
  );
}

public static void saveRaster(Raster raster, OutputStream stream, TMSUtils.Bounds bounds, double nodata, String[] options)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      "GTiff",
      options
  );
}

public static void saveRaster(Raster raster, OutputStream stream, TMSUtils.Bounds bounds, double nodata, String format, String[] options)
{
  GDALUtils.saveRaster(
      new Left<Raster, Dataset>(raster),
      new Right<String, OutputStream>(stream),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      format,
      options
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, TMSUtils.Bounds bounds)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      "GTiff",
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, TMSUtils.Bounds bounds, String format)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      format,
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, TMSUtils.Bounds bounds, double nodata)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      "GTiff",
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, TMSUtils.Bounds bounds, double nodata, String format)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      format,
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, TMSUtils.Bounds bounds, String[] options)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      "GTiff",
      options
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, TMSUtils.Bounds bounds, String format, String[] options)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      Double.NEGATIVE_INFINITY,
      format,
      options
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, TMSUtils.Bounds bounds, double nodata, String[] options)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      "GTiff",
      options
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, TMSUtils.Bounds bounds, double nodata, String format, String[] options)
{
  GDALUtils.saveRaster(
      new Right<Raster, Dataset>(dataset),
      new Right<String, OutputStream>(stream),
      new Right<Bounds, TMSUtils.Bounds>(bounds),
      nodata,
      format,
      options
  );
}
}


//raster:Either[Raster, Dataset], output:Either[String, OutputStream],
//    bounds:Either[Bounds, TMSUtils.Bounds] = null, nodata:Double = Double.NegativeInfinity,
//    format:String = "GTiff", options:Array[String] = Array.empty[String]