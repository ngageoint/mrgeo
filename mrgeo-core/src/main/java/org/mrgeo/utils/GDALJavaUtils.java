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
import org.mrgeo.utils.tms.Bounds;
import scala.util.Left;
import scala.util.Right;

import java.io.OutputStream;

public class GDALJavaUtils
{

public static void saveRaster(Dataset dataset, String filename)
{
  GDALUtils.saveRaster(
      dataset,
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
      dataset,
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
      dataset,
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
      dataset,
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
      dataset,
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
      dataset,
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
      dataset,
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
      dataset,
      new Left<String, OutputStream>(filename),
      null,
      nodata,
      format,
      options
  );
}


public static void saveRaster(Dataset dataset, OutputStream stream)
{
  GDALUtils.saveRaster(
      dataset,
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
      dataset,
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
      dataset,
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
      dataset,
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
      dataset,
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
      dataset,
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
      dataset,
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
      dataset,
      new Right<String, OutputStream>(stream),
      null,
      nodata,
      format,
      options
  );
}


public static void saveRasterTile(Dataset dataset, String filename, long tx, long ty, int zoom)
{
  GDALUtils.saveRasterTile(
      dataset,
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
      dataset,
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
      dataset,
      new Left<String, OutputStream>(filename),
      tx, ty, zoom,
      nodata,
      "GTiff",
      new String[]{}
  );
}

public static void saveRasterTile(Dataset dataset, String filename, long tx, long ty, int zoom, double nodata,
    String format)
{
  GDALUtils.saveRasterTile(
      dataset,
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
      dataset,
      new Left<String, OutputStream>(filename),
      tx, ty, zoom,
      Double.NEGATIVE_INFINITY,
      "GTiff",
      options
  );
}

public static void saveRasterTile(Dataset dataset, String filename, long tx, long ty, int zoom, String format,
    String[] options)
{
  GDALUtils.saveRasterTile(
      dataset,
      new Left<String, OutputStream>(filename),
      tx, ty, zoom,
      Double.NEGATIVE_INFINITY,
      format,
      options
  );
}

public static void saveRasterTile(Dataset dataset, String filename, long tx, long ty, int zoom, double nodata,
    String[] options)
{
  GDALUtils.saveRasterTile(
      dataset,
      new Left<String, OutputStream>(filename),
      tx, ty, zoom,
      nodata,
      "GTiff",
      options
  );
}

public static void saveRasterTile(Dataset dataset, String filename, long tx, long ty, int zoom, double nodata,
    String format, String[] options)
{
  GDALUtils.saveRasterTile(
      dataset,
      new Left<String, OutputStream>(filename),
      tx, ty, zoom,
      nodata,
      format,
      options
  );
}

public static void saveRasterTile(Dataset dataset, OutputStream stream, long tx, long ty, int zoom)
{
  GDALUtils.saveRasterTile(
      dataset,
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
      dataset,
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
      dataset,
      new Right<String, OutputStream>(stream),
      tx, ty, zoom,
      nodata,
      "GTiff",
      new String[]{}
  );
}

public static void saveRasterTile(Dataset dataset, OutputStream stream, long tx, long ty, int zoom, double nodata,
    String format)
{
  GDALUtils.saveRasterTile(
      dataset,
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
      dataset,
      new Right<String, OutputStream>(stream),
      tx, ty, zoom,
      Double.NEGATIVE_INFINITY,
      "GTiff",
      options
  );
}

public static void saveRasterTile(Dataset dataset, OutputStream stream, long tx, long ty, int zoom, String format,
    String[] options)
{
  GDALUtils.saveRasterTile(
      dataset,
      new Right<String, OutputStream>(stream),
      tx, ty, zoom,
      Double.NEGATIVE_INFINITY,
      format,
      options
  );
}

public static void saveRasterTile(Dataset dataset, OutputStream stream, long tx, long ty, int zoom, double nodata,
    String[] options)
{
  GDALUtils.saveRasterTile(
      dataset,
      new Right<String, OutputStream>(stream),
      tx, ty, zoom,
      nodata,
      "GTiff",
      options
  );
}

public static void saveRasterTile(Dataset dataset, OutputStream stream, long tx, long ty, int zoom, double nodata,
    String format, String[] options)
{
  GDALUtils.saveRasterTile(
      dataset,
      new Right<String, OutputStream>(stream),
      tx, ty, zoom,
      nodata,
      format,
      options
  );
}


public static void saveRaster(Dataset dataset, String filename, Bounds bounds)
{
  GDALUtils.saveRaster(
      dataset,
      new Left<String, OutputStream>(filename),
      bounds,
      Double.NEGATIVE_INFINITY,
      "GTiff",
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, String filename, Bounds bounds, String format)
{
  GDALUtils.saveRaster(
      dataset,
      new Left<String, OutputStream>(filename),
      bounds,
      Double.NEGATIVE_INFINITY,
      format,
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, String filename, Bounds bounds, double nodata)
{
  GDALUtils.saveRaster(
      dataset,
      new Left<String, OutputStream>(filename),
      bounds,
      nodata,
      "GTiff",
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, String filename, Bounds bounds, double nodata, String format)
{
  GDALUtils.saveRaster(
      dataset,
      new Left<String, OutputStream>(filename),
      bounds,
      nodata,
      format,
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, String filename, Bounds bounds, String[] options)
{
  GDALUtils.saveRaster(
      dataset,
      new Left<String, OutputStream>(filename),
      bounds,
      Double.NEGATIVE_INFINITY,
      "GTiff",
      options
  );
}

public static void saveRaster(Dataset dataset, String filename, Bounds bounds, String format, String[] options)
{
  GDALUtils.saveRaster(
      dataset,
      new Left<String, OutputStream>(filename),
      bounds,
      Double.NEGATIVE_INFINITY,
      format,
      options
  );
}

public static void saveRaster(Dataset dataset, String filename, Bounds bounds, double nodata, String[] options)
{
  GDALUtils.saveRaster(
      dataset,
      new Left<String, OutputStream>(filename),
      bounds,
      nodata,
      "GTiff",
      options
  );
}

public static void saveRaster(Dataset dataset, String filename, Bounds bounds, double nodata, String format,
    String[] options)
{
  GDALUtils.saveRaster(
      dataset,
      new Left<String, OutputStream>(filename),
      bounds,
      nodata,
      format,
      options
  );
}


public static void saveRaster(Dataset dataset, OutputStream stream, Bounds bounds)
{
  GDALUtils.saveRaster(
      dataset,
      new Right<String, OutputStream>(stream),
      bounds,
      Double.NEGATIVE_INFINITY,
      "GTiff",
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, Bounds bounds, String format)
{
  GDALUtils.saveRaster(
      dataset,
      new Right<String, OutputStream>(stream),
      bounds,
      Double.NEGATIVE_INFINITY,
      format,
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, Bounds bounds, double nodata)
{
  GDALUtils.saveRaster(
      dataset,
      new Right<String, OutputStream>(stream),
      bounds,
      nodata,
      "GTiff",
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, Bounds bounds, double nodata, String format)
{
  GDALUtils.saveRaster(
      dataset,
      new Right<String, OutputStream>(stream),
      bounds,
      nodata,
      format,
      new String[]{}
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, Bounds bounds, String[] options)
{
  GDALUtils.saveRaster(
      dataset,
      new Right<String, OutputStream>(stream),
      bounds,
      Double.NEGATIVE_INFINITY,
      "GTiff",
      options
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, Bounds bounds, String format, String[] options)
{
  GDALUtils.saveRaster(
      dataset,
      new Right<String, OutputStream>(stream),
      bounds,
      Double.NEGATIVE_INFINITY,
      format,
      options
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, Bounds bounds, double nodata, String[] options)
{
  GDALUtils.saveRaster(
      dataset,
      new Right<String, OutputStream>(stream),
      bounds,
      nodata,
      "GTiff",
      options
  );
}

public static void saveRaster(Dataset dataset, OutputStream stream, Bounds bounds, double nodata, String format,
    String[] options)
{
  GDALUtils.saveRaster(
      dataset,
      new Right<String, OutputStream>(stream),
      bounds,
      nodata,
      format,
      options
  );
}


}


//raster:Either[Raster, Dataset], output:Either[String, OutputStream],
//    bounds:Either[Bounds, TMSUtils.Bounds] = null, nodata:Double = Double.NegativeInfinity,
//    format:String = "GTiff", options:Array[String] = Array.empty[String]