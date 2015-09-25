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

package org.mrgeo.utils;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.Driver;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconstConstants;
import org.gdal.osr.osr;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.image.*;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Vector;

public class GDALUtils
{
private static final Logger log = LoggerFactory.getLogger(GDALUtils.class);

// it's quicker (the EPSG doesn't change) to just hardcode this instead of trying to get it from GDAL/
public static final String EPSG4326 = osr.SRS_WKT_WGS84;
public static final String GDAL_LIBS;
private static final String VSI_PREFIX = "/vsimem/";

static
{

  // TODO:  We're counting on the system to be set up correct for gdal.  We should look into doing the
  // distribution ourselves
//    System.load("/usr/lib/jni/libgdaljni.so");
//    System.load("/usr/lib/jni/libgdalconstjni.so");
//    System.load("/usr/lib/jni/libosrjni.so");
//    System.load("/usr/lib/libgdal.so");
  String[] libs = {"libgdaljni.so", "libgdalconstjni.so", "libosrjni.so", "libgdal.so"};

  osr.UseExceptions();
  String rawPath = MrGeoProperties.getInstance().getProperty(MrGeoConstants.GDAL_PATH, null);

  if (rawPath != null)
  {
    // gdal jars to load.  In the order they should be loaded...

    final String[] paths = rawPath.split(":");

    final String[] found = new String[libs.length];

    log.debug("Looking for GDAL Libraries in " + rawPath);
    for (int i = 0; i < libs.length; i++)
    {
      final String lib = libs[i];
      for (String path : paths)
      {
        try
        {
          final File file = new File(path, lib);
          String msg = " Looking for: " + file.getCanonicalPath();
          if (file.exists())
          {
            found[i] = file.getCanonicalPath();
            log.debug(msg + " found");
            break;
          }

          log.debug(msg + " not found");
        }
        catch (IOException e)
        {
        }
      }
    }

    String libStr = "";
    for (int i = 0; i < libs.length; i++)
    {
      final String jar = found[i];
      if (jar != null)
      {
        System.load(jar);

        if (libStr.length() > 0)
        {
          libStr += ":";
        }
        libStr += jar;
      }
      else
      {
        log.error("ERROR!!! Can not find gdal library " + libs[i] + " in path " + rawPath);
      }
    }

    GDAL_LIBS = libStr;

  }
  else
  {
    log.warn("Can't load GDAL libraries, " + MrGeoConstants.GDAL_PATH +
        " not found in the mrgeo configuration.  This may or may not be a problem.");

//      System.out.println("java.library.path: ");
//      String property = System.getProperty("java.library.path");
//      StringTokenizer parser = new StringTokenizer(property, ";");
//      while (parser.hasMoreTokens()) {
//        System.out.println("  " + parser.nextToken());
//      }

    System.loadLibrary("gdal");
    GDAL_LIBS = null;
  }

  if (gdal.GetDriverCount() == 0)
  {
    gdal.AllRegister();
  }

  final int drivers = gdal.GetDriverCount();
  if (drivers == 0)
  {
    log.error("GDAL libraries were not loaded!  This probibly an error.");
  }

  if (log.isDebugEnabled())
  {
    log.debug("GDAL Drivers supported:");
    for (int i = 0; i < drivers; i++)
    {
      Driver driver = gdal.GetDriver(i);
      log.debug("  " + driver.getLongName() + "(" + driver.getShortName() + ")");
    }

    log.debug("GDAL Projections supported:");
    for (Object o : osr.GetProjectionMethods())
    {
      log.debug("  " + o);
    }
  }
}


@SuppressWarnings("EmptyMethod")
public static void register()
{
  // no-op to force the statics...
}

public static boolean isValidDataset(String imagename)
{
  try
  {
    Dataset image = GDALUtils.open(imagename);
    GDALUtils.close(image);

    return true;
  }
  catch (final IOException ignored)
  {
  }

  return false;
}

public static Dataset open(String imagename) throws IOException
{
  try
  {
    final URI uri = new URI(imagename);
    Dataset image = null;

    GDALUtils.log.debug("Loading image with GDAL: {}", imagename);

    File file = new File(uri.getPath());
    if (file.exists())
    {
      java.nio.file.Path p = Paths.get(file.toURI());
      byte[] bytes = Files.readAllBytes(p);
      gdal.FileFromMemBuffer(GDALUtils.VSI_PREFIX + imagename, bytes);
      image = gdal.Open(GDALUtils.VSI_PREFIX + imagename);

      //image = gdal.Open(uri.getPath());

      if (image != null)
      {
        GDALUtils.log.debug("  Image loaded successfully: {}", imagename);
        return image;
      }
    }
    GDALUtils.log.debug("  Image not on the local filesystem, trying HDFS: {}", imagename);

    final Path p = new Path(uri);
    final FileSystem fs = HadoopFileUtils.getFileSystem(p);
    final InputStream is = fs.open(p);
    final DataInputStream dis = new DataInputStream(is);
    try
    {
      byte[] bytes = IOUtils.toByteArray(is);
      gdal.FileFromMemBuffer(GDALUtils.VSI_PREFIX + imagename, bytes);

      image = gdal.Open(GDALUtils.VSI_PREFIX + imagename);

      if (image != null)
      {
        GDALUtils.log.debug("  Image loaded successfully: {}", imagename);
        return image;
      }

      GDALUtils.log.info(
          "Image not loaded, but unfortunately no exceptions were thrown, look for a logged explanation somewhere above");
    }
    finally
    {
      dis.close();
      is.close();
    }
  }
  catch (final URISyntaxException e)
  {
    throw new IOException("Error opening image file: " + imagename, e);
  }

  return null;
  //throw new IOException("Error opening image file: " + imagename);
}

public static void close(Dataset image)
{
  Vector<String> files = image.GetFileList();

  image.delete();

  for (String file: files)
  {
    if (file.startsWith(VSI_PREFIX))
    {
      gdal.Unlink(file);
    }
  }

}

public static Dataset createEmptyMemoryRaster(final Dataset src, final int width, final int height)
{
  final int bands = src.getRasterCount();
  int datatype = -1;

  final double[] nodatas = new double[bands];
  Arrays.fill(nodatas, Double.NaN);

  final Double[] val = new Double[1];
  for (int i = 1; i <= src.getRasterCount(); i++)
  {
    final Band band = src.GetRasterBand(i);

    if (datatype < 0)
    {
      datatype = band.getDataType();
    }
    band.GetNoDataValue(val);
    if (val[0] != null)
    {
      nodatas[i - 1] = val[0];
    }

  }
  return GDALUtils.createEmptyMemoryRaster(width, height, src.getRasterCount(), datatype, nodatas);
}

public static Dataset createEmptyMemoryRaster(final int width, final int height, final int bands,
    final int datatype, final double[] nodatas)
{
  final Dataset raster = createEmptyMemoryRaster(width, height, bands, datatype);

  if (raster == null)
  {
    return null;
  }

  for (int i = 1; i <= raster.getRasterCount(); i++)
  {
    final double nodata = nodatas[i - 1];
    final Band band = raster.GetRasterBand(i);

    band.Fill(nodata);
    band.SetNoDataValue(nodata);
  }

  return raster;
}

public static Dataset createEmptyMemoryRaster(final int width, final int height, final int bands,
    final int datatype)
{
  final Driver driver = gdal.GetDriverByName("MEM");
  return driver.Create("InMem", width, height, bands, datatype);
}

public static void saveRaster(Dataset raster, String filename)
{
  GDALUtils.saveRaster(raster, filename, "GTiff");
}

public static void saveRaster(Raster raster, String filename)
{
  final Dataset src = GDALUtils.toDataset(raster);
  GDALUtils.saveRaster(src, filename, "GTiff");
}

public static void saveRaster(Dataset raster, String filename, String type)
{
  final Driver driver = gdal.GetDriverByName(type);
  final Dataset copy = driver.CreateCopy(filename, raster);

  copy.delete();
}

public static void saveRaster(Raster raster, String filename, long tx, long ty, int zoom, int tilesize, double nodata)
{
  if (!filename.endsWith(".tif") && !filename.endsWith(".tiff"))
  {
    filename += ".tif";
  }

  GDALUtils.saveRaster(raster, filename, "GTiff", tx, ty, zoom, tilesize, nodata, new String[]{"COMPRESS=LZW"});
}

public static void saveRaster(Raster raster, String filename, Bounds bounds, int zoom, int tilesize, double nodata)
{
  if (!filename.endsWith(".tif") && !filename.endsWith(".tiff"))
  {
    filename += ".tif";
  }

  GDALUtils.saveRaster(raster, filename, "GTiff", bounds, zoom, tilesize, nodata, new String[]{"COMPRESS=LZW"});
}

public static void saveRaster(Raster raster, String filename, TMSUtils.Bounds bounds, int zoom, int tilesize, double nodata)
{
  if (!filename.endsWith(".tif") && !filename.endsWith(".tiff"))
  {
    filename += ".tif";
  }

  GDALUtils.saveRaster(raster, filename, "GTiff", bounds, zoom, tilesize, nodata, new String[]{"COMPRESS=LZW"});
}

public static void saveRaster(Raster raster, String filename, String type, long tx, long ty, int zoom, int tilesize, final double nodata)
{
  saveRaster(raster, filename, type, tx, ty, zoom, tilesize, nodata, null);
}

public static void saveRaster(Raster raster, String filename, String type, long tx, long ty, int zoom, int tilesize, final double nodata, String[] options)
{
  final TMSUtils.Bounds tileBounds = TMSUtils.tileBounds(tx, ty, zoom, tilesize);

  saveRaster(raster, filename, type, tileBounds, zoom, tilesize, nodata, options);
}
public static void saveRaster(Raster raster, String filename, String type, Bounds bounds, int zoom, int tilesize, final double nodata, String[] options)
{
  saveRaster(raster, filename, type, bounds.getTMSBounds(), zoom, tilesize, nodata, options);
}

public static void saveRaster(Raster raster, String filename, String type, TMSUtils.Bounds bounds, int zoom, int tilesize, final double nodata, String[] options)
{
  final Driver driver = gdal.GetDriverByName(type);
  final Dataset src = GDALUtils.toDataset(raster, nodata);

  final double res = TMSUtils.resolution(zoom, tilesize);

  final double[] xform = new double[6];

  xform[0] = bounds.w; /* top left x */
  xform[1] = res; /* w-e pixel resolution */
  xform[2] = 0; /* 0 */
  xform[3] = bounds.n; /* top left y */
  xform[4] = 0; /* 0 */
  xform[5] = -res; /* n-s pixel resolution (negative value) */

  src.SetGeoTransform(xform);
  src.SetProjection(GDALUtils.EPSG4326);

  final Dataset copy;
  if (options == null)
  {
    copy  = driver.CreateCopy(filename, src);
  }
  else {
    copy = driver.CreateCopy(filename, src, options);
  }

  copy.delete();
  src.delete();
}

public static Dataset toDataset(Raster raster, final double nodata)
{
  final double[] nodatas = new double[raster.getNumBands()];
  Arrays.fill(nodatas, nodata);

  final int type = GDALUtils.toGDALDataType(raster.getTransferType());

  final Dataset ds = GDALUtils.createEmptyMemoryRaster(raster.getWidth(), raster.getHeight(),
      raster.getNumBands(), type, nodatas);

  if (ds != null)
  {
    copyToDataset(ds, raster);

  }

  return ds;
}

public static Dataset toDataset(Raster raster)
{
  final int type = GDALUtils.toGDALDataType(raster.getTransferType());
  final Dataset ds = GDALUtils.createEmptyMemoryRaster(raster.getWidth(), raster.getHeight(),
      raster.getNumBands(), type);

  if (ds != null)
  {
    copyToDataset(ds, raster);
  }

  return ds;
}

public static Raster toRaster(Dataset image)
{
  final int bands = image.GetRasterCount();
  final int[] bandlist = new int[bands];
  for (int x = 0; x < bands; x++)
  {
    bandlist[x] = x + 1;
  }

  final int datatype = image.GetRasterBand(1).getDataType();
  final int pixelsize = gdal.GetDataTypeSize(datatype) / 8;

  final int width = image.getRasterXSize();
  final int height = image.getRasterYSize();

  final int pixelstride = pixelsize * bands;
  final int linestride = pixelstride * width;

  final int rastersize = linestride * height;

  ByteBuffer data = ByteBuffer.allocateDirect(rastersize);
  data.order(ByteOrder.nativeOrder());

  // read the data interleaved (it _should_ be much more efficient reading)
  image.ReadRaster_Direct(0, 0, width, height, width, height, datatype, data, bandlist, pixelstride, linestride, 1);

  data.rewind();
  return GDALUtils.toRaster(height, width, bands, datatype, data);
}

public static Raster toRaster(int height, int width, int bands,
    int gdaldatatype, final byte[] data)
{
  return toRaster(height, width, bands, gdaldatatype, ByteBuffer.wrap(data));
}

public static Raster toRaster(int height, int width, int bands,
    int gdaldatatype, ByteBuffer data)
{
  final int datatype = GDALUtils.toRasterDataBufferType(gdaldatatype);

  final int bandbytes = height * width * (gdal.GetDataTypeSize(gdaldatatype) / 8);
  final int databytes = bandbytes * bands;

  int[] bandOffsets = new int[bands];
  for (int i = 0; i < bands; i++) {
    bandOffsets[i] = i;
  }

  // keep the raster interleaved
  SampleModel sm = new PixelInterleavedSampleModel(datatype, width, height, bands, bands * width, bandOffsets);

  DataBuffer db;
  WritableRaster raster = null;

  switch (datatype)
  {
  case DataBuffer.TYPE_BYTE:
    byte[] bytedata = new byte[databytes];
    data.get(bytedata);

    db = new DataBufferByte(bytedata, bytedata.length);

    break;
  case DataBuffer.TYPE_FLOAT:
    FloatBuffer floatbuff = data.asFloatBuffer();
    float[] floatdata = new float[databytes / RasterUtils.FLOAT_BYTES];

    floatbuff.get(floatdata);

    db = new DataBufferFloat(floatdata, floatdata.length);
    break;
  case DataBuffer.TYPE_DOUBLE:
    DoubleBuffer doublebuff = data.asDoubleBuffer();
    double[] doubledata = new double[databytes / RasterUtils.DOUBLE_BYTES];

    doublebuff.get(doubledata);

    db = new DataBufferDouble(doubledata, doubledata.length);
    break;
  case DataBuffer.TYPE_INT:
    IntBuffer intbuff = data.asIntBuffer();
    int[] intdata = new int[databytes / RasterUtils.INT_BYTES];

    intbuff.get(intdata);

    db = new DataBufferInt(intdata, intdata.length);
    break;
  case DataBuffer.TYPE_SHORT:
    ShortBuffer shortbuff = data.asShortBuffer();
    short[] shortdata = new short[databytes / RasterUtils.SHORT_BYTES];
    shortbuff.get(shortdata);

    db = new DataBufferShort(shortdata, shortdata.length);
    break;
  case DataBuffer.TYPE_USHORT:
    ShortBuffer ushortbuff = data.asShortBuffer();
    short[] ushortdata = new short[databytes / RasterUtils.SHORT_BYTES];
    ushortbuff.get(ushortdata);

    db = new DataBufferUShort(ushortdata, ushortdata.length);
    break;
  default:
    throw new GDALUtils.GDALException("Error trying to read raster.  Bad raster data type");
  }

  raster = Raster.createWritableRaster(sm, db, null);

  return raster;
}

public static int toRasterDataBufferType(int gdaldatatype)
{
  if (gdaldatatype == gdalconstConstants.GDT_Byte)
  {
    return DataBuffer.TYPE_BYTE;
  }
  if (gdaldatatype == gdalconstConstants.GDT_UInt16)
  {
    return DataBuffer.TYPE_USHORT;
  }
  if (gdaldatatype == gdalconstConstants.GDT_Int16)
  {
    return DataBuffer.TYPE_SHORT;
  }
  if (gdaldatatype == gdalconstConstants.GDT_UInt32)
  {
    return DataBuffer.TYPE_INT;
  }
  if (gdaldatatype == gdalconstConstants.GDT_Int32)
  {
    return DataBuffer.TYPE_INT;
  }
  if (gdaldatatype == gdalconstConstants.GDT_Float32)
  {
    return DataBuffer.TYPE_FLOAT;
  }
  if (gdaldatatype == gdalconstConstants.GDT_Float64)
  {
    return DataBuffer.TYPE_DOUBLE;
  }

  return DataBuffer.TYPE_UNDEFINED;
}

public static void swapBytes(byte[] bytes, int gdaldatatype)
{
  byte tmp;

  // 2 byte value... swap byte 1 with 2
  if ((gdaldatatype == gdalconstConstants.GDT_UInt16) ||
      (gdaldatatype == gdalconstConstants.GDT_Int16))
  {
    for (int i = 0; i < bytes.length; i += 2)
    {
      // swap 0 and 1
      tmp = bytes[i];
      bytes[i] = bytes[i + 1];
      bytes[i + 1] = tmp;
    }
  }
  // 4 byte value... swap bytes 1 & 4, 2 & 3
  else if ((gdaldatatype == gdalconstConstants.GDT_UInt32) ||
      (gdaldatatype == gdalconstConstants.GDT_Int32) ||
      (gdaldatatype == gdalconstConstants.GDT_Float32))
  {
    for (int i = 0; i < bytes.length; i += 4)
    {
      // swap 0 and 3
      tmp = bytes[i];
      bytes[i] = bytes[i + 3];
      bytes[i + 3] = tmp;

      // swap 1 and 2
      tmp = bytes[i + 1];
      bytes[i + 1] = bytes[i + 2];
      bytes[i + 2] = tmp;
    }
  }

  // 8 byte value... swap bytes 1 & 8, 2 & 7, 3 & 6, 4 & 5
  else if (gdaldatatype == gdalconstConstants.GDT_Float64)
  {
    for (int i = 0; i < bytes.length; i += 8)
    {
      // swap 0 and 7
      tmp = bytes[i];
      bytes[i] = bytes[i + 7];
      bytes[i + 7] = tmp;

      // swap 1 and 6
      tmp = bytes[i + 1];
      bytes[i + 1] = bytes[i + 6];
      bytes[i + 6] = tmp;

      // swap 2 and 5
      tmp = bytes[i + 2];
      bytes[i + 2] = bytes[i + 5];
      bytes[i + 5] = tmp;

      // swap 3 and 4
      tmp = bytes[i + 3];
      bytes[i + 3] = bytes[i + 4];
      bytes[i + 4] = tmp;
    }
  }
}

public static int toGDALDataType(int rasterType)
{
  switch (rasterType)
  {
  case DataBuffer.TYPE_BYTE:
    return gdalconstConstants.GDT_Byte;
  case DataBuffer.TYPE_SHORT:
    return gdalconstConstants.GDT_Int16;
  case DataBuffer.TYPE_USHORT:
    return gdalconstConstants.GDT_UInt16;
  case DataBuffer.TYPE_INT:
    return gdalconstConstants.GDT_Int32;
  case DataBuffer.TYPE_FLOAT:
    return gdalconstConstants.GDT_Float32;
  case DataBuffer.TYPE_DOUBLE:
    return gdalconstConstants.GDT_Float64;
  default:
    return gdalconstConstants.GDT_Unknown;
  }

}

public static Bounds getBounds(final Dataset image)
{
  // calculate zoom level for the image
  final double[] xform = image.GetGeoTransform();

  double pixelsizeLon = xform[1];
  double pixelsizeLat = -xform[5];

  final double w = xform[0];
  final double n = xform[3];

  final int pixelWidth = image.GetRasterXSize();
  final int pixelHeight = image.GetRasterYSize();

  return new Bounds(w, n - pixelsizeLat * pixelHeight, w + pixelsizeLon * pixelWidth, n);
}

public static double getnodata(Dataset src)
{
  final Double[] val = new Double[1];
  src.GetRasterBand(1).GetNoDataValue(val);
  return val[0];
}

public static double[] getnodatas(Dataset src)
{
  final int bands = src.GetRasterCount();
  final double[] nodatas = new double[bands];
  Arrays.fill(nodatas, Double.NaN);

  final Double[] val = new Double[1];
  for (int i = 1; i <= bands; i++)
  {
    final Band band = src.GetRasterBand(i);

    band.GetNoDataValue(val);
    if (val[0] != null)
    {
      nodatas[i - 1] = val[0];
    }

  }
  return nodatas;
}


public static void iowrite(Raster raster, String filename, String type)
{
  try
  {
    ImageIO.write(RasterUtils.makeBufferedImage(raster), type, new File(filename));
  }
  catch (IOException e)
  {
    e.printStackTrace();
  }
}

private static void copyToDataset(Dataset ds, Raster raster)
{
  final int type = GDALUtils.toGDALDataType(raster.getTransferType());

  ds.SetProjection(GDALUtils.EPSG4326);

  final int bands = raster.getNumBands();
  final int[] bandlist = new int[bands];

  final int width = raster.getWidth();
  final int height = raster.getHeight();

  //boolean interleaved = raster.getSampleModel() instanceof PixelInterleavedSampleModel;

  for (int i = 0; i < bands; i++)
  {
    bandlist[i] = i + 1; // remember, GDAL bands are 1's based
  }

  // data coming from getDataElements is always interleaved (pixel1, pixel2, pixel3...), so we need to make the
  // GDAL dataset also interleaved (using pixelstride, linestride, and bandstride)
  Object elements =
      raster.getDataElements(raster.getMinX(), raster.getMinY(), raster.getWidth(), raster.getHeight(), null);

  int pixelsize = gdal.GetDataTypeSize(type) / 8;
  int pixelstride = pixelsize * bands;
  int linestride = pixelstride * width;
  int bandstride = pixelsize;

  int imagesize = pixelsize * linestride * height;
  ByteBuffer bytes = ByteBuffer.allocateDirect(imagesize);
  bytes.order(ByteOrder.nativeOrder());

  if (type == gdalconstConstants.GDT_Byte)
  {
    bytes.put((byte[])elements);
  }
  else if (type == gdalconstConstants.GDT_Int16 || type == gdalconstConstants.GDT_UInt16)
  {
    ShortBuffer sb = bytes.asShortBuffer();
    sb.put((short[])elements);
  }
  else if (type == gdalconstConstants.GDT_Int32)
  {
    IntBuffer ib = bytes.asIntBuffer();
    ib.put((int[])elements);
  }
  else if (type == gdalconstConstants.GDT_Float32)
  {
    FloatBuffer fb = bytes.asFloatBuffer();
    fb.put((float[])elements);
  }
  else if (type == gdalconstConstants.GDT_Float64)
  {
    DoubleBuffer db = bytes.asDoubleBuffer();
    db.put((double[])elements);
  }
  ds.WriteRaster_Direct(0, 0, width, height, width, height, type, bytes, bandlist, pixelstride, linestride,
      bandstride);
}

public static void main(final String[] args) throws Exception
{
  interleaved();
  banded();
}

private static void interleaved()
{
  final int[] offsets = new int[]{0, 1, 2};
  final SampleModel model = new PixelInterleavedSampleModel(DataBuffer.TYPE_BYTE, 10, 10, 3, 3 * 10, offsets);

  WritableRaster raw = Raster.createWritableRaster(model, null);
  for (int y = 0, pixel = 0; y < raw.getHeight(); y++)
  {
    for (int x = 0; x < raw.getWidth(); x++, pixel++)
    {
      for (int b = 0; b < raw.getNumBands(); b++)
      {
        raw.setSample(x, y, b, pixel);
      }
    }
  }

  String path = System.getProperty("user.home") + "/tmp/raster/";

  GDALUtils.saveRaster(raw, path + "interleaved_raw.tif");
  iowrite(raw, path + "io_interleaved_raw.png", "png");

  Dataset ds = GDALUtils.toDataset(raw);

  GDALUtils.saveRaster(ds, path + "interleaved_dataset.tif");

  Raster raster = GDALUtils.toRaster(ds);

  GDALUtils.saveRaster(raster, path + "interleaved_raster.tif");
  iowrite(raster, path + "io_interleaved_raster.png", "png");

  Dataset ds2 = GDALUtils.toDataset(raster);

  GDALUtils.saveRaster(ds2, path + "interleaved_dataset2.tif");

  Raster raster2 = GDALUtils.toRaster(ds2);

  iowrite(raster2, path + "io_interleaved_raster2.png", "png");
  GDALUtils.saveRaster(raster2, path + "interleaved_raster2.tif");
}

private static void banded()
{
  final SampleModel model = new BandedSampleModel(DataBuffer.TYPE_BYTE, 10, 10, 3);

  WritableRaster raw = Raster.createWritableRaster(model, null);
  for (int y = 0, pixel = 0; y < raw.getHeight(); y++)
  {
    for (int x = 0; x < raw.getWidth(); x++, pixel++)
    {
      for (int b = 0; b < raw.getNumBands(); b++)
      {
        raw.setSample(x, y, b, pixel);
      }
    }
  }

  String path = System.getProperty("user.home") + "/tmp/raster/";

  GDALUtils.saveRaster(raw, path + "banded_raw.tif");
  iowrite(raw, path + "io_banded_raw.png", "png");

  Dataset ds = GDALUtils.toDataset(raw);

  GDALUtils.saveRaster(ds, path + "banded_dataset.tif");

  Raster raster = GDALUtils.toRaster(ds);

  iowrite(raster, path + "io_banded_raster.png", "png");
  GDALUtils.saveRaster(raster, path + "banded_raster.tif");

  Dataset ds2 = GDALUtils.toDataset(raster);

  GDALUtils.saveRaster(ds2, path + "banded_dataset.2tif");

  Raster raster2 = GDALUtils.toRaster(ds2);

  iowrite(raster2, path + "io_banded_raster2.png", "png");
  GDALUtils.saveRaster(raster2, path + "banded_raster2.tif");
}

@SuppressWarnings("unused")
public static class GDALException extends RuntimeException
{

  private static final long serialVersionUID = 1L;
  private final Exception origException;

  public GDALException(Exception e)
  {
    super();
    origException = e;
  }

  public GDALException(String msg)
  {
    super();
    origException = new Exception(msg);
  }

  @Override
  public void printStackTrace()
  {
    this.origException.printStackTrace();
  }
}
}
