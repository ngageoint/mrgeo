package org.mrgeo.utils;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.Driver;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconstConstants;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.*;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.*;
import java.util.Arrays;

public class GDALUtils
{
  // it's quicker (the EPSG doesn't change) to just hardcode this instead of trying to get it from GDAL/
  public static final String EPSG4326 = "GEOGCS[\"WGS 84\",DATUM[\"WGS_1984\",SPHEROID[\"WGS 84\",6378137,298.257223563,AUTHORITY[\"EPSG\",\"7030\"]],AUTHORITY[\"EPSG\",\"6326\"]],PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],UNIT[\"degree\",0.0174532925199433,AUTHORITY[\"EPSG\",\"9122\"]],AUTHORITY[\"EPSG\",\"4326\"]]\n";
  private static final Logger log = LoggerFactory.getLogger(GDALUtils.class);

  private static final String VSI_PREFIX = "/vsimem/";

  public static final String GDAL_LIBS;
  static
  {

    // TODO:  We're counting on the system to be set up correct for gdal.  We should look into doing the
    // distribution ourselves
//    System.load("/usr/lib/jni/libgdaljni.so");
//    System.load("/usr/lib/jni/libgdalconstjni.so");
//    System.load("/usr/lib/jni/libosrjni.so");
//    System.load("/usr/lib/libgdal.so");
    String[] libs = {"libgdaljni.so", "libgdalconstjni.so", "libosrjni.so", "libgdal.so"};

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
      log.warn("Can't load GDAL libraries, " + MrGeoConstants.GDAL_PATH + " not found in the mrgeo configuration.  This may or may not be a problem.");

//      System.out.println("java.library.path: ");
//      String property = System.getProperty("java.library.path");
//      StringTokenizer parser = new StringTokenizer(property, ";");
//      while (parser.hasMoreTokens()) {
//        System.out.println("  " + parser.nextToken());
//      }

      System.loadLibrary("gdal");
      GDAL_LIBS = null;
    }

    gdal.AllRegister();

    final int drivers = gdal.GetDriverCount();
    if (drivers == 0)
    {
      log.error("GDAL libraries were not loaded!  This probibly an error.");
    }

//    final SpatialReference sr = new SpatialReference();
//    sr.ImportFromEPSG(4326);
//    EPSG4326 = sr.ExportToWkt();
//
//    System.out.println(EPSG4326);
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
      GDALUtils.open(imagename);
      GDALUtils.close(imagename);

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
      GDALUtils.log.debug("Loading image with GDAL: {}", imagename);
      Dataset image = gdal.Open(uri.getPath());

      if (image != null)
      {
        GDALUtils.log.debug("  Image loaded successfully: {}", imagename);
        return image;
      }

      GDALUtils.log.debug("  Image not on the local filesystem, trying HDFS: {}", imagename);

      final Path p = new Path(uri);
      final FileSystem fs = HadoopFileUtils.getFileSystem(p);
      FileStatus stats = fs.getFileStatus(p);

      final InputStream is = fs.open(p);

      final byte[] bytes = IOUtils.toByteArray(is);

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
    catch (final URISyntaxException e)
    {
      throw new IOException("Error opening image file: " + imagename, e);
    }

    throw new IOException("Error opening image file: " + imagename);
  }

  public static void close(String imagename)
  {
    gdal.Unlink(GDALUtils.VSI_PREFIX + imagename);
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
    final Driver driver = gdal.GetDriverByName("MEM");
    final Dataset raster = driver.Create("InMem", width, height, bands, datatype);

    for (int i = 1; i <= raster.getRasterCount(); i++)
    {
      final double nodata = nodatas[i - 1];
      final Band band = raster.GetRasterBand(i);
      band.Fill(nodata);
      band.SetNoDataValue(nodata);
    }

    return raster;
  }

  public static void saveRaster(Dataset raster, String filename)
  {
    GDALUtils.saveRaster(raster, filename, "GTiff");
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

  public static void saveRaster(Raster raster, String filename, String type, long tx, long ty, int zoom, int tilesize, final double nodata)
  {
    saveRaster(raster, filename, type, tx, ty, zoom, tilesize, nodata, null);
  }

  public static void saveRaster(Raster raster, String filename, String type, long tx, long ty, int zoom, int tilesize, final double nodata, String[] options)
  {
    final Driver driver = gdal.GetDriverByName(type);
    final Dataset src = GDALUtils.toDataset(raster, nodata);

    final TMSUtils.Bounds tileBounds = TMSUtils.tileBounds(tx, ty, zoom, tilesize);
    final double res = TMSUtils.resolution(zoom, tilesize);

    final double[] xform = new double[6];

    xform[0] = tileBounds.w; /* top left x */
    xform[1] = res; /* w-e pixel resolution */
    xform[2] = 0; /* 0 */
    xform[3] = tileBounds.n; /* top left y */
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

    for (int b = 0; b < raster.getNumBands(); b++)
    {
      final Band band = ds.GetRasterBand(b + 1);

      Object elements = raster.getDataElements(raster.getMinX(), raster.getMinY(),
          raster.getWidth(), raster.getHeight(), null);

      if (type == gdalconstConstants.GDT_Byte)
      {
        band.WriteRaster(0, 0, raster.getWidth(),
            raster.getHeight(), (byte[]) elements);
      }
      else if (type == gdalconstConstants.GDT_Int16 || type == gdalconstConstants.GDT_UInt16)
      {
        band.WriteRaster(0, 0, raster.getWidth(),
            raster.getHeight(), (short[]) elements);
      }
      else if (type == gdalconstConstants.GDT_Int32)
      {
        band.WriteRaster(0, 0, raster.getWidth(),
            raster.getHeight(), (int[]) elements);
      }
      else if (type == gdalconstConstants.GDT_Float32)
      {
        band.WriteRaster(0, 0, raster.getWidth(),
            raster.getHeight(), (float[]) elements);
      }
      else if (type == gdalconstConstants.GDT_Float64)
      {
        band.WriteRaster(0, 0, raster.getWidth(),
            raster.getHeight(), (double[]) elements);
      }
    }

    return ds;
  }

  public static Raster toRaster(Dataset image)
  {
    final int bands = image.GetRasterCount();
    final int[] bandlist = new int[bands];
    for (int x = 0; x < bands; x++)
    {
      bandlist[x] = x;
    }

    final int datatype = image.GetRasterBand(1).getDataType();
    final int datasize = gdal.GetDataTypeSize(datatype) / 8;

    final int w = image.getRasterXSize();
    final int h = image.getRasterYSize();

    final ByteBuffer buf = ByteBuffer.allocateDirect(datasize * w * h * bands);
    image.ReadRaster_Direct(0, 0, w, h, w, h, datatype, buf, bandlist);

    return GDALUtils.toRaster(h, w, bands, datatype, buf.array());
  }

  public static Raster toRaster(int height, int width, int bands,
      int gdaldatatype, final byte[] data)
  {
    final int datatype = GDALUtils.toRasterDataBufferType(gdaldatatype);

    final SampleModel model = new BandedSampleModel(datatype, width, height, bands);

    // the corner of the raster is always 0,0
    final WritableRaster raster = Raster.createWritableRaster(model, null);

    ByteBuffer rasterBuffer = ByteBuffer.wrap(data);
    rasterBuffer.order(ByteOrder.nativeOrder());

    final int databytes = model.getHeight() * model.getWidth() * (gdal.GetDataTypeSize(gdaldatatype) / 8);


    switch (datatype)
    {
    case DataBuffer.TYPE_BYTE:
      // we can't use the byte buffer explicitly because the header info is
      // still in it...
      byte[] bytedata = new byte[databytes];
      rasterBuffer.get(bytedata);

      raster.setDataElements(0, 0, width, height, bytedata);
      break;
    case DataBuffer.TYPE_FLOAT:
      FloatBuffer floatbuff = rasterBuffer.asFloatBuffer();
      float[] floatdata = new float[databytes / RasterUtils.FLOAT_BYTES];

      floatbuff.get(floatdata);

      raster.setDataElements(0, 0, width, height, floatdata);
      break;
    case DataBuffer.TYPE_DOUBLE:
      DoubleBuffer doublebuff = rasterBuffer.asDoubleBuffer();
      double[] doubledata = new double[databytes / RasterUtils.DOUBLE_BYTES];

      doublebuff.get(doubledata);

      raster.setDataElements(0, 0, width, height, doubledata);

      break;
    case DataBuffer.TYPE_INT:
      IntBuffer intbuff = rasterBuffer.asIntBuffer();
      int[] intdata = new int[databytes / RasterUtils.INT_BYTES];

      intbuff.get(intdata);

      raster.setDataElements(0, 0, width, height, intdata);

      break;
    case DataBuffer.TYPE_SHORT:
    case DataBuffer.TYPE_USHORT:
      ShortBuffer shortbuff = rasterBuffer.asShortBuffer();
      short[] shortdata = new short[databytes / RasterUtils.SHORT_BYTES];
      shortbuff.get(shortdata);
      raster.setDataElements(0, 0, width, height, shortdata);
      break;
    default:
      throw new GDALUtils.GDALException("Error trying to read raster.  Bad raster data type");
    }

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
    if (gdaldatatype == gdalconstConstants.GDT_Byte)
    {
      // nothing to do for bytes...
      return;
    }

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

      return;
    }

    // 4 byte value... swap bytes 1 & 4, 2 & 3
    if ((gdaldatatype == gdalconstConstants.GDT_UInt32) ||
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

      return;
    }

    // 8 byte value... swap bytes 1 & 8, 2 & 7, 3 & 6, 4 & 5
    if (gdaldatatype == gdalconstConstants.GDT_Float64)
    {
      for (int i = 0; i < bytes.length; i += 2)
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
