package org.mrgeo.utils;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.Driver;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconstConstants;
import org.gdal.osr.SpatialReference;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.*;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.*;
import java.util.Arrays;

public class GDALUtils
{
  public static class GDALException extends RuntimeException
  {

    private static final long serialVersionUID = 1L;
    private final Exception origException;

    public GDALException(final Exception e)
    {
      this.origException = e;
    }

    public GDALException(final String msg)
    {
      final Exception e = new Exception(msg);
      this.origException = e;
    }

    @Override
    public void printStackTrace()
    {
      origException.printStackTrace();
    }
  }

  private static Logger log = LoggerFactory.getLogger(GDALUtils.class);

  private final static String VSI_PREFIX = "/vsimem/";

  public final static String epsg4326;
  static
  {

    System.load("/usr/lib/jni/libgdaljni.so");
    System.load("/usr/lib/jni/libgdalconstjni.so");
    System.load("/usr/lib/jni/libosrjni.so");
    System.load("/usr/lib/libgdal.so");

    gdal.AllRegister();


    SpatialReference sr = new SpatialReference();
    sr.ImportFromEPSG(4326);
    epsg4326 = sr.ExportToWkt();

  }

  public static void register()
  {
    // no-op to force the statics...
  }

  public static boolean isValidDataset(final String imagename)
  {
    try
    {
      Dataset d = open(imagename);

      close(imagename);

      return true;
    }
    catch (IOException ignored)
    {
    }

    return false;
  }

  public static Dataset open(final String imagename) throws IOException
  {
    try
    {
      URI f = new URI(imagename);
      log.debug("Loading image with GDAL: " + imagename);
      Dataset image = gdal.Open(f.getPath());

      if (image != null)
      {
        log.debug("  Image loaded successfully: " + imagename);
        return image;
      }

      Path p = new Path(f);
      FileSystem fs = HadoopFileUtils.getFileSystem(p);
      InputStream is = fs.open(p);

      byte[] bytes = IOUtils.toByteArray(is);
      gdal.FileFromMemBuffer(VSI_PREFIX + imagename, bytes);

      image = gdal.Open(VSI_PREFIX + imagename);
      if (image != null)
      {
        log.debug("  Image loaded successfully: " + imagename);
        return image;
      }

      log.info(
          "Image not loaded, but unfortunately no exceptions were thrown, look for a logged explanation somewhere above");
    }
    catch (URISyntaxException e)
    {
      throw new IOException("Error opening image file: " + imagename, e);
    }

    throw new IOException("Error opening image file: " + imagename);
  }

  public static void close(final String imagename) throws IOException
  {
    gdal.Unlink(VSI_PREFIX + imagename);
  }

  public static Dataset createEmptyMemoryRaster(Dataset src,int width, int height)
  {
    int datatype = src.GetRasterBand(1).getDataType();

    double nodatas[] = new double[src.getRasterCount()];
    Arrays.fill(nodatas, Double.NaN);

    for (int i = 1; i <= src.getRasterCount(); i++)
    {
      Double val[] = new Double[1];
      Band b = src.GetRasterBand(i);

      b.GetNoDataValue(val);
      if (val[0] != null)
      {
        nodatas[i - 1] = val[0];
      }

    }
    return createEmptyMemoryRaster(width, height, src.getRasterCount(), datatype, nodatas);
  }

  public static Dataset createEmptyMemoryRaster(int width, int height, int bands, int datatype, double[] nodatas)
  {
    Driver driver = gdal.GetDriverByName("MEM");
    Dataset raster = driver.Create("InMem", width, height, bands, datatype);

    for (int i = 1; i <= raster.getRasterCount(); i++)
    {
      Band b = raster.GetRasterBand(i);

      b.SetNoDataValue(nodatas[i - 1]);
    }

    return raster;
  }
  public static void saveRaster(final Dataset raster, final String filename)
  {
    saveRaster(raster, filename, "GTiff");
  }

  public static void saveRaster(final Dataset raster, final String filename, final String type)
  {
    Driver driver = gdal.GetDriverByName(type);
    Dataset copy = driver.CreateCopy(filename, raster);

    copy.delete();
  }

  public static void saveRaster(final Raster raster, final String filename, double nodata)
  {
    saveRaster(raster, filename, "GTiff", nodata);
  }

  public static void saveRaster(final Raster raster, final String filename, final String type, double nodata)
  {
    Driver driver = gdal.GetDriverByName(type);
    Dataset src = toDataset(raster, nodata);
    src.SetProjection(epsg4326);

    Dataset copy = driver.CreateCopy(filename, src);

    copy.delete();
    src.delete();
  }

  public static Dataset toDataset(final Raster raster, double nodata)
  {
    double[] nodatas = new double[raster.getNumBands()];
    Arrays.fill(nodatas, nodata);

    int type = toGDALDataType(raster.getTransferType());

    Dataset ds = createEmptyMemoryRaster(raster.getWidth(), raster.getHeight(),
          raster.getNumBands(), type, nodatas);

    for (int b = 0; b < raster.getNumBands(); b++)
    {
      Band band = ds.GetRasterBand(b + 1);

      System.out.println(band.GetRasterDataType());

      final Object elements = raster.getDataElements(raster.getMinX(), raster.getMinY(),
          raster.getWidth(), raster.getHeight(), null);

      if (type == gdalconstConstants.GDT_Byte)
      {
        band.WriteRaster(0, 0, 0, 0, raster.getWidth(),
            raster.getHeight(), raster.getWidth(), (byte[])elements);
      }
      else if (type == gdalconstConstants.GDT_Int16)
      {
        band.WriteRaster(0, 0, 0, 0, raster.getWidth(),
            raster.getHeight(), raster.getWidth(), (short[])elements);
      }
      else if (type == gdalconstConstants.GDT_UInt16)
      {
        band.WriteRaster(0, 0, 0, 0, raster.getWidth(),
            raster.getHeight(), raster.getWidth(), (short[])elements);
      }
      else if (type == gdalconstConstants.GDT_Int32)
      {
        band.WriteRaster(0, 0, 0, 0, raster.getWidth(),
            raster.getHeight(), raster.getWidth(), (int[])elements);
      }
      else if (type == gdalconstConstants.GDT_Float32)
      {
        band.WriteRaster(0, 0, 0, 0, raster.getWidth(),
            raster.getHeight(), raster.getWidth(), (float[])elements);
      }
      else if (type == gdalconstConstants.GDT_Float64)
      {
        band.WriteRaster(0, 0, 0, 0, raster.getWidth(),
            raster.getHeight(), raster.getWidth(), (double[])elements);
      }
    }

    return ds;
  }


  public static Raster toRaster(final Dataset image)
  {
    int bands = image.GetRasterCount();
    int bandlist[] = new int[bands];
    for (int x = 0; x < bands; x++)
    {
      bandlist[x] = x;
    }

    int datatype = image.GetRasterBand(1).getDataType();
    int datasize = gdal.GetDataTypeSize(datatype) / 8;

    int w = image.getRasterXSize();
    int h = image.getRasterYSize();

    ByteBuffer buf = ByteBuffer.allocateDirect(datasize * w * h * bands);
    image.ReadRaster_Direct(0, 0, w, h, w, h, datatype, buf, bandlist);

    return toRaster(h, w, bands, datatype, buf.array());
  }

  public static Raster toRaster(final int height, final int width, final int bands,
      final int gdaldatatype, byte[] data)
  {
    int datatype = toRasterDataBufferType(gdaldatatype);

    SampleModel model = new BandedSampleModel(datatype, width, height, bands);

    // the corner of the raster is always 0,0
    WritableRaster raster = Raster.createWritableRaster(model, null);

    final ByteBuffer rasterBuffer = ByteBuffer.wrap(data);
    rasterBuffer.order(ByteOrder.nativeOrder());

    int databytes = model.getHeight() * model.getWidth() * (gdal.GetDataTypeSize(gdaldatatype) / 8);


    switch (datatype)
    {
    case DataBuffer.TYPE_BYTE:
    {
      // we can't use the byte buffer explicitly because the header info is
      // still in it...
      final byte[] bytedata = new byte[databytes];
      rasterBuffer.get(bytedata);

      raster.setDataElements(0, 0, width, height, bytedata);
      break;
    }
    case DataBuffer.TYPE_FLOAT:
    {
      final FloatBuffer floatbuff = rasterBuffer.asFloatBuffer();
      final float[] floatdata = new float[databytes / RasterUtils.FLOAT_BYTES];

      floatbuff.get(floatdata);

      raster.setDataElements(0, 0, width, height, floatdata);
      break;
    }
    case DataBuffer.TYPE_DOUBLE:
    {
      final DoubleBuffer doublebuff = rasterBuffer.asDoubleBuffer();
      final double[] doubledata = new double[databytes / RasterUtils.DOUBLE_BYTES];

      doublebuff.get(doubledata);

      raster.setDataElements(0, 0, width, height, doubledata);

      break;
    }
    case DataBuffer.TYPE_INT:
    {
      final IntBuffer intbuff = rasterBuffer.asIntBuffer();
      final int[] intdata = new int[databytes / RasterUtils.INT_BYTES];

      intbuff.get(intdata);

      raster.setDataElements(0, 0, width, height, intdata);

      break;
    }
    case DataBuffer.TYPE_SHORT:
    case DataBuffer.TYPE_USHORT:
    {
      final ShortBuffer shortbuff = rasterBuffer.asShortBuffer();
      final short[] shortdata = new short[databytes / RasterUtils.SHORT_BYTES];
      shortbuff.get(shortdata);
      raster.setDataElements(0, 0, width, height, shortdata);
      break;
    }
    default:
      throw new GDALException("Error trying to read raster.  Bad raster data type");
    }

    return raster;
  }

  public static int toRasterDataBufferType(final int gdaldatatype)
  {
    if (gdaldatatype == gdalconstConstants.GDT_Byte)
    {
      return DataBuffer.TYPE_BYTE;
    }
    if (gdaldatatype ==  gdalconstConstants.GDT_UInt16)
    {
      return DataBuffer.TYPE_USHORT;
    }
    if (gdaldatatype ==  gdalconstConstants.GDT_Int16)
    {
      return DataBuffer.TYPE_SHORT;
    }
    if (gdaldatatype ==  gdalconstConstants.GDT_UInt32)
    {
      return DataBuffer.TYPE_INT;
    }
    if (gdaldatatype ==  gdalconstConstants.GDT_Int32)
    {
      return DataBuffer.TYPE_INT;
    }
    if (gdaldatatype ==  gdalconstConstants.GDT_Float32)
    {
      return DataBuffer.TYPE_FLOAT;
    }
    if (gdaldatatype ==  gdalconstConstants.GDT_Float64)
    {
      return DataBuffer.TYPE_DOUBLE;
    }

    return DataBuffer.TYPE_UNDEFINED;
  }

  public static int toGDALDataType(final int rasterType)
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
    }

    return gdalconstConstants.GDT_Unknown;
  }

  public static Bounds getBounds(Dataset image)
  {
    // calculate zoom level for the image
    double[] xform = image.GetGeoTransform();

    final double pixelsizeLon = xform[1];
    final double pixelsizeLat = -xform[5];

    double w = xform[0];
    double n = xform[3];

    int pixelWidth = image.GetRasterXSize();
    int pixelHeight = image.GetRasterYSize();

    return new Bounds(w, n - (pixelsizeLat * pixelHeight), w + (pixelsizeLon * pixelWidth), n);
  }
}
