/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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

import com.sun.media.jai.codec.TIFFEncodeParam;
import com.sun.media.jai.codec.TIFFField;
import org.apache.commons.lang.NotImplementedException;
import org.geotiff.image.jai.GeoTIFFDirectory;
import org.libtiff.jai.codec.XTIFFField;
import org.mrgeo.utils.Bounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.media.jai.operator.EncodeDescriptor;
import java.awt.image.DataBuffer;
import java.awt.image.RenderedImage;
import java.io.*;
import java.util.Vector;
import java.util.zip.Deflater;

public class GeoTiffExporter
{
  public static final int NULL_TAG = 42113;

  private static double default_nodata = -9999.0;

  @SuppressWarnings("unused")
  private static final Logger _log = LoggerFactory.getLogger(GeoTiffExporter.class);

  
  public static void exportTfw()
  {
    // TODO:  Implement this for real...
    //    BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file));
    //    
    //    String fn = file.getPath();
    //    String twfFn = fn.substring(0, fn.length() - 4) + ".tfw";
    //    
    //    PrintStream tfw = new PrintStream(new FileOutputStream(new File(twfFn)));
    //    tfw.printf("%.16g\n", gt.getPixelWidth());
    //    tfw.printf("0\n");
    //    tfw.printf("0\n");
    //    tfw.printf("%.16g\n", -gt.getPixelHeight());
    //    tfw.printf("%.16g\n", gt.convertToWorldX(0.5));
    //    tfw.printf("%.16g\n", gt.convertToWorldY(0.5));
    //    tfw.close();
    //    
    //    TIFFEncodeParam param = new TIFFEncodeParam();
    //    param.setCompression(TIFFEncodeParam.COMPRESSION_DEFLATE);
    //    param.setDeflateLevel(1);
    //    param.setTileSize(image.getTileWidth(), image.getTileHeight());
    //    param.setWriteTiled(true);
    //    
    //    String[] nullValues = new String[1];
    //    double newValue = Double.NaN;
    //    switch (image.getSampleModel().getDataType())
    //    {
    //    case DataBuffer.TYPE_DOUBLE:
    //      newValue = -Double.MAX_VALUE;
    //      nullValues[0] = String.format("%16f", -Double.MAX_VALUE);
    //      break;
    //    case DataBuffer.TYPE_FLOAT:
    //      newValue = -Float.MAX_VALUE;
    //      nullValues[0] = String.format("%16f", -Float.MAX_VALUE);
    //      break;
    //    case DataBuffer.TYPE_INT:
    //      newValue = Integer.MIN_VALUE;
    //      nullValues[0] = Integer.toString(Integer.MIN_VALUE);
    //      break;
    //    case DataBuffer.TYPE_SHORT:
    //      newValue = Short.MIN_VALUE;
    //      nullValues[0] = Short.toString(Short.MIN_VALUE);
    //      break;
    //    case DataBuffer.TYPE_BYTE:
    //      newValue = Byte.MIN_VALUE;
    //      nullValues[0] = Byte.toString(Byte.MIN_VALUE);
    //      break;
    //    }
    //    
    //    RenderedOp rop = ReplaceNullDescriptor.create(image, newValue, null);
    //    
    //    Vector<TIFFField> fields = new Vector<TIFFField>();
    //    fields.add(new TIFFField(GeoTiffExporter.NULL_TAG, XTIFFField.TIFF_ASCII, 1, nullValues));
    //    param.setExtraFields(fields.toArray(new TIFFField[0]));
    //
    //    EncodeDescriptor.create(rop, bos, "TIFF", param, null);
    //    bos.flush();
    //    bos.close();

    throw new NotImplementedException("Need to implemented export as TIFF/TFW");
  }
  public static void export(final RenderedImage image, final Bounds bounds, final File file)
      throws IOException
      {
    export(image, bounds, file, null, default_nodata);
      }

  public static void export(final RenderedImage image, final Bounds bounds, final File file,
    final boolean replaceNan)
        throws IOException
        {
    final BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file));

    export(image, bounds, bos, replaceNan, null, default_nodata);
    bos.flush();
    bos.close();
        }

  public static void export(final RenderedImage image, final Bounds bounds, final File file,
    final boolean replaceNan, final Number nodata)
        throws IOException
        {

    final BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file));
    export(image, bounds, bos, replaceNan, null, nodata);
    bos.flush();
    bos.close();
        }

  public static void export(final RenderedImage image, final Bounds bounds, final File file,
    final boolean replaceNan, final String xmp) throws IOException
    {
    final BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file));
    export(image, bounds, bos, replaceNan, xmp, default_nodata);
    bos.flush();
    bos.close();
    }

  public static void export(final RenderedImage image, final Bounds bounds, final File file,
    final Number nodata)
        throws IOException
        {
    export(image, bounds, file, null, nodata);
        }

  public static void export(final RenderedImage image, final Bounds bounds, final File file,
    final String xmp)
        throws IOException
        {
    export(image, bounds, file, xmp, default_nodata);
        }

  public static void export(final RenderedImage image, final Bounds bounds, final File file,
    final String xmp, final Number nodata)
        throws IOException
        {
    final BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file));

    export(image, bounds, bos, true, xmp, nodata);

    bos.flush();
    bos.close();
        }

  public static void export(final RenderedImage image, final Bounds bounds, final OutputStream os,
    final boolean replaceNan)
        throws IOException
        {
    export(image, bounds, os, replaceNan, null, default_nodata);
        }

  public static void export(final RenderedImage image, final Bounds bounds, final OutputStream os,
    final boolean replaceNan, final Number nodata)
        throws IOException
        {
    export(image, bounds, os, replaceNan, null, nodata);
        }

  public static void export(final RenderedImage image, final Bounds bounds, final OutputStream os,
    final boolean replaceNan, final String xmp) throws IOException
    {
    export(image, bounds, os, replaceNan, xmp, default_nodata);
    }

  public static void export(final RenderedImage image, final Bounds bounds, final OutputStream os,
    final boolean replaceNan, final String xmp, final Number nodata) throws IOException
    {
    OpImageRegistrar.registerMrGeoOps();

    final TIFFEncodeParam param = new TIFFEncodeParam();
    // The version of GDAL that Legion is using requires a tile size > 1
    param.setTileSize(image.getTileWidth(), image.getTileHeight());
    param.setWriteTiled(true);

    // if the image only has 1 pixel, the value of this pixel changes after compressing (especially
    // if this pixel is no data value. e.g -9999 changes to -8192 when read the image back).
    // So don't do compress if the image has only 1 pixel.
    if (image.getWidth() > 1 && image.getHeight() > 1)
    {
      // Deflate lossless compression (also known as "Zip-in-TIFF")
      param.setCompression(TIFFEncodeParam.COMPRESSION_DEFLATE);
      param.setDeflateLevel(Deflater.BEST_COMPRESSION);
    }

    final GeoTIFFDirectory dir = new GeoTIFFDirectory();

    
    // GTModelTypeGeoKey : using geographic coordinate system.
    dir.addGeoKey(new XTIFFField(1024, XTIFFField.TIFF_SHORT, 1, new char[] { 2 }));
    // GTRasterTypeGeoKey : pixel is point
    dir.addGeoKey(new XTIFFField(1025, XTIFFField.TIFF_SHORT, 1, new char[] { 1 }));
    // GeographicTypeGeoKey : 4326 WGS84
    dir.addGeoKey(new XTIFFField(2048, XTIFFField.TIFF_SHORT, 1, new char[] { 4326 }));
    dir.addGeoKey(new XTIFFField(2049, XTIFFField.TIFF_ASCII, 7, new String[] { "WGS 84" }));
    // GeogAngularUnitsGeoKey : Angular Degree
    dir.addGeoKey(new XTIFFField(2054, XTIFFField.TIFF_SHORT, 1, new char[] { 9102 }));
    if (xmp != null)
    {
      final byte[] b = xmp.getBytes("UTF8");
      dir.addField(new XTIFFField(700, XTIFFField.TIFF_BYTE, b.length, b));
    }
    dir.getFields();

    final double[] tiePoints = new double[6];
    tiePoints[0] = 0.0;
    tiePoints[1] = 0.0;
    tiePoints[2] = 0.0;
    tiePoints[3] = bounds.getMinX();
    tiePoints[4] = bounds.getMaxY();
    tiePoints[5] = 0.0;
    dir.setTiepoints(tiePoints);
    final double[] pixelScale = new double[3];
    pixelScale[0] = bounds.getWidth() / image.getWidth();
    pixelScale[1] = bounds.getHeight() / image.getHeight();
    pixelScale[2] = 0;
    dir.setPixelScale(pixelScale);

    final Vector<TIFFField> fields = toTiffField(dir.getFields());

    RenderedImage output = image;

    final String[] nullValues = new String[1];
    switch (image.getSampleModel().getDataType())
    {
    case DataBuffer.TYPE_DOUBLE:
      nullValues[0] = Double.toString(nodata.doubleValue());
      if (replaceNan)
      {
        output = ReplaceNanDescriptor.create(image, nodata.doubleValue());
      }
      // Tiff exporter doesn't handle doubles. Yuck!
      output = ConvertToFloatDescriptor.create(output);
      
      // Double.NaN (our default nodata on ingest) should not be written out as nodata on export
      // (i.e. GeoTiffs imported without NODATA metadata field should be exported as such)
      if (!Double.isNaN(nodata.doubleValue()))
      {
        fields.add(new TIFFField(NULL_TAG, XTIFFField.TIFF_ASCII, 1, nullValues));
      }
      break;
    case DataBuffer.TYPE_FLOAT:
      nullValues[0] = Double.toString(nodata.floatValue());
      if (replaceNan)
      {
        output = ReplaceNanDescriptor.create(image, nodata.floatValue());
      }
      // Float.NaN (our default nodata on ingest) should not be written out as nodata on export
      // (i.e. GeoTiffs imported without NODATA metadata field should be exported as such)
      if (!Float.isNaN(nodata.floatValue()))
      {
        fields.add(new TIFFField(NULL_TAG, XTIFFField.TIFF_ASCII, 1, nullValues));
      }
      break;
    case DataBuffer.TYPE_INT:
    case DataBuffer.TYPE_USHORT:
    case DataBuffer.TYPE_SHORT:
    case DataBuffer.TYPE_BYTE:
      nullValues[0] = Integer.toString(nodata.intValue());
      fields.add(new TIFFField(NULL_TAG, XTIFFField.TIFF_ASCII, 1, nullValues));
      break;
    }


    param.setExtraFields(fields.toArray(new TIFFField[0]));

    EncodeDescriptor.create(output, os, "TIFF", param, null);
    }


  public static Vector<TIFFField> toTiffField(final XTIFFField[] fields)
  {
    final Vector<TIFFField> result = new Vector<TIFFField>();
    for (final XTIFFField field : fields)
    {
      int count = field.getCount();
      Object obj;
      switch (field.getType())
      {
      case XTIFFField.TIFF_BYTE:
      case XTIFFField.TIFF_SBYTE:
        obj = field.getAsBytes();
        break;
      case XTIFFField.TIFF_SHORT:
        obj = field.getAsChars();
        break;
      case XTIFFField.TIFF_ASCII:
        obj = field.getAsStrings();
        final String[] tmp = (String[]) obj;
        final Vector<String> newStr = new Vector<String>();
        for (final String element : tmp)
        {
          if (element != null)
          {
            newStr.add(element);
          }
        }
        obj = newStr.toArray(new String[] {});
        count = newStr.size();
        break;
      case XTIFFField.TIFF_DOUBLE:
        obj = field.getAsDoubles();
        break;
      case XTIFFField.TIFF_FLOAT:
        obj = field.getAsFloats();
        break;
      case XTIFFField.TIFF_LONG:
        obj = field.getAsInts();
        break;
      case XTIFFField.TIFF_SLONG:
        obj = field.getAsLongs();
        break;
      case XTIFFField.TIFF_RATIONAL:
        obj = field.getAsRationals();
        break;
      case XTIFFField.TIFF_SSHORT:
        obj = field.getAsShorts();
        break;
      case XTIFFField.TIFF_SRATIONAL:
        obj = field.getAsSRationals();
        break;
      default:
        throw new ClassCastException();
      }
      result.add(new TIFFField(field.getTag(), field.getType(), count, obj));
    }

    return result;
  }
}
