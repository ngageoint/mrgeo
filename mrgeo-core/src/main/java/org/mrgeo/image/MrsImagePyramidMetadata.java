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

package org.mrgeo.image;

import org.apache.commons.lang.ArrayUtils;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.util.DefaultPrettyPrinter;
import org.mrgeo.pyramid.MrsPyramidMetadata;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.DataBuffer;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


/**
 * MrsImagePyramidMetada is the class that describes a MrGeo Pyramid.
 * The contents of the pyramid are generally rasters.  The
 * metadata object describes geospatial bounds, geospatial
 * tiled bounds, and pixel bounds of images.
 * 
 * A pyramid of imagery is a set of images produced from a
 * base zoom level and then all derived image levels from
 * the base.  For example, a set of images are used to produce
 * zoom level 12.  This builds out images in some datastore.
 * The images of 12 are used to create image for zoom level 11
 * and then zoom level 11 is used for 10 and so on until zoom level
 * 1 is created.  So, the pyramid exists from 12 to 1.  
 * 
 */
public class MrsImagePyramidMetadata extends MrsPyramidMetadata
{
  
  // logger for the class
  private static final Logger log = LoggerFactory.getLogger(MrsImagePyramidMetadata.class);

  // version
  private static final long serialVersionUID = 1L;

  
  /**
   * ImageMetadata is a container of the multiple types of
   * bounds for images. 
   */
  public static class ImageMetadata extends MrsPyramidMetadata.TileMetadata
  {
    private static final long serialVersionUID = 1L;

    // pixel bounds of the image (0, 0, width, height)
    public LongRectangle pixelBounds = null;
    
    // statistics of the image
    public ImageStats[] stats = null;

    // basic constructor
    public ImageMetadata()
    {
    }
    
    // For backward compatibility with older image pyramids that
    // still have a reference to the "image" property. It was renamed
    // to "name".
    public void setImage(final String image)
    {
      name = image;
    }
  } // end ImageMetadata
  
  
  /*
   * start globals section
   */
  
  private ImageMetadata[] imageData = null; // data specific to a single
  // pyramid-level image

  private int bands = 0; // number of bands in the image

  // default (pixel) value, by band. Geotools calls these defaults, but they are
  // really
  // the nodata value for each pixel
  private double[] defaultValues;
  private int tileType; // pixel type for the image

  private ImageStats[] stats; // min, max, mean, std dev of pixel values by band for the source resolution level image

  private Classification classification = Classification.Continuous;

  private String resamplingMethod;

  
  /*
   * end globals section
   */

  // types
  public enum Classification {
    Categorical, Continuous
  }

  /*
   * start methods
   */

  /**
   * Loading a metadata file from the local file system.  The objects of
   * the file are stored in a json format.  This enables the ObjectMapper
   * to parse out the values correctly.
   * 
   * @param file metadata file on the local file system to load
   * @return a valid MrsImagePyramidMetadata object
   * @throws JsonGenerationException
   * @throws JsonMappingException
   * @throws IOException
   */
  @Deprecated
  public static MrsImagePyramidMetadata load(final File file) throws JsonGenerationException,
  JsonMappingException, IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    final MrsImagePyramidMetadata metadata = mapper.readValue(file, MrsImagePyramidMetadata.class);

    // make sure the name of the pyramid is set correctly for where the
    // metadata file was pulled
    metadata.setPyramid("file://" + file.getParentFile().getAbsolutePath());

    return metadata;
  } // end load - File


  /**
   * Loading metadata from an InputStream.  The objects of
   * the file are stored in a json format.  This enables the ObjectMapper
   * to parse out the values correctly.
   * 
   * @param stream - the stream attached to the metadata input
   * @return a valid MrsImagePyramidMetadata object
   * @throws JsonGenerationException
   * @throws JsonMappingException
   * @throws IOException
   */
  public static MrsImagePyramidMetadata load(final InputStream stream) throws JsonGenerationException,
  JsonMappingException, IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    final MrsImagePyramidMetadata metadata = mapper.readValue(stream, MrsImagePyramidMetadata.class);
    return metadata;
  } // end load - InputStream
  

  /*
   * Helper functions for types used in rasters and databuffers 
   */
  
  public static int toBytes(final int tiletype)
  {
    switch (tiletype)
    {
    case DataBuffer.TYPE_BYTE:
    {
      return 1;
    }
    case DataBuffer.TYPE_FLOAT:
    {
      return RasterUtils.FLOAT_BYTES;
    }
    case DataBuffer.TYPE_DOUBLE:
    {
      return RasterUtils.DOUBLE_BYTES;
    }
    case DataBuffer.TYPE_INT:
    {
      return RasterUtils.INT_BYTES;
    }
    case DataBuffer.TYPE_SHORT:
    {
      return RasterUtils.SHORT_BYTES;
    }
    case DataBuffer.TYPE_USHORT:
    {
      return RasterUtils.USHORT_BYTES;
    }
    }

    return 0;
  } // end toBytes

  
  public static int toTileType(final String tiletype)
  {
    if (tiletype == "Byte")
    {
      return DataBuffer.TYPE_BYTE;
    }
    if (tiletype == "Float")
    {
      return DataBuffer.TYPE_FLOAT;
    }
    if (tiletype == "Double")
    {
      return DataBuffer.TYPE_DOUBLE;
    }
    if (tiletype == "Int")
    {
      return DataBuffer.TYPE_INT;
    }
    if (tiletype == "Short")
    {
      return DataBuffer.TYPE_SHORT;
    }
    if (tiletype == "UShort")
    {
      return DataBuffer.TYPE_USHORT;
    }

    return DataBuffer.TYPE_UNDEFINED;
  } // end toTileType
  

  public static String toTileTypeText(final int tiletype)
  {
    switch (tiletype)
    {
    case DataBuffer.TYPE_BYTE:
    {
      return "Byte";
    }
    case DataBuffer.TYPE_FLOAT:
    {
      return "Float";
    }
    case DataBuffer.TYPE_DOUBLE:
    {
      return "Double";
    }
    case DataBuffer.TYPE_INT:
    {
      return "Int";
    }
    case DataBuffer.TYPE_SHORT:
    {
      return "Short";
    }
    case DataBuffer.TYPE_USHORT:
    {
      return "UShort";
    }
    }

    return "Undefined";
  } // end toTileTypeText

  
  /*
   * end helper functions
   */

  
  /*
   * start get section
   */

  
  public int getBands()
  {
    return bands;
  }
  

  @JsonIgnore
  public double getPixelHeight(int zoom) {
    return TMSUtils.resolution(zoom, tilesize);
  }

  
  @JsonIgnore
  public double getPixelWidth(int zoom) {
    return TMSUtils.resolution(zoom, tilesize);
  }

  
  @JsonIgnore
  public double getDefaultValue(final int band)
  {
    if (band < getBands())
    {
      return defaultValues[band];
    }

    return Double.NaN;
  }

  
  @JsonIgnore
  public byte getDefaultValueByte(final int band)
  {
    if (band < getBands())
    {
      return Double.valueOf(defaultValues[band]).byteValue();
    }

    return 0;
  }
  
  
  @JsonIgnore
  public double getDefaultValueDouble(final int band)
  {
    return getDefaultValue(band);
  }

  
  @JsonIgnore
  public float getDefaultValueFloat(final int band)
  {
    if (band < getBands())
    {
      return Double.valueOf(defaultValues[band]).floatValue();
    }

    return Float.NaN;
  }

  
  @JsonIgnore
  public int getDefaultValueInt(final int band)
  {
    if (band < getBands())
    {
      return Double.valueOf(defaultValues[band]).intValue();
    }

    return 0;
  }

  
  @JsonIgnore
  public long getDefaultValueLong(final int band)
  {
    if (band < getBands())
    {
      return Double.valueOf(defaultValues[band]).longValue();
    }

    return 0;
  }

  
  public double[] getDefaultValues()
  {
    return defaultValues;
  }

  
  @JsonIgnore
  public byte[] getDefaultValuesByte()
  {
    final byte[] defaults = new byte[bands];
    for (int i = 0; i < bands; i++)
    {
      defaults[i] = Double.valueOf(defaultValues[i]).byteValue();
    }

    return defaults;
  }

  
  @JsonIgnore
  public double[] getDefaultValuesDouble()
  {
    return getDefaultValues();
  }

  
  @JsonIgnore
  public float[] getDefaultValuesFloat()
  {
    final float[] defaults = new float[bands];
    for (int i = 0; i < bands; i++)
    {
      defaults[i] = Double.valueOf(defaultValues[i]).floatValue();
    }

    return defaults;
  }

  
  @JsonIgnore
  public short getDefaultValueShort(final int band)
  {
    if (band < getBands())
    {
      return Double.valueOf(defaultValues[band]).shortValue();
    }

    return 0;
  }

  
  @JsonIgnore
  public int[] getDefaultValuesInt()
  {
    final int[] defaults = new int[bands];
    for (int i = 0; i < bands; i++)
    {
      defaults[i] = Double.valueOf(defaultValues[i]).intValue();
    }

    return defaults;
  }

  
  @JsonIgnore
  public long[] getDefaultValuesLong()
  {
    final long[] defaults = new long[bands];
    for (int i = 0; i < bands; i++)
    {
      defaults[i] = Double.valueOf(defaultValues[i]).longValue();
    }

    return defaults;
  }

  
  @JsonIgnore
  public short[] getDefaultValuesShort()
  {
    final short[] defaults = new short[bands];
    for (int i = 0; i < bands; i++)
    {
      defaults[i] = Double.valueOf(defaultValues[i]).shortValue();
    }

    return defaults;
  }

  @Override
  public String getName(final int zoomlevel)
  {
    if (imageData != null && zoomlevel < imageData.length && imageData[zoomlevel].name != null)
    {
      return imageData[zoomlevel].name;
    }
    return null;
  }

  public ImageMetadata[] getImageMetadata()
  {
    return imageData;
  }

  
  public LongRectangle getPixelBounds(final int zoomlevel)
  {
    if (imageData != null)
    {
      if (zoomlevel < imageData.length)
      {
        return imageData[zoomlevel].pixelBounds;
      }
      return getOrCreatePixelBounds(zoomlevel);
    }
    return null;
  }

  
  @Override
  public LongRectangle getTileBounds(final int zoomlevel)
  {
    if (imageData != null)
    {
      if (zoomlevel < imageData.length)
      {
        return imageData[zoomlevel].tileBounds;
      }

      // If we have _some_ tilebounds, calculate the bounds for the higher level
      return getOrCreateTileBounds(zoomlevel);
    }
    return null;
  }

  
  public ImageStats[] getImageStats(final int zoomlevel)
  {
    if (imageData != null)
    {
      if (zoomlevel < imageData.length)
      {
        return imageData[zoomlevel].stats;
      }

      // if we don't have this level, return the highest level available
      return imageData[imageData.length - 1].stats;
    }
    return null;
  }

  
  public ImageStats getImageStats(final int zoomlevel, int band)
  {

    if (imageData != null)
    {
      if (zoomlevel < imageData.length)
      {
        if (imageData[zoomlevel].stats != null && band < imageData[zoomlevel].stats.length) 
        {
          return imageData[zoomlevel].stats[band];
        }
      }
      if (imageData[imageData.length - 1].stats != null && band < imageData[imageData.length - 1].stats.length)
      {
        return imageData[imageData.length - 1].stats[band];
      }
    }
    return null;
  }

  
  public ImageStats getStats(int band)
  {
    return (stats == null) ? null : stats[band];
  }

  
  public ImageStats[] getStats()
  {
    return stats;
  }

  
  @Override
  public LongRectangle getOrCreateTileBounds(final int zoomlevel)
  {
    if (imageData != null && zoomlevel < imageData.length)
    {
      return imageData[zoomlevel].tileBounds;
    }

    LongRectangle tilebounds = getTileBounds(zoomlevel);
    if (tilebounds == null)
    {
      TMSUtils.Bounds b = new TMSUtils.Bounds(bounds.getMinX(), bounds.getMinY(),
          bounds.getMaxX(), bounds.getMaxY());

      TMSUtils.TileBounds tb = TMSUtils.boundsToTile(b, zoomlevel, tilesize);
      tilebounds = new LongRectangle(tb.w, tb.s, tb.e, tb.n);
    }
    return tilebounds;
  }

  
  public LongRectangle getOrCreatePixelBounds(final int zoomlevel)
  {  
    if (imageData != null && zoomlevel < imageData.length)
    {
      return imageData[zoomlevel].pixelBounds;
    }

//    TMSUtils.Bounds b = new TMSUtils.Bounds(bounds.getMinX(), bounds.getMinY(),
//        bounds.getMaxX(), bounds.getMaxY());

    TMSUtils.Pixel ll = TMSUtils.latLonToPixels(bounds.getMinX(), bounds.getMinY(), zoomlevel, tilesize);
    TMSUtils.Pixel ur = TMSUtils.latLonToPixels(bounds.getMaxX(), bounds.getMaxY(), zoomlevel, tilesize);

    return new LongRectangle(0, 0, ur.px - ll.px, ur.py - ll.py);
  }

  
  public int getTileType()
  {
    return tileType;
  }

  
  public Classification getClassification()
  {
    return classification;
  }

  
  /**
   * Return the minimum and maximum values from the statistics for the image at the requested zoom 
   * level
   * @param zoomLevel requested zoom level
   * @return array with position one containing the statistical minimum value and position two
   * containing the maximum value
   */
  public double[] getExtrema(final int zoomLevel)
  {
    double[] extrema = new double[3];
    ImageStats st = getImageStats(zoomLevel, 0);
    if (st != null)
    {
      extrema[0] = Math.max(0.0, st.min);
      extrema[1] = st.max;
    }
    else
    {
      log.warn("No statistics have been calculated on the requested image " + 
          pyramid + "/" + imageData[zoomLevel].name + ".  Using default range of 0.0 to 1.0...");
      extrema[0] = 0.0;
      extrema[1] = 1.0;
    }
    return extrema;
  }

  
  public String getResamplingMethod()
  {
    return resamplingMethod;
  }

  
  public void setResamplingMethod(String resamplingMethod)
  {
    this.resamplingMethod = resamplingMethod;
  }

  

  public void save(final OutputStream stream) throws JsonGenerationException, JsonMappingException,
  IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    try
    {
      DefaultPrettyPrinter pp = new DefaultPrettyPrinter();
      pp.indentArraysWith(new DefaultPrettyPrinter.Lf2SpacesIndenter());

      mapper.prettyPrintingWriter(pp).writeValue(stream, this);
    }
    catch (NoSuchMethodError e)
    {
      // if we don't have the pretty printer, just write the json
      mapper.writeValue(stream, this);
    }
  }
  
  
  /*
   * start set section
   */

  public void setBands(final int bands)
  {
    this.bands = bands;
  }

  
  public void setDefaultValues(final double[] defaultValues)
  {
    this.defaultValues = defaultValues;
  }

  
  @Override
  public void setName(final int zoomlevel, final String name)
  {
    if (imageData == null || zoomlevel > maxZoomLevel)
    {
      setMaxZoomLevel(zoomlevel);
    }
    imageData[zoomlevel].name = name;
  }

  
  public void setImageMetadata(final ImageMetadata[] metadata)
  {
    imageData = metadata;
  }

  
  @Override
  public void setMaxZoomLevel(final int zoomlevel)
  {
    if (imageData == null)
    {
      for (int i = 0; i <= zoomlevel; i++)
      {
        imageData = (ImageMetadata[]) ArrayUtils.add(imageData, new ImageMetadata());
      }
    }
    else if (zoomlevel < maxZoomLevel)
    {
      imageData = (ImageMetadata[]) ArrayUtils.subarray(imageData, 0, zoomlevel + 1);
    }
    else if (zoomlevel > maxZoomLevel)
    {
      for (int i = maxZoomLevel + 1; i <= zoomlevel; i++)
      {
        imageData = (ImageMetadata[]) ArrayUtils.add(imageData, new ImageMetadata());
      }
    }
    this.maxZoomLevel = zoomlevel;
  }

  
  public void setPixelBounds(final int zoomlevel, final LongRectangle pixelBounds)
  {
    if (imageData == null || zoomlevel > maxZoomLevel)
    {
      setMaxZoomLevel(zoomlevel);
    }
    imageData[zoomlevel].pixelBounds = pixelBounds;
  }

  
  @Override
  public void setTileBounds(final int zoomlevel, final LongRectangle tileBounds)
  {
    if (imageData == null || zoomlevel > maxZoomLevel)
    {
      setMaxZoomLevel(zoomlevel);
    }
    imageData[zoomlevel].tileBounds = tileBounds;
  }

  
  public void setImageStats(final int zoomlevel, final ImageStats[] stats)
  {
    if (imageData == null || zoomlevel > maxZoomLevel)
    {
      setMaxZoomLevel(zoomlevel);
    }
    imageData[zoomlevel].stats = stats;
  }

  
  public void setStats(final ImageStats[] stats)
  {
    this.stats = stats;
  }

  
  public void setTileType(final int tileType)
  {
    this.tileType = tileType;
  }

  
  /* Note: Commented out because having two versions of the setter, one with @JsonIgnore 
   * and one without resulted in tileType being serialized on some boxes and not in others
   * On the other hand, removing the @JsonIgnore resulted in 
   *        JsonMappingException: Conflicting setter definitions for property tileType
   * Since there were no users of this method, commenting out made sense.
   */
  //    public void setTileType(final String tileType)
  //    {
  //      this.tileType = toTileType(tileType);
  //    }

  public void setClassification(Classification classification)
  {
    this.classification = classification;
  }

  
    
} // end MrsImagePyramidMetadata

