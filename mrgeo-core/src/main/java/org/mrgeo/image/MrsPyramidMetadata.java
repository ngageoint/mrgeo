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

package org.mrgeo.image;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.ArrayUtils;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.util.DefaultPrettyPrinter;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.tms.Bounds;
import org.mrgeo.utils.tms.Pixel;
import org.mrgeo.utils.tms.TMSUtils;
import org.mrgeo.utils.tms.TileBounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.DataBuffer;
import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


/**
 * MrsPyramidMetada is the class that describes a MrGeo Pyramid.
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
public class MrsPyramidMetadata implements Serializable
{
private static final Logger log = LoggerFactory.getLogger(MrsPyramidMetadata.class);
// version
private static final long serialVersionUID = 1L;


/**
 * TileMetadata is a container of the multiple types of
 * bounds for tiled data.
 */
public static class TileMetadata implements Serializable
{
  private static final long serialVersionUID = 1L;

  // tile bounds (min tx, min ty, max tx, max ty)
  public LongRectangle tileBounds = null;

  // hdfs path or accumulo table for the image min, max, mean pixel values
  public String name = null;

  // basic constructor
  public TileMetadata()
  {
  }

} // end TileMetadata
  
  
  /*
   * start globals section
   */

protected String pyramid; // base hdfs path or accumulo table for the pyramid

// NOTE: bounds, and pixel bounds are for the exact image data, they do
// not include the extra "slop" around the edges of the tile.
protected Bounds bounds; // The bounds of the image in decimal degrees.

protected int tilesize; // tile width/height, in pixels

protected int maxZoomLevel; // maximum zoom level (minimum pixel size)

protected Map<String, String> tags = new HashMap<>(); // freeform k,v pairs of tags

protected String protectionLevel = "";

  /*
   * end globals section
   */

public MrsPyramidMetadata()
{
}

public MrsPyramidMetadata(MrsPyramidMetadata copy) {
  this.pyramid = copy.pyramid;
  this.bounds = copy.bounds == null? null : copy.bounds.clone();
  this.tilesize = copy.tilesize;
  this.maxZoomLevel = copy.maxZoomLevel;
  this.tags.putAll(copy.tags);
  this.protectionLevel = copy.protectionLevel;
  this.bands = copy.bands;
  this.defaultValues = ArrayUtils.clone(copy.defaultValues);
  this.tileType = copy.tileType;
  if (copy.quantiles == null)
  {
    this.quantiles = null;
  }
  else
  {
    quantiles = new double[copy.quantiles.length][];
    for (int b=0; b < copy.quantiles.length; b++)
    {
      if (copy.quantiles[b] != null)
      {
        this.quantiles[b] = copy.quantiles[b].clone();
      }
    }
  }
  if (copy.categories == null)
  {
    this.categories = null;
  }
  else
  {
    categories = new String[copy.categories.length][];
    for (int b=0; b < copy.categories.length; b++)
    {
      if (copy.categories[b] != null)
      {
        this.categories[b] = copy.categories[b].clone();
      }
    }
  }

  this.classification = copy.classification;
  this.resamplingMethod = copy.resamplingMethod;

  this.imageData = new ImageMetadata[copy.imageData.length];
  for (int i = 0; i < copy.imageData.length; i++)
  {
    this.imageData[i] = new ImageMetadata();
    if (copy.imageData[i] != null)
    {
      this.imageData[i].name = copy.imageData[i].name;
      if (copy.imageData[i].stats != null)
      {
        this.imageData[i].stats = ArrayUtils.clone(copy.imageData[i].stats);
      }
      if (copy.imageData[i].pixelBounds != null)
      {
        this.imageData[i].pixelBounds = new LongRectangle(copy.imageData[i].pixelBounds);
      }
      if (copy.imageData[i].tileBounds != null)
      {
        this.imageData[i].tileBounds = new LongRectangle(copy.imageData[i].tileBounds);
      }
    }
  }

  this.stats = ArrayUtils.clone(copy.stats);
}

  /*
   * start get section
   */


public Bounds getBounds()
{
  return bounds;
}

public String getName(final int zoomlevel)
{
  if (imageData != null && zoomlevel < imageData.length && imageData[zoomlevel].name != null)
  {
    return imageData[zoomlevel].name;
  }
  return null;
}

public int getMaxZoomLevel()
{
  return maxZoomLevel;
}


@JsonIgnore
public String getPyramid()
{
  return pyramid;
}


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


public int getTilesize()
{
  return tilesize;
}

public Map<String, String> getTags()
{
  return tags;
}

@JsonIgnore
public String getTag(final String tag)
{
  return getTag(tag, null);
}

@JsonIgnore
public String getTag(final String tag, final String defaultValue)
{
  if (tags.containsKey(tag))
  {
    return tags.get(tag);
  }

  return defaultValue;
}

public String getProtectionLevel()
{
  return protectionLevel;
}

/**
 * Returns the quantiles for the specified band for this image. Note that the return value
 * can be null.
 */
@JsonIgnore
@SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "api")
public double[] getQuantiles(int band)
{
  if (quantiles != null && band < getBands())
  {
    return quantiles[band];
  }
  return null;
}

public double[][] getQuantiles()
{
  if (quantiles == null)
  {
    return null;
  }


  double[][] q = new double[quantiles.length][];
  for (int i = 0; i < quantiles.length; i++)
  {
    if (quantiles[i] != null)
    {
      q[i] = ArrayUtils.clone(quantiles[i]);
    }
  }

  return q;
}

/**
 * Returns the categories for the specified band for this image. Note that the return value
 * can be null.
 */
@JsonIgnore
@SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "api")
public String[] getCategories(int band)
{
  if (categories != null && band < getBands())
  {
    return categories[band];
  }
  return null;
}

public String[][] getCategories()
{
  if (categories == null)
  {
    return null;
  }


  String[][] c = new String[categories.length][];
  for (int i = 0; i < categories.length; i++)
  {
    if (categories[i] != null)
    {
      c[i] = ArrayUtils.clone(categories[i]);
    }
  }

  return c;
}

  /*
   * end get section
   */

  
  /*
   * start set section
   */

public void setBounds(final Bounds bounds)
{
  this.bounds = bounds;
}


public void setName(final int zoomlevel)
{
  setName(zoomlevel, Integer.toString(zoomlevel));
}


public void setName(final int zoomlevel, final String name)
{
  if (imageData == null || zoomlevel > maxZoomLevel)
  {
    setMaxZoomLevel(zoomlevel);
  }
  imageData[zoomlevel].name = name;
}


public void setPyramid(final String pyramid)
{
  this.pyramid = pyramid;
}


public void setTilesize(final int size)
{
  tilesize = size;
}

public void setTags(final Map<String, String> tags)
{
  // make a copy of the tags...
  this.tags = new HashMap<>(tags);
}

@JsonIgnore
public void setTag(final String tag, final String value)
{
  tags.put(tag, value);
}

public void setProtectionLevel(final String protectionLevel)
{
  this.protectionLevel = protectionLevel;
}

public void setQuantiles(double[][] quantiles)
{
  if (quantiles == null)
  {
    this.quantiles = new double[getBands()][];
  }
  else
  {
    this.quantiles = new double[quantiles.length][];
    for (int i = 0; i < quantiles.length; i++)
    {
      if (quantiles[i] != null)
      {
        this.quantiles[i] = ArrayUtils.clone(quantiles[i]);
      }
    }
  }
}

@JsonIgnore
public void setQuantiles(final int band, final double[] quantiles)
{
  if (this.quantiles == null) {
    this.quantiles = new double[getBands()][];
  }
  this.quantiles[band] = quantiles.clone();
}

public void setCategories(String[][] categories)
{
  if (categories == null)
  {
    this.categories = new String[getBands()][];
  }
  else
  {
    this.categories = new String[categories.length][];
    for (int i = 0; i < categories.length; i++)
    {
      if (categories[i] != null)
      {
        this.categories[i] = ArrayUtils.clone(categories[i]);
      }
    }
  }
}

@JsonIgnore
public void setCategories(final int band, final String[] categories)
{
  if (this.categories == null) {
    this.categories = new String[getBands()][];
  }
  this.categories[band] = categories.clone();
}
  /*
   * end set section
   */

/**
 * Return true if there is data at each of the pyramid levels.
 */
public boolean hasPyramids()
{
  int levels = 0;
  if (getMaxZoomLevel() > 0)
  {
    for (int i = 1; i <= getMaxZoomLevel(); i++)
    {
      if (getName(i) != null)
      {
        levels++;
      }
    }
  }
  return (levels == getMaxZoomLevel());
}

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
private double[][] quantiles; // quantiles computed in each band for the entire image pyramid

// default (pixel) value, by band. Geotools calls these defaults, but they are
// really
// the nodata value for each pixel
private double[] defaultValues;
private int tileType; // pixel type for the image

private ImageStats[] stats; // min, max, mean, std dev of pixel values by band for the source resolution level image

private String[][] categories; // categories for each band in the image

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
 * @return a valid MrsPyramidMetadata object
 * @throws JsonGenerationException
 * @throws JsonMappingException
 * @throws IOException
 */
@Deprecated
public static MrsPyramidMetadata load(final File file) throws IOException
{
  final ObjectMapper mapper = new ObjectMapper();
  final MrsPyramidMetadata metadata = mapper.readValue(file, MrsPyramidMetadata.class);

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
 * @return a valid MrsPyramidMetadata object
 * @throws JsonGenerationException
 * @throws JsonMappingException
 * @throws IOException
 */
public static MrsPyramidMetadata load(final InputStream stream) throws IOException
{
  final ObjectMapper mapper = new ObjectMapper();
  return mapper.readValue(stream, MrsPyramidMetadata.class);
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
  if (tiletype.equals("Byte"))
  {
    return DataBuffer.TYPE_BYTE;
  }
  if (tiletype.equals("Float"))
  {
    return DataBuffer.TYPE_FLOAT;
  }
  if (tiletype.equals("Double"))
  {
    return DataBuffer.TYPE_DOUBLE;
  }
  if (tiletype.equals("Int"))
  {
    return DataBuffer.TYPE_INT;
  }
  if (tiletype.equals("Short"))
  {
    return DataBuffer.TYPE_SHORT;
  }
  if (tiletype.equals("UShort"))
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
  return  ArrayUtils.clone(defaultValues);
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

@JsonIgnore
public Number[] getDefaultValuesNumber()
{
  final Number[] defaults = new Number[bands];
  for (int i = 0; i < bands; i++)
  {
    defaults[i] = defaultValues[i];
  }

  return defaults;
}

public ImageMetadata[] getImageMetadata()
{
  return ArrayUtils.clone(imageData);
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

@SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "api")
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
  return ArrayUtils.clone(stats);
}


public LongRectangle getOrCreateTileBounds(final int zoomlevel)
{
  if (imageData != null && zoomlevel < imageData.length)
  {
    return imageData[zoomlevel].tileBounds;
  }

  LongRectangle tilebounds = getTileBounds(zoomlevel);
  if (tilebounds == null)
  {
    TileBounds tb = TMSUtils.boundsToTile(bounds, zoomlevel, tilesize);
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

  Pixel ll = TMSUtils.latLonToPixels(bounds.w, bounds.s, zoomlevel, tilesize);
  Pixel ur = TMSUtils.latLonToPixels(bounds.e, bounds.n, zoomlevel, tilesize);

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
    extrema[0] = st.min;
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



public void save(final OutputStream stream) throws IOException
{
  final ObjectMapper mapper = new ObjectMapper();
  try
  {
    DefaultPrettyPrinter pp = new DefaultPrettyPrinter();
    pp.indentArraysWith(new DefaultPrettyPrinter.Lf2SpacesIndenter());

    //mapper.prettyPrintingWriter(pp).writeValue(stream, this);
    mapper.writer(pp).writeValue(stream, this);
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
  this.defaultValues = ArrayUtils.clone(defaultValues);
}

//public void setDefaultValues(final Number[] defaultValues)
//{
//  this.defaultValues = new double[defaultValues.length];
//  for (int i = 0; i < defaultValues.length; i++)
//  {
//    this.defaultValues[i] = defaultValues[i].doubleValue();
//  }
//}


public void setImageMetadata(final ImageMetadata[] metadata)
{
  // this will make sure the size of the image metadata matches the zoom, with empty levels as needed
  if (metadata == null)
  {
    imageData = null;
    for (int i = 0; i <= maxZoomLevel; i++)
    {
      imageData = ArrayUtils.add(imageData, new ImageMetadata());
    }

    return;
  }

  // this could be the case when reading the data in, but the maxzoom comes after the imagedata
  // in the JSON
  if (maxZoomLevel <= 0)
  {
    setMaxZoomLevel(metadata.length - 1);
  }

  imageData = metadata.clone();
  if ((maxZoomLevel + 1) < imageData.length)
  {
    imageData = ArrayUtils.subarray(metadata, 0, maxZoomLevel + 1);
  }
  else if (maxZoomLevel > imageData.length)
  {
    for (int i = imageData.length; i <= maxZoomLevel; i++)
    {
      imageData = ArrayUtils.add(imageData, new ImageMetadata());
    }
  }

}


public void setMaxZoomLevel(final int zoomlevel)
{
  if (imageData == null)
  {
    for (int i = 0; i <= zoomlevel; i++)
    {
      imageData = ArrayUtils.add(imageData, new ImageMetadata());
    }
  }
  else if (zoomlevel < maxZoomLevel)
  {
    imageData = ArrayUtils.subarray(imageData, 0, zoomlevel + 1);
  }
  else if (zoomlevel > maxZoomLevel)
  {
    for (int i = maxZoomLevel + 1; i <= zoomlevel; i++)
    {
      imageData = ArrayUtils.add(imageData, new ImageMetadata());
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
  imageData[zoomlevel].stats = ArrayUtils.clone(stats);
}


public void setStats(final ImageStats[] stats)
{
  this.stats = ArrayUtils.clone(stats);
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



} // end MrsPyramidMetadata

