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

package org.mrgeo.colorscale;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.mrgeo.utils.FloatUtils;
import org.mrgeo.utils.XmlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import java.awt.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

/**
 * @author jason.surratt
 *         <p/>
 *         Example file format: <ColorMap name="My Color Map"> <Scaling>MinMax</Scaling> <!-defaults
 *         to Absolute --> <ReliefShading>0</ReliefShading> <!-- defaults to 0/false -->
 *         <Interpolate>1</Interpolate> <!-- defaults to 1/true --> <NullColor color="0,0,0"
 *         opacity="0"> <!-- defaults to transparent --> <Color value="0.0" color="255,0,0"
 *         opacity="255"> <Color value="0.2" color="255,255,0"> <!-- if not specified an opacity
 *         defaults to 255 --> <Color value="0.4" color="0,255,0"> <Color value="0.6"
 *         color="0,255,255"> <Color value="0.8" color="0,0,255"> <Color value="1.0"
 *         color="255,0,255"> </ColorMap>
 */
public class ColorScale extends TreeMap<Double, Color>
{
  private static final Logger log = LoggerFactory.getLogger(ColorScale.class);
private static final long serialVersionUID = 1L;
private int CACHE_SIZE = 1024;
private static final int A = 3;
private static final int B = 2;
private static final int G = 1;
private static final int R = 0;
private static final Object lock = new Object();
private static ColorScale _colorScale;
private static ColorScale _grayScale;
private int[][] cache;
private boolean interpolate;
private Double min, max;
private int[] nullColor = {0, 0, 0, 0};
private boolean reliefShading;
private Scaling scaling = Scaling.Absolute;
private boolean forceValuesIntoRange;
private double transparent = Double.NaN;
private String name;
private String title;
private String description;
private double[] quantiles;
private ColorScale[] quantileSubScales;

private ColorScale()
{
  clear();
}

public static ColorScale loadFromXML(InputStream stream) throws ColorScaleException
{
  ColorScale cs = new ColorScale();
  cs.fromXML(stream);

  return cs;
}

//  public static ColorScale loadFromXML(String filename) throws ColorScaleException
//  {
//    try
//    {
//      InputStream stream = null;
//      try
//      {
//        stream = new FileInputStream(filename);
//        ColorScale cs = new ColorScale();
//        cs.fromXML(stream);
//
//        return cs;
//      }
//      finally
//      {
//        if (stream != null)
//        {
//          IOUtils.closeQuietly(stream);
//        }
//      }
//    }
//    catch (IOException e)
//    {
//      e.printStackTrace();
//      throw new ColorScaleException(e);
//    }
//  }
//
//  public static ColorScale loadFromJSON(InputStream stream) throws ColorScaleException
//  {
//    ColorScale cs = new ColorScale();
//    cs.fromJSON(stream);
//    return cs;
//  }

public static ColorScale loadFromJSON(String json) throws ColorScaleException
{
  ColorScale cs = new ColorScale();
  cs.fromJSON(json);
  return cs;
}

public static ColorScale empty()
{
  return new ColorScale();
}

/**
 * Method returns a default color scale.
 *
 * @return ColorScale An object representing the color scale.
 */
public synchronized static ColorScale createDefault()
{
  // Make sure the color scale has been initiated before returning
  if (_colorScale == null) //  || _colorScale.isEmpty())
  {
    _colorScale = new ColorScale();
    _colorScale.setDefaultValues();
  }
  return (ColorScale) _colorScale.clone();
}

public synchronized static ColorScale createDefaultGrayScale()
{
  // Make sure the color scale has been initiated before returning
  if (_grayScale == null || _grayScale.isEmpty())
  {
    _grayScale = new ColorScale();
    _grayScale.setDefaultGrayScaleValues();
  }
  return (ColorScale) _grayScale.clone();
}

/**
 * Method to set the default color scale. The source for the color scale can come from either an
 * xml file or from hard coded values in the absent of the file.
 *
 * @param colorScale The URI of the color xml file.
 */
public synchronized static void setDefault(ColorScale colorScale)
{
  _colorScale = colorScale;

  if (_colorScale == null)
  {
    _colorScale = new ColorScale();
    _colorScale.setDefaultValues();
  }
}

private static int parseOpacity(String opacityStr)
{
  if (opacityStr != null && !opacityStr.isEmpty())
  {
    return Integer.parseInt(opacityStr);
  }
  else
  {
    return 255;
  }
}

private static void parseColor(String colorStr, String opacityStr, int[] color)
    throws IOException
{
  String[] colors = colorStr.split(",");
  if (colors.length == 3) {
    color[0] = Integer.parseInt(colors[0]);
    color[1] = Integer.parseInt(colors[1]);
    color[2] = Integer.parseInt(colors[2]);
    color[3] = parseOpacity(opacityStr);
  }
  else if (colors.length == 1) {
    // Allows colors to be specified in hex as #0F0F0F for example
    int c = Integer.decode(colors[0]);
    color[0] = (c & 0xFF0000) >> 16;
    color[1] = (c & 0x00FF00) >> 8;
    color[2] = (c & 0x0000FF);
    color[3] = parseOpacity(opacityStr);
  }
  else {
    throw new IOException("Error parsing XML: There must be three elements in color.");
  }
}

public String getName()
{
  return name;
}

public void setName(String name)
{
  this.name = name;
}


public String getTitle()
{
  return title;
}

//  public static ColorScale loadFromJSONFile(String filename) throws ColorScaleException
//  {
//    try
//    {
//      InputStream stream;
//      try
//      {
//       stream = HadoopFileUtils.open(new Path(filename));
//      }
//      catch (FileNotFoundException e)
//      {
//        // if the file wasn't found, try a local file
//        LocalFileSystem lf = FileSystem.getLocal(HadoopUtils.createConfiguration());
//        stream = lf.open(new Path(filename));
//      }
//      ColorScale cs = new ColorScale();
//      cs.fromJSON(stream);
//      IOUtils.closeQuietly(stream);
//
//      return cs;
//    }
//    catch (IOException e)
//    {
//      e.printStackTrace();
//      throw new ColorScaleException(e);
//    }
//  }

public String getDescription()
{
  return description;
}

@Override
public void clear()
{
  super.clear();
  scaling = Scaling.Absolute;
  reliefShading = false;
  interpolate = true;
  forceValuesIntoRange = false;
  min = null;
  max = null;
  cache = null;
}
//
//  public ColorScale(final InputStream strm) throws ColorScaleException
//  {
//    load(strm);
//  }
//
//  public ColorScale(final String filename) throws ColorScaleException
//  {
//    load(filename);
//  }

@Override
public Object clone()
{
  super.clone();

  ColorScale result = new ColorScale();
  result.cache = null;
  result.interpolate = interpolate;
  result.min = min;
  result.max = max;
  result.nullColor = nullColor.clone();
  result.reliefShading = reliefShading;
  result.scaling = scaling;
  result.transparent = transparent;
  result.forceValuesIntoRange = forceValuesIntoRange;
  result.putAll(this);

  return result;
}

@Override
public boolean equals(Object obj)
{
  return obj instanceof ColorScale && equals((ColorScale) obj);
}

@Override
public int hashCode()
{
  return new HashCodeBuilder(29, 5)
      .append(interpolate)
      .append(min)
      .append(max)
      .append(reliefShading)
      .append(nullColor)
      .append(forceValuesIntoRange)
      .append(transparent)
      .append(name)
      .append(title)
      .append(description).toHashCode();
}

public boolean equals(ColorScale cs)
{
  if (min == null && cs.min != null || min != null && cs.min == null)
  {
    return false;
  }
  if ((min != null) && Double.compare(min, cs.min) != 0)
  {
    return false;
  }
  if (max == null && cs.max != null || max != null && cs.max == null)
  {
    return false;
  }
  if ((max != null) && Double.compare(max, cs.max) != 0)
  {
    return false;
  }
  if (!scaling.equals(cs.scaling))
  {
    return false;
  }
  if (interpolate != cs.interpolate)
  {
    return false;
  }
  if (forceValuesIntoRange != cs.forceValuesIntoRange)
  {
    return false;
  }
  if (reliefShading != cs.reliefShading)
  {
    return false;
  }
  if (nullColor.length != cs.nullColor.length)
  {
    return false;
  }
  for (int i = 0; i < nullColor.length; i++)
  {
    if (nullColor[i] != cs.nullColor[i])
    {
      return false;
    }
  }
  if (size() != cs.size())
  {
    return false;
  }
  Iterator<Double> iterator2 = cs.keySet().iterator();
  for (Map.Entry<Double, Color> iterator1 : entrySet())
  {
    Double d2 = iterator2.next();
    if (iterator1.getKey().compareTo(d2) != 0)
    {
      return false;
    }

    if (!iterator1.getValue().equals(get(d2)))
    {
      return false;
    }
  }
  if (!Arrays.equals(quantiles, cs.quantiles)) {
    return false;
  }
  return Arrays.equals(quantileSubScales, cs.quantileSubScales);
}

public boolean getForceValuesIntoRange()
{
  return forceValuesIntoRange;
}

/**
 * If true, values below the min will be assigned the min color while those above the max will be
 * assigned the max color. If false, values outside the min/max range will be assigned the nodata
 * color.
 *
 * @param i If true interpolation is enabled.
 */
public void setForceValuesIntoRange(boolean i)
{
  forceValuesIntoRange = i;
}

public boolean getInterpolate()
{
  return interpolate;
}

/**
 * If true, the colors in the color scale will be interpolated. For instance if 0 is mapped to
 * black and 1 is mapped to white then .5 would be a grey. If interpolate is set to false then the
 * nearest color that is <= to the value will be used. E.g. .5 would be black.
 *
 * @param i If true interpolation is enabled.
 */
public void setInterpolate(boolean i)
{
  interpolate = i;
  cache = null;
}

public Double getMax()
{
  return max;
}

public Double getMin()
{
  return min;
}

public int[] getNullColor()
{
  return nullColor.clone();
}

public void setNullColor(int[] color)
{
  nullColor = color.clone();
}

final public boolean getReliefShading()
{
  return reliefShading;
}

public Scaling getScaling()
{
  return scaling;
}

public void setScaling(Scaling s)
{
  scaling = s;
  cache = null;
}

public double getTransparent()
{
  return transparent;
}

public void setTransparent(double transparent)
{
  this.transparent = transparent;
}

public void fromXML(Document doc) throws ColorScaleException
{
  try
  {
    clear();

    XPath xpath = XmlUtils.createXPath();

    name = xpath.evaluate("/ColorMap/@name", doc);

    Node nodeTitle = (Node) xpath.evaluate("/ColorMap/Title", doc, XPathConstants.NODE);
    if (nodeTitle != null)
    {
      title = xpath.evaluate("text()", nodeTitle);
    }

    Node nodeDesc = (Node) xpath.evaluate("/ColorMap/Description", doc, XPathConstants.NODE);
    if (nodeDesc != null)
    {
      description = xpath.evaluate("text()", nodeDesc);
    }

    scaling = Scaling.valueOf(xpath.evaluate("/ColorMap/Scaling/text()", doc));
    String reliefShadingStr = xpath.evaluate("/ColorMap/ReliefShading/text()", doc)
        .toLowerCase();
    reliefShading = reliefShadingStr.equals("1") || reliefShadingStr.equals("true");

    String interpolateStr = xpath.evaluate("/ColorMap/Interpolate/text()", doc)
        .toLowerCase();
    interpolate = interpolateStr.isEmpty() || (interpolateStr.equals("1") || interpolateStr.equals("true"));

    String forceStr = xpath.evaluate("/ColorMap/ForceValuesIntoRange/text()", doc)
        .toLowerCase();
    forceValuesIntoRange = (forceStr.equals("1") || forceStr.equals("true"));

    Node nullColorNode = (Node) xpath.evaluate("/ColorMap/NullColor", doc,
        XPathConstants.NODE);
    if (nullColorNode != null)
    {
      String colorStr = xpath.evaluate("@color", nullColorNode);
      String opacityStr = xpath.evaluate("@opacity", nullColorNode);
      parseColor(colorStr, opacityStr, nullColor);
    }

    int[] color = new int[4];
    XPathExpression expr = xpath.compile("/ColorMap/Color");
    NodeList nodes = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
    for (int i = 0; i < nodes.getLength(); i++)
    {
      Node node = nodes.item(i);

      String valueStr = xpath.evaluate("@value", node);
      String colorStr = xpath.evaluate("@color", node);
      String opacityStr = xpath.evaluate("@opacity", node);

      if (valueStr.isEmpty())
      {
        throw new IOException("Error parsing XML: A value must be specified for a color element.");
      }

      double value = Double.valueOf(valueStr);
      parseColor(colorStr, opacityStr, color);
      put(value, color);
    }
    cache = null;
  }
  catch (XPathExpressionException | IOException e)
  {
    log.error("Got XML error " + e.getMessage(), e);
    throw new BadXMLException(e);
  }
}

public void fromXML(InputStream strm) throws ColorScaleException
{
  try
  {
    fromXML(XmlUtils.parseInputStream(strm));
  }
  catch (Exception e)
  {
    log.error("Got exception while parsing color scale " + e.getMessage(), e);
    throw new ColorScaleException(e);
  }
}

//  public void load(final String filename) throws ColorScaleException
//  {
//    try
//    {
//      load(new FileInputStream(new File(filename)));
//    }
//    catch (final Exception e)
//    {
//      throw new BadFileException(e);
//    }
//  }

public void fromJSON(InputStream stream) throws ColorScaleException
{
  try
  {
    BufferedReader reader = new BufferedReader(new InputStreamReader(stream));

    StringBuilder builder = new StringBuilder();
    String line;
    while ((line = reader.readLine()) != null)
    {
      builder.append(line);
    }

    fromJSON(builder.toString());
  }
  catch (IOException e)
  {
    throw new ColorScaleException(e);
  }

}

// load color scale from json
public void fromJSON(String json) throws ColorScaleException
{
  try
  {
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> colorScaleMap = (Map<String, Object>)mapper.readValue(json, Map.class);
    String scalingStr = (String) colorScaleMap.get("Scaling");
    if (scalingStr != null)
    {
      scaling = Scaling.valueOf(scalingStr);
    }
    String reliefShadingStr = (String) colorScaleMap.get("ReliefShading");
    reliefShading = ("1").equals(reliefShadingStr) || ("true").equalsIgnoreCase(reliefShadingStr);

    String interpolateStr = (String) colorScaleMap.get("Interpolate");
    interpolate = (interpolateStr == null) || interpolateStr.isEmpty() || ("1").equals(interpolateStr) ||
        ("true").equalsIgnoreCase(interpolateStr);

    String forceStr = (String) colorScaleMap.get("ForceValuesIntoRange");
    forceValuesIntoRange = ("1").equals(forceStr) || ("true").equalsIgnoreCase(forceStr);

    Map<String, String> nullColorMap = (Map<String, String>) colorScaleMap.get("NullColor");
    String nullColorStr = nullColorMap.get("color");
    if (nullColorStr != null)
    {
      parseColor(nullColorStr, nullColorMap.get("opacity"), nullColor);
    }

    ArrayList<Map<String, String>> colorsList = (ArrayList<Map<String, String>>) colorScaleMap
        .get("Colors");
    if (colorsList != null)
    {
      for (Map<String, String> color : colorsList)
      {
        int[] colorArr = new int[4];
        String colorStr = color.get("color");
        String valueStr = color.get("value");
        Double value = Double.valueOf(valueStr);
        if (colorStr != null)
        {
          parseColor(colorStr, color.get("opacity"), colorArr);
          put(value, colorArr);
        }
      }
    }
  }
  catch (IOException | NullPointerException e)
  {
    throw new BadJSONException(e);
  }
}

final public int[] lookup(double v)
{
  if (scaling == Scaling.Quantile) {
    // Lookup the sub-scale for the value, then call it's lookup
    for (int q = 0; q < quantiles.length; q++) {
      if (v < quantiles[q]) {
        return quantileSubScales[q].lookup(v);
      }
    }
    return quantileSubScales[quantileSubScales.length-1].lookup(v);
  }
  if (Double.isNaN(v) || FloatUtils.isEqual(v, transparent))
  {
    return getNullColor();
  }
  if (cache == null)
  {
    buildCache();
  }

  if (v < min)
  {
    if (forceValuesIntoRange)
    {
      return cache[0];
    }
    return getNullColor();
  }
  else if (v > max)
  {
    if (forceValuesIntoRange)
    {
      return cache[CACHE_SIZE - 1];
    }
    return getNullColor();
  }
  else
  {
    int i = (int) ((v - min) / (max - min) * (CACHE_SIZE - 1) + 0.5);
    // int i = (int) ((v - min) / (max - min) * (CACHE_SIZE - 1));
    return cache[i];
  }
}

final public void lookup(double v, int[] color)
{
  int[] c = lookup(v);
  System.arraycopy(c, 0, color, 0, 4);
}

final public void lookup(double v, int[] colors, int offset)
{
  int[] c = lookup(v);
  System.arraycopy(c, 0, colors, offset, 4);
}

final public int[] lookupRGB(double v)
{
  int[] c = lookup(v);

  int[] mapped = new int[3];
  System.arraycopy(c, 0, c, 0, 3);

  return mapped;
}

final public void lookupRGB(double v, int[] color)
{
  int[] c = lookup(v);
  System.arraycopy(c, 0, color, 0, 3);
}

final public void lookupRGB(double v, int[] colors, int offset)
{
  int[] c = lookup(v);
  System.arraycopy(c, 0, colors, offset, 3);
}

public void put(double key, Color c)
{
  put(Double.valueOf(key), c);
  cache = null;
}

public void put(double key, int r, int g, int b)
{
  put(Double.valueOf(key), new Color(r, g, b));
  cache = null;
}

public void put(double key, int r, int g, int b, int a)
{
  put(Double.valueOf(key), new Color(r, g, b, a));
  cache = null;
}

public void put(double key, int[] c)
{
  put(Double.valueOf(key), new Color(c[0], c[1], c[2], c[3]));
  cache = null;
}

/**
 * Method is responsible for generating a color scale from hard coded values. It should be called
 * when there is no input default color scale.
 */
public void setDefaultGrayScaleValues()
{
  clear();
  setScaling(Scaling.MinMax);
  put(0.0, new Color(0, 0, 0));
  put(0.1, new Color(26, 26, 26));
  put(0.2, new Color(52, 52, 52));
  put(0.3, new Color(77, 77, 77));
  put(0.4, new Color(102, 102, 102));
  put(0.5, new Color(128, 128, 128));
  put(0.6, new Color(154, 154, 154));
  put(0.7, new Color(179, 179, 179));
  put(0.8, new Color(205, 205, 205));
  put(0.9, new Color(230, 230, 230));
  put(1.0, new Color(255, 255, 255));
  setInterpolate(true);
  setForceValuesIntoRange(false);
}

/**
 * Method is responsible for generating a color scale from hard coded values. It should be called
 * when there is no input default color scale.
 */
public void setDefaultValues()
{
  clear();
  setScaling(Scaling.MinMax);
  put(0.0, new Color(0, 0, 0, 0));
  put(1e-12, new Color(255, 255, 255, 128));
  put(0.1, new Color(247, 252, 253));
  put(0.2, new Color(229, 245, 249));
  put(0.3, new Color(204, 236, 230));
  put(0.4, new Color(153, 216, 201));
  put(0.5, new Color(102, 194, 164));
  put(0.6, new Color(65, 174, 118));
  put(0.7, new Color(35, 139, 69));
  put(0.8, new Color(0, 109, 44));
  put(0.9, new Color(0, 68, 27));
  put(1.0, new Color(0, 0, 0));
  setInterpolate(true);
  setForceValuesIntoRange(false);
}

/**
 * Sets the min and max value for scaling. This assumes that the entries in the color scale range
 * from 0 to 1. This also forces the scaling to be min/max. Min will be scaled to 0 and max will
 * be scaled to 1 in the color scale. All values between will be linearly interpolated. Values out
 * of bounds will be forced into range according to the forceValuesIntoRange property, which is
 * false by default.
 *
 * @param min The minimum value to be expected (smaller values will just be forced to min.
 * @param max The maximum value to be expected (larger values will just be forced to max.
 */
public void setScaleRange(double min, double max)
{
  this.min = min;
  this.max = max;
  if (scaling != Scaling.Modulo && scaling != Scaling.Quantile)
  {
    scaling = Scaling.MinMax;
  }
  cache = null;
  buildCache();
}

public void setScaleRangeWithQuantiles(double min, double max, double[] quantiles)
{
  this.min = min;
  this.max = max;
  cache = null;
  if (quantiles == null) {
    this.quantiles = null;
  }
  else {
    this.quantiles = Arrays.copyOf(quantiles, quantiles.length);
  }
  buildCache();
}

protected void buildCache()
{
  cache = new int[CACHE_SIZE][4];

  if (scaling == Scaling.Absolute) {
    min = firstKey();
    max = lastKey();
  }
  else if (scaling == Scaling.Quantile) {
    // If there are no quantiles for the image, then revert to an Absolute scale
    if (quantiles == null || quantiles.length < 1) {
      // Revert to interpolated Absolute scale.
      scaling = Scaling.Absolute;
      interpolate = true;
      // Update the color entries with actual values
      Iterator<Color> iter = values().iterator();
      Color[] colors = new Color[size()];
      int i = 0;
      while (iter.hasNext()) {
        colors[i] = iter.next();
        i++;
      }
      // Remove the existing entries fromthe color map and rewrite them with
      // new key values to accommodate the use of MinMax.
      super.clear();
      super.put(min, colors[0]);
      super.put(max, colors[colors.length-1]);
      for (int index = 1; index < colors.length-1; index++) {
        double value = (double)index / (double)(colors.length - 2);
        super.put(value, colors[index]);
        index++;
      }
    }
  }

  if (scaling == Scaling.Quantile) {
    // The color scale is initially configured with only the defined color
    // ramp "anchor" colors. Now compute a color for each quantile value
    // within that ramp. At this point, we know we have at least 2 color "anchors"
    // and 1 quantile value.
    Iterator<Color> iter = values().iterator();
    Color[] colors = new Color[size()];
    int i = 0;
    while (iter.hasNext()) {
      colors[i] = iter.next();
      i++;
    }
    // Clear only the TreeMap entries, not all of the setting for this ColorScale
//    super.clear();

    Color[] quantileColors = new Color[quantiles.length + 2];
    quantileColors[0] = colors[0];
    quantileColors[quantileColors.length-1] = colors[colors.length-1];
    boolean allQuantilesHaveColors = false;
    // When there are more than two colors in the color ramp, we need to assign
    // each of the colors (besides the first and last) to a quantile value. For
    // example if there are three total colors in the ramp, then the second color
    // should be assigned to the median quantile.
    if (colors.length > 2) {
      // First assign each defined color to a quantile
      // We will need to define a color for each data bin (the quantile values
      // are the "breaks" between bins).
      if (colors.length - 2 < quantiles.length + 1) {
        // There are fewer defined colors in the color ramp than there are data
        // bins. Map each color to the appropriate data bin.
        float colorFraction = 1.f / (float) (colors.length - 1);
        for (int c = 1; c < colors.length - 1; c++) {
          int qIndex = (int) ((float) (quantiles.length - 1) * colorFraction) * c;
          quantileColors[qIndex + 1] = colors[c];
        }
      }
      else {
        // There are at least as many colors as there are data bins. Map a subset
        // of the colors to the data bins.
        float colorsPerQuantileBin = (float)(colors.length - 2) / (float)(quantiles.length + 1);
        for (int q = 1; q <= quantiles.length; q++) {
          int index = Math.round(colorsPerQuantileBin * q);
          quantileColors[q] = colors[index];
        }
        allQuantilesHaveColors = true;
      }
    }

    if (!allQuantilesHaveColors) {
      // Now that all of the colors of the color ramp have been assigned to quantile
      // values, we need to interpolate colors to assign to the remaining quantile values.
      int lastAssignedQuantile = 0; // a color is always assigned to min
      for (int q = 1; q < quantileColors.length; q++) {
        if (quantileColors[q] != null) {
          // Found a quantile that already has a color
          Color color1 = quantileColors[lastAssignedQuantile];
          Color color2 = quantileColors[q];
          int numSlots = q - lastAssignedQuantile - 1;
          // If there is one open slot, that color should be halfway between
          // color1 and color2. If there are two slots, then each slot color should
          // be 1/3 of the way between color1 and color2, etc... (hence adding 1
          // in the slotFactor computation).
          float slotFactor = 1.0f / ((float)numSlots + 1);
          for (int slot = 1; slot <= numSlots; slot++) {
            quantileColors[lastAssignedQuantile + slot] = new Color(
                    interpolateValue(color1.getRed(), color2.getRed(), slotFactor * slot),
                    interpolateValue(color1.getGreen(), color2.getGreen(), slotFactor * slot),
                    interpolateValue(color1.getBlue(), color2.getBlue(), slotFactor * slot),
                    interpolateValue(color1.getAlpha(), color2.getAlpha(), slotFactor * slot));
          }
          lastAssignedQuantile = q;
        }
      }
    }

    // The underlying implementation of quantile coloring is to use an interpolated
    // Absolute color scale for each quantile, since we already have the code that
    // handles interpolating the color for individual values. We just store sub-color
    // scales for each quantile, and when the "lookup" method is called, we can quickly
    // determine which sub-color scale handles the value and then call its "lookup".
    quantileSubScales = new ColorScale[quantiles.length + 1];
    int subCacheSize = CACHE_SIZE / quantileSubScales.length;
    // The first color is for the min value through the first quantile value
    ColorScale qcs = new ColorScale();
    qcs.setScaling(Scaling.Absolute);
    qcs.CACHE_SIZE = subCacheSize;
    qcs.setInterpolate(true);
    qcs.put(min, quantileColors[0]);
    qcs.put(quantiles[0], quantileColors[1]);
    quantileSubScales[0] = qcs;

    for (int q = 0; q < quantiles.length - 1; q++) {
      qcs = new ColorScale();
      qcs.setScaling(Scaling.Absolute);
      qcs.CACHE_SIZE = subCacheSize;
      qcs.setInterpolate(true);
      qcs.put(quantiles[q], quantileColors[q+1]);
      qcs.put(quantiles[q+1], quantileColors[q+2]);
      quantileSubScales[q+1] = qcs;
    }
    // The last color is for the last quantile value through the max value
    qcs = new ColorScale();
    qcs.setScaling(Scaling.Absolute);
    qcs.CACHE_SIZE = subCacheSize;
    qcs.setInterpolate(true);
    qcs.put(quantiles[quantiles.length-1], quantileColors[quantileColors.length-2]);
    qcs.put(max, quantileColors[quantileColors.length-1]);
    quantileSubScales[quantileSubScales.length-1] = qcs;
  }
  else {
    for (int i = 0; i < CACHE_SIZE; i++) {
      double v = min + (max - min) * ((double) i / (double) (CACHE_SIZE - 1));
      if (interpolate) {
        interpolateColor(v, cache[i]);
      } else {
        absoluteColor(v, cache[i]);
      }
    }
  }
}

private int interpolateValue(int v1, int v2, float factor)
{
  if (v1 == v2) {
    return v1;
  }
  return (v1 + (int)(factor * (v2 - v1)));
}

/**
 * Find the color band that this value falls in and assign that color.
 *
 * @param v
 * @param color
 */
private void absoluteColor(double v, int[] color)
{
  double search;
  switch (scaling)
  {
  case Absolute:
  case Quantile:
    search = v;
    break;
  case MinMax:
    search = (v - min) / (max - min);
    break;
  case Modulo:
    search = v % (max - min);
    break;
  default:
    search = 0;
    break;
  }

  Map.Entry<Double, Color> lower = floorEntry(search);

  Color c;
  if (lower == null)
  {
    c = entrySet().iterator().next().getValue();
  }
  else
  {
    c = lower.getValue();
  }

  color[R] = c.getRed();
  color[G] = c.getGreen();
  color[B] = c.getBlue();
  color[A] = c.getAlpha();
}

/**
 * Interpolate the color value for the given scalar value. The result is placed in color.
 *
 * @param v
 * @param color
 * @return
 */
private void interpolateColor(double v, int[] color)
{
  double search;
  switch (scaling)
  {
  case Absolute:
  case Quantile:
    search = v;
    break;
  case MinMax:
    search = (v - min) / (max - min);
    break;
  case Modulo:
    search = (v - min) % (max - min);
    break;
  default:
    search = 0;
    break;
  }

  Map.Entry<Double, Color> lower = floorEntry(search);
  Map.Entry<Double, Color> upper = higherEntry(search);

  assert (upper != null || lower != null);

  if (upper == null)
  {
    Color c = lower.getValue();
    color[R] = c.getRed();
    color[G] = c.getGreen();
    color[B] = c.getBlue();
    color[A] = c.getAlpha();
  }
  else if (lower == null)
  {
    Color c = upper.getValue();
    color[R] = c.getRed();
    color[G] = c.getGreen();
    color[B] = c.getBlue();
    color[A] = c.getAlpha();
  }
  else
  {
    double diff = upper.getKey() - lower.getKey();
    double lw = 1.0 - ((search - lower.getKey()) / diff);
    double uw = 1.0 - lw;

    Color lc = lower.getValue();
    Color uc = upper.getValue();
    color[R] = (int) Math.round(lc.getRed() * lw + uc.getRed() * uw);
    color[G] = (int) Math.round(lc.getGreen() * lw + uc.getGreen() * uw);
    color[B] = (int) Math.round(lc.getBlue() * lw + uc.getBlue() * uw);
    color[A] = (int) Math.round(lc.getAlpha() * lw + uc.getAlpha() * uw);
  }
}

public enum Scaling
{
  Absolute, MinMax, Modulo, Quantile
}

public static class BadSourceException extends ColorScaleException
{
  private static final long serialVersionUID = 1L;

  public BadSourceException(Exception e)
  {
    super(e);
  }

}

public static class BadJSONException extends ColorScaleException
{
  private static final long serialVersionUID = 1L;

  public BadJSONException(Exception e)
  {
    super(e);
  }

}

public static class BadXMLException extends ColorScaleException
{
  private static final long serialVersionUID = 1L;

  public BadXMLException(Exception e)
  {
    super(e);
  }

}

public static class ColorScaleException extends Exception
{
  private static final long serialVersionUID = 1L;
  private final Exception origException;

  public ColorScaleException(Exception e)
  {
    origException = e;
    // printStackTrace();
  }

  public ColorScaleException(String msg)
  {
    origException = new Exception(msg);
  }

  @Override
  public void printStackTrace()
  {
    origException.printStackTrace();
  }

}
}
