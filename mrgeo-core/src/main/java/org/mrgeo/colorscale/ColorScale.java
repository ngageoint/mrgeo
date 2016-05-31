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

package org.mrgeo.colorscale;

import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.mrgeo.utils.FloatUtils;
import org.mrgeo.utils.XmlUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import scala.tools.cmd.gen.AnyVals;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import java.awt.*;
import java.io.*;
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

  private static final long serialVersionUID = 1L;
  private static final int CACHE_SIZE = 1024;
  private static final int A = 3;
  private static final int B = 2;
  private static final int G = 1;
  private static final int R = 0;
  private static ColorScale _colorScale = null;
  private static ColorScale _grayScale = null;
  private int[][] cache = null;
  private boolean interpolate;
  private Double min, max;
  private int[] nullColor = {0, 0, 0, 0};
  private boolean reliefShading;
  private Scaling scaling = Scaling.Absolute;
  private boolean forceValuesIntoRange;
  private double transparent = Double.NaN;
  private String name = null;
  private String title = null;
  private String description = null;

  private static final Object lock = new Object();

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

  public static ColorScale loadFromXML(String filename) throws ColorScaleException
  {
    try
    {
      InputStream stream = null;
      try
      {
        stream = new FileInputStream(filename);
        ColorScale cs = new ColorScale();
        cs.fromXML(stream);

        return cs;
      }
      finally
      {
        if (stream != null)
        {
          IOUtils.closeQuietly(stream);
        }
      }
    }
    catch (IOException e)
    {
      e.printStackTrace();
      throw new ColorScaleException(e);
    }
  }

  public static ColorScale loadFromJSON(InputStream stream) throws ColorScaleException
  {
    ColorScale cs = new ColorScale();
    cs.fromJSON(stream);
    return cs;
  }

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
  public synchronized static void setDefault(final ColorScale colorScale)
  {
    _colorScale = colorScale;

    if (_colorScale == null)
    {
      _colorScale = new ColorScale();
      _colorScale.setDefaultValues();
    }
  }

  private static void parseColor(final String colorStr, final String opacityStr, final int[] color)
      throws IOException
  {
    final String[] colors = colorStr.split(",");
    if (colors.length != 3)
    {
      throw new IOException("Error parsing XML: There must be three elements in color.");
    }
    color[0] = Integer.parseInt(colors[0]);
    color[1] = Integer.parseInt(colors[1]);
    color[2] = Integer.parseInt(colors[2]);
    if (opacityStr != null && !opacityStr.isEmpty())
    {
      color[3] = Integer.parseInt(opacityStr);
    }
    else
    {
      color[3] = 255;
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

    final ColorScale result = new ColorScale();
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
  return obj instanceof ColorScale && equals((ColorScale)obj);
}

public boolean equals(final ColorScale cs)
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
    final Iterator<Double> iterator2 = cs.keySet().iterator();
    for (Map.Entry<Double, Color> iterator1 : entrySet())
    {
      final Double d2 = iterator2.next();
      if (iterator1.getKey().compareTo(d2) != 0)
      {
        return false;
      }

      if (!iterator1.getValue().equals(get(d2)))
      {
        return false;
      }
    }

    return true;
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
  public void setForceValuesIntoRange(final boolean i)
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
  public void setInterpolate(final boolean i)
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

  public void setNullColor(final int[] color)
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

  public void setScaling(final Scaling s)
  {
    scaling = s;
    cache = null;
  }

  public double getTransparent()
  {
    return transparent;
  }

  public void setTransparent(final double transparent)
  {
    this.transparent = transparent;
  }

  public void fromXML(final Document doc) throws ColorScaleException
  {
    try
    {
      clear();

      final XPath xpath = XmlUtils.createXPath();

      name = xpath.evaluate("/ColorMap/@name", doc);

      final Node nodeTitle = (Node)xpath.evaluate("/ColorMap/Title", doc, XPathConstants.NODE);
      if (nodeTitle != null)
      {
        title = xpath.evaluate("text()", nodeTitle);
      }

      final Node nodeDesc = (Node)xpath.evaluate("/ColorMap/Description", doc, XPathConstants.NODE);
      if (nodeDesc != null)
      {
        description = xpath.evaluate("text()", nodeDesc);
      }

      scaling = Scaling.valueOf(xpath.evaluate("/ColorMap/Scaling/text()", doc));
      final String reliefShadingStr = xpath.evaluate("/ColorMap/ReliefShading/text()", doc)
          .toLowerCase();
      reliefShading = reliefShadingStr.equals("1") || reliefShadingStr.equals("true");

      final String interpolateStr = xpath.evaluate("/ColorMap/Interpolate/text()", doc)
          .toLowerCase();
      interpolate = interpolateStr.isEmpty() || (interpolateStr.equals("1") || interpolateStr.equals("true"));

      final String forceStr = xpath.evaluate("/ColorMap/ForceValuesIntoRange/text()", doc)
          .toLowerCase();
      forceValuesIntoRange = (forceStr.equals("1") || forceStr.equals("true"));

      final Node nullColorNode = (Node) xpath.evaluate("/ColorMap/NullColor", doc,
          XPathConstants.NODE);
      if (nullColorNode != null)
      {
        final String colorStr = xpath.evaluate("@color", nullColorNode);
        final String opacityStr = xpath.evaluate("@opacity", nullColorNode);
        parseColor(colorStr, opacityStr, nullColor);
      }

      final int[] color = new int[4];
      final XPathExpression expr = xpath.compile("/ColorMap/Color");
      final NodeList nodes = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
      for (int i = 0; i < nodes.getLength(); i++)
      {
        final Node node = nodes.item(i);

        final String valueStr = xpath.evaluate("@value", node);
        final String colorStr = xpath.evaluate("@color", node);
        final String opacityStr = xpath.evaluate("@opacity", node);

        if (valueStr.isEmpty())
        {
          throw new IOException("Error parsing XML: A value must be specified for a color element.");
        }

        final double value = Double.valueOf(valueStr);
        parseColor(colorStr, opacityStr, color);
        put(value, color);
      }
      cache = null;
    }
    catch (XPathExpressionException | IOException e)
    {
      throw new BadXMLException(e);
    }
  }

  public void fromXML(final InputStream strm) throws ColorScaleException
  {
    try
    {
      fromXML(XmlUtils.parseInputStream(strm));
    }
    catch (final Exception e)
    {
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

  public void fromJSON(final InputStream stream) throws ColorScaleException
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
      e.printStackTrace();
      throw new ColorScaleException(e);
    }

  }

  // load color scale from json
  public void fromJSON(final String json) throws ColorScaleException
  {
    try
    {
      final ObjectMapper mapper = new ObjectMapper();
      final Map<String, Object> colorScaleMap = mapper.readValue(json, Map.class);
      final String scalingStr = (String) colorScaleMap.get("Scaling");
      if (scalingStr != null)
      {
        scaling = Scaling.valueOf(scalingStr);
      }
      final String reliefShadingStr = (String) colorScaleMap.get("ReliefShading");
      reliefShading = ("1").equals(reliefShadingStr) || ("true").equalsIgnoreCase(reliefShadingStr);

      final String interpolateStr = (String) colorScaleMap.get("Interpolate");
      interpolate = (interpolateStr == null) || interpolateStr.isEmpty() || ("1").equals(interpolateStr) ||
          ("true").equalsIgnoreCase(interpolateStr);

      final String forceStr = (String) colorScaleMap.get("ForceValuesIntoRange");
      forceValuesIntoRange = ("1").equals(forceStr) || ("true").equalsIgnoreCase(forceStr);

      final Map<String, String> nullColorMap = (Map<String, String>) colorScaleMap.get("NullColor");
      final String nullColorStr = nullColorMap.get("color");
      if (nullColorStr != null)
      {
        parseColor(nullColorStr, nullColorMap.get("opacity"), nullColor);
      }

      final ArrayList<Map<String, String>> colorsList = (ArrayList<Map<String, String>>) colorScaleMap
          .get("Colors");
      if (colorsList != null)
      {
        for (final Map<String, String> color : colorsList)
        {
          final int[] colorArr = new int[4];
          final String colorStr = color.get("color");
          final String valueStr = color.get("value");
          final Double value = Double.valueOf(valueStr);
          if (colorStr != null)
          {
            parseColor(colorStr, color.get("opacity"), colorArr);
            put(value, colorArr);
          }
        }
      }
    }
    catch (IOException e)
    {
      throw new BadJSONException(e);
    }
  }

  final public int[] lookup(final double v)
  {
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
      final int i = (int) ((v - min) / (max - min) * (CACHE_SIZE - 1) + 0.5);
      // int i = (int) ((v - min) / (max - min) * (CACHE_SIZE - 1));
      return cache[i];
    }
  }

  final public void lookup(final double v, final int[] color)
  {
    final int[] c = lookup(v);
    System.arraycopy(c, 0, color, 0, 4);
  }

  public void put(final double key, final Color c)
  {
    put(Double.valueOf(key), c);
    cache = null;
  }

  public void put(final double key, final int r, final int g, final int b)
  {
    put(Double.valueOf(key), new Color(r, g, b));
    cache = null;
  }

  public void put(final double key, final int r, final int g, final int b, final int a)
  {
    put(Double.valueOf(key), new Color(r, g, b, a));
    cache = null;
  }

  public void put(final double key, final int[] c)
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
    setScaling(ColorScale.Scaling.MinMax);
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
    setScaling(ColorScale.Scaling.MinMax);
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
  public void setScaleRange(final double min, final double max)
  {
    this.min = min;
    this.max = max;
    if (scaling != Scaling.Modulo)
    {
      scaling = Scaling.MinMax;
    }
    cache = null;
    buildCache();
  }

  protected void buildCache()
  {
    cache = new int[CACHE_SIZE][4];

    if (scaling == Scaling.Absolute)
    {
      min = firstKey();
      max = lastKey();
    }

    for (int i = 0; i < CACHE_SIZE; i++)
    {
      final double v = min + (max - min) * ((double) i / (double) (CACHE_SIZE - 1));
      if (interpolate)
      {
        interpolateValue(v, cache[i]);
      }
      else
      {
        absoluteValue(v, cache[i]);
      }
    }
  }

  /**
   * Find the color band that this value falls in and assign that color.
   *
   * @param v
   * @param color
   */
  final private void absoluteValue(final double v, final int[] color)
  {
    final double search;
    switch (scaling)
    {
    case Absolute:
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

    final Map.Entry<Double, Color> lower = floorEntry(search);

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
  final private void interpolateValue(final double v, final int[] color)
  {
    final double search;
    switch (scaling)
    {
    case Absolute:
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

    final Map.Entry<Double, Color> lower = floorEntry(search);
    final Map.Entry<Double, Color> upper = higherEntry(search);

    assert (upper != null || lower != null);

    if (upper == null)
    {
      final Color c = lower.getValue();
      color[R] = c.getRed();
      color[G] = c.getGreen();
      color[B] = c.getBlue();
      color[A] = c.getAlpha();
    }
    else if (lower == null)
    {
      final Color c = upper.getValue();
      color[R] = c.getRed();
      color[G] = c.getGreen();
      color[B] = c.getBlue();
      color[A] = c.getAlpha();
    }
    else
    {
      final double diff = upper.getKey().doubleValue() - lower.getKey().doubleValue();
      final double lw = 1.0 - ((search - lower.getKey().doubleValue()) / diff);
      final double uw = 1.0 - lw;

      final Color lc = lower.getValue();
      final Color uc = upper.getValue();
      color[R] = (int) Math.round(lc.getRed() * lw + uc.getRed() * uw);
      color[G] = (int) Math.round(lc.getGreen() * lw + uc.getGreen() * uw);
      color[B] = (int) Math.round(lc.getBlue() * lw + uc.getBlue() * uw);
      color[A] = (int) Math.round(lc.getAlpha() * lw + uc.getAlpha() * uw);
    }
  }

  public enum Scaling
  {
    Absolute, MinMax, Modulo
  }

  public static class BadSourceException extends ColorScaleException
  {
    private static final long serialVersionUID = 1L;

    public BadSourceException(final Exception e)
    {
      super(e);
    }

  }

  public static class BadJSONException extends ColorScaleException
  {
    private static final long serialVersionUID = 1L;

    public BadJSONException(final Exception e)
    {
      super(e);
    }

  }

  public static class BadXMLException extends ColorScaleException
  {
    private static final long serialVersionUID = 1L;

    public BadXMLException(final Exception e)
    {
      super(e);
    }

  }

  public static class ColorScaleException extends Exception
  {
    private static final long serialVersionUID = 1L;
    private final Exception origException;

    public ColorScaleException(final Exception e)
    {
      this.origException = e;
      printStackTrace();
    }

    public ColorScaleException(final String msg)
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
}
