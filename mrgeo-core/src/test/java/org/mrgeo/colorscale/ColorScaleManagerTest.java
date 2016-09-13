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

import junit.framework.Assert;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.TestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author Steve Ingram
 *         Date: 10/27/13
 */
@SuppressWarnings("static-method")
public class ColorScaleManagerTest {

    @Before
    public void init()
    {   
      ColorScaleManager.invalidateCache();
    }
    
    @Test(expected = Exception.class)
    @Category(UnitTest.class)
    public void testGetColorScale_ColorScaleBaseDirNotExist() throws Exception
    {
      final Properties mrgeoConf = new Properties();
      ColorScaleManager.colorscaleProvider = null;
      @SuppressWarnings("unused")
      final ColorScale cs = ColorScaleManager.fromName("ColorScaleTest", mrgeoConf);
    }

    @Test
    @Category(UnitTest.class)
    public void testGetColorScale_ColorScaleFromColorScaleBaseDir() throws Exception
    {
      final String colorScaleJSON = getTestColorScale();
      final ColorScale csExp = ColorScale.loadFromJSON(colorScaleJSON);
      
      final Properties mrgeoConf = new Properties();
      mrgeoConf.put(MrGeoConstants.MRGEO_COMMON_HOME, TestUtils.composeInputDir(ColorScaleManagerTest.class));
      mrgeoConf.put(MrGeoConstants.MRGEO_HDFS_IMAGE, TestUtils.composeInputDir(ColorScaleManagerTest.class));
      mrgeoConf.put(MrGeoConstants.MRGEO_HDFS_COLORSCALE, "file://" + TestUtils.composeInputDir(ColorScaleManagerTest.class));

      final ColorScale cs = ColorScaleManager.fromName("ColorScaleTest", mrgeoConf);
      Assert.assertEquals(true, cs.equals(csExp));
    }


    @Test
    @Category(UnitTest.class)
    public void testGetColorScale_WithColorScaleNameNoXML() throws Exception
    {
      final String colorScaleJSON = getTestColorScale();
      final Properties mrgeoConf = new Properties();
      mrgeoConf.put(MrGeoConstants.MRGEO_HDFS_COLORSCALE, TestUtils.composeInputDir(ColorScaleManagerTest.class));
      final ColorScale cs = ColorScaleManager.fromName("ColorScaleTest", mrgeoConf);
      final ColorScale csExpected = ColorScale.loadFromJSON(colorScaleJSON);
      Assert.assertEquals(true, cs.equals(csExpected));
    }

    @Test(expected = Exception.class)
    @Category(UnitTest.class)
    public void testGetColorScale_WithColorScaleNameNotExist() throws Exception
    {
      final Properties mrgeoConf = new Properties();
      mrgeoConf.put(MrGeoConstants.MRGEO_HDFS_COLORSCALE,  TestUtils.composeInputDir(ColorScaleManagerTest.class));
      ColorScaleManager.fromName("ColorScaleTest123", mrgeoConf);
    }

    @Test
    @Category(UnitTest.class)
    public void testGetColorScale_WithJSON() throws Exception
    {
      final String colorScaleJSON = getAspectColorScale();
      final ColorScale cs = ColorScaleManager.fromJSON(colorScaleJSON);
      final ColorScale csExpected = ColorScale.loadFromJSON(colorScaleJSON);
      Assert.assertEquals(true, cs.equals(csExpected));
    }

    private String getAspectColorScale() throws JsonGenerationException, JsonMappingException,
        IOException
    {
      // create colorScale json
      final ObjectMapper mapper = new ObjectMapper();

      final Map<String, Object> colorScale = new HashMap<String, Object>();
      colorScale.put("Scaling", "MinMax");
      colorScale.put("ForceValuesIntoRange", "1");

      final Map<String, String> nullColor = new HashMap<String, String>();
      nullColor.put("color", "0,0,0");
      nullColor.put("opacity", "0");
      colorScale.put("NullColor", nullColor);
      final Map<String, String> color1 = new HashMap<String, String>();
      color1.put("value", "0.0");
      color1.put("color", "0,0,255");
      color1.put("opacity", "128");
      final Map<String, String> color2 = new HashMap<String, String>();
      color2.put("value", "0.26");
      color2.put("color", "255,255,0");
      color2.put("opacity", "128");
      final Map<String, String> color3 = new HashMap<String, String>();
      color3.put("value", "0.51");
      color3.put("color", "34,139,34");
      color3.put("opacity", "128");
      final Map<String, String> color4 = new HashMap<String, String>();
      color4.put("value", "0.76");
      color4.put("color", "255,0,0");
      color4.put("opacity", "128");
      final Map<String, String> color5 = new HashMap<String, String>();
      color5.put("value", "1.0");
      color5.put("color", "0,0,255");
      color5.put("opacity", "128");

      final ArrayList<Map<String, String>> colors = new ArrayList<Map<String, String>>();
      colors.add(color1);
      colors.add(color2);
      colors.add(color3);
      colors.add(color4);
      colors.add(color5);

      colorScale.put("Colors", colors);

      return mapper.writeValueAsString(colorScale);
    }

//    private String getDefaultColorScale() throws JsonGenerationException, JsonMappingException,
//        IOException
//    {
//      // create colorScale json
//      final ObjectMapper mapper = new ObjectMapper();
//
//      final Map<String, Object> colorScale = new HashMap<String, Object>();
//      colorScale.put("Scaling", "MinMax");
//      colorScale.put("ForceValuesIntoRange", "1");
//
//      final Map<String, String> nullColor = new HashMap<String, String>();
//      nullColor.put("color", "0,0,0");
//      nullColor.put("opacity", "0");
//      colorScale.put("NullColor", nullColor);
//      final Map<String, String> color1 = new HashMap<String, String>();
//      color1.put("value", "0.0");
//      color1.put("color", "255,0,0");
//      final Map<String, String> color2 = new HashMap<String, String>();
//      color2.put("value", "0.25");
//      color2.put("color", "255,255,0");
//      final Map<String, String> color3 = new HashMap<String, String>();
//      color3.put("value", "0.75");
//      color3.put("color", "0,255,255");
//      final Map<String, String> color4 = new HashMap<String, String>();
//      color4.put("value", "1.0");
//      color4.put("color", "255,255,255");
//
//      final ArrayList<Map<String, String>> colors = new ArrayList<Map<String, String>>();
//      colors.add(color1);
//      colors.add(color2);
//      colors.add(color3);
//      colors.add(color4);
//
//      colorScale.put("Colors", colors);
//
//      return mapper.writeValueAsString(colorScale);
//    }

//    private String getRainbowColorScale() throws JsonGenerationException, JsonMappingException,
//        IOException
//    {
//      // create colorScale json
//      final ObjectMapper mapper = new ObjectMapper();
//
//      final Map<String, Object> colorScale = new HashMap<String, Object>();
//      colorScale.put("Scaling", "MinMax");
//      colorScale.put("ReliefShading", "0");
//      colorScale.put("Interpolate", "1");
//      colorScale.put("ForceValuesIntoRange", "1");
//
//      final Map<String, String> nullColor = new HashMap<String, String>();
//      nullColor.put("color", "0,0,0");
//      nullColor.put("opacity", "0");
//      colorScale.put("NullColor", nullColor);
//      final Map<String, String> color1 = new HashMap<String, String>();
//      color1.put("value", "0.0");
//      color1.put("color", "0,0,127");
//      color1.put("opacity", "255");
//      final Map<String, String> color2 = new HashMap<String, String>();
//      color2.put("value", "0.2");
//      color2.put("color", "0,0,255");
//      final Map<String, String> color3 = new HashMap<String, String>();
//      color3.put("value", "0.4");
//      color3.put("color", "0,255,255");
//      final Map<String, String> color4 = new HashMap<String, String>();
//      color4.put("value", "0.6");
//      color4.put("color", "0,255,0");
//      final Map<String, String> color5 = new HashMap<String, String>();
//      color5.put("value", "0.8");
//      color5.put("color", "255,255,0");
//      final Map<String, String> color6 = new HashMap<String, String>();
//      color6.put("value", "1.0");
//      color6.put("color", "255,0,0");
//
//      final ArrayList<Map<String, String>> colors = new ArrayList<Map<String, String>>();
//      colors.add(color1);
//      colors.add(color2);
//      colors.add(color3);
//      colors.add(color4);
//      colors.add(color5);
//      colors.add(color6);
//
//      colorScale.put("Colors", colors);
//
//      return mapper.writeValueAsString(colorScale);
//    }

    private String getTestColorScale() throws JsonGenerationException, JsonMappingException,
            IOException
    {
      // create colorScale json
      final ObjectMapper mapper = new ObjectMapper();

      final Map<String, Object> colorScale = new HashMap<String, Object>();
      colorScale.put("Scaling", "Absolute");
      colorScale.put("ReliefShading", "1");
      colorScale.put("Interpolate", "1");
      colorScale.put("ForceValuesIntoRange", "1");

      final Map<String, String> nullColor = new HashMap<String, String>();
      nullColor.put("color", "0,0,0");
      nullColor.put("opacity", "0");
      colorScale.put("NullColor", nullColor);
      final Map<String, String> color1 = new HashMap<String, String>();
      color1.put("value", "0.0");
      color1.put("color", "0,0,127");
      color1.put("opacity", "170");
      final Map<String, String> color2 = new HashMap<String, String>();
      color2.put("value", "0.5");
      color2.put("color", "0,0,255");
      color2.put("opacity", "170");
      final Map<String, String> color3 = new HashMap<String, String>();
      color3.put("value", "1.0");
      color3.put("color", "255,0,0");
      color3.put("opacity", "170");

      final ArrayList<Map<String, String>> colors = new ArrayList<Map<String, String>>();
      colors.add(color1);
      colors.add(color2);
      colors.add(color3);

      colorScale.put("Colors", colors);

      return mapper.writeValueAsString(colorScale);
    }
}
