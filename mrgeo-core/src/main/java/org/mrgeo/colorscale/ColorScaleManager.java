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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.DataProviderNotFound;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ColorScaleManager
{
  private static final Logger log = LoggerFactory.getLogger(ColorScaleManager.class);

//  static FileSystemDriver driver;
  static AdHocDataProvider colorscaleProvider;

  private static LoadingCache<Map<String, Object>, ColorScale> colorscaleCache = CacheBuilder
    .newBuilder().maximumSize(1000).expireAfterAccess(10, TimeUnit.MINUTES).build(
      new CacheLoader<Map<String, Object>, ColorScale>()
      {
        @Override
        public ColorScale load(final Map<String, Object> colorScaleInfo) throws Exception
        {
          // The following code was used to load a color scale from a specific HDFS
          // path. It is no longer supported. Color scales must reside in the
          // configured color scale location.
//          final org.apache.hadoop.fs.Path path = (org.apache.hadoop.fs.Path) colorScaleInfo
//            .get("path");
//          final Double min = (Double) colorScaleInfo.get("min");
//          final Double max = (Double) colorScaleInfo.get("max");
//          final Double transparent = (Double) colorScaleInfo.get("transparent");
//          final FileSystem fs = HadoopFileUtils.getFileSystem();
//          final FSDataInputStream fdis = fs.open(path);
//          final ColorScale cs = ColorScale.loadFromXML(fdis);
//          fdis.close();
//
//          cs.setTransparent(transparent);
//          cs.setScaleRange(min, max);
//          return cs;
          return null;
        }
      });

  private static Properties mrgeoProperties = MrGeoProperties.getInstance();

  // returns a ColorScale object based on the following rules
  // 1. If a colorScaleJSON is provided, load the color scale from the JSON
  // 2. If a colorScaleName is provided, look for xml file with that name in the
  // default color scales
  // dir, if not found throw an exception
  // 3. if none of the above is provided, use the following to determine the
  // colorscale
  // ColorScale.xml file in pyramid dir
  // Default.xml in image base dir
  // color scale hardcoded into ColorScale class
  // public static ColorScale getColorScale(String colorScaleName, String colorScaleJSON,
  // String pyramidPath) throws Exception
  // {
  // return getColorScale(
  // colorScaleName, colorScaleJSON, pyramidPath, MrGeoProperties.getInstance());
  // }

  public static ColorScale fromJSON(final String colorScaleJSON) throws ColorScale.ColorScaleException
  {
    ColorScale cs = null;
    if (colorScaleJSON != null)
    {
      cs = ColorScale.loadFromJSON(colorScaleJSON);
    }

    return cs;
  }

  public static ColorScale fromName(final String colorScaleName) throws ColorScale.ColorScaleException
  {
    return fromName(colorScaleName, mrgeoProperties);
  }

  public static ColorScale fromName(final String colorScaleName,
      final Properties props) throws ColorScale.ColorScaleException
  {
    final Map<String, Object> colorScaleInfo = new HashMap<String, Object>();
    colorScaleInfo.put("colorscalename", colorScaleName);

    ColorScale cs = colorscaleCache.getIfPresent(colorScaleInfo);
    if (cs == null)
    {
      if (colorScaleName != null)
      {
        cs = createColorScale(colorScaleInfo, colorScaleName, props);
      }
    }
    return cs;
  }

//  public static ColorScale getColorScale(final Map<String, Object> colorScaleInfo) throws Exception
//  {
//    return colorscaleCache.get(colorScaleInfo);
//  }

  public static void invalidateCache()
  {
    colorscaleCache.invalidateAll();
  }

  private static ColorScale createColorScale(final Map<String, Object> colorScaleInfo,
      final String colorScaleName, final Properties props) throws ColorScale.ColorScaleException
  {
    InputStream fdis = null;
    try
    {
      initializeProvider(props);
      fdis = colorscaleProvider.get(colorScaleName);
      ColorScale cs = ColorScale.loadFromXML(fdis);
      colorscaleCache.put(colorScaleInfo, cs);
      return cs;
    }
    catch (IOException e)
    {
      throw new ColorScale.BadSourceException(e);
    }
    finally
    {
      if (fdis != null)
      {
        IOUtils.closeQuietly(fdis);
      }
    }
  }

  private static void initializeProvider(final Properties props) throws ColorScale.ColorScaleException
  {
    if (colorscaleProvider == null)
    {
      final String colorScaleBase = HadoopUtils.getDefaultColorScalesBaseDirectory(props);
      if (colorScaleBase != null)
      {
        try
        {
          colorscaleProvider = DataProviderFactory.getAdHocDataProvider(colorScaleBase,
                                                                        AccessMode.READ,
                                                                        HadoopUtils.createConfiguration());
        }
        catch (DataProviderNotFound e)
        {
          throw new ColorScale.BadSourceException(e);
        }
      }
      else
      {
        throw new ColorScale.ColorScaleException("No ColorScaleBase directory configured");
      }
    }
  }

  @SuppressWarnings("squid:S1166") // Exception caught and handled
  @SuppressFBWarnings(value = "WEAK_FILENAMEUTILS", justification = "Using adhoc provider, our class, for the filename")
  public static ColorScale[] getColorScaleList() throws IOException
  {
    try
    {
      initializeProvider(mrgeoProperties);

      ArrayList<ColorScale> scales = new ArrayList<>();
      for (int i = 0; i < colorscaleProvider.size(); i++)
      {
        InputStream fdis = null;
        try
        {
          fdis = colorscaleProvider.get(i);

          ColorScale scale = ColorScale.loadFromXML(fdis);

          // strip the name from the filename
          String name = colorscaleProvider.getName(i);
          name = FilenameUtils.getBaseName(name);
          scale.setName(name);

          scales.add(scale);
        }
        catch (IOException ignored)
        {
          // no-op could be something other than a color scale in this directory
        }
        finally
        {
          if (fdis != null)
          {
            IOUtils.closeQuietly(fdis);
          }
        }

      }
      return scales.toArray(new ColorScale[0]);
    }
    catch (ColorScale.ColorScaleException e)
    {
      log.error("Unable to get a list of color scales", e);
    }
    return new ColorScale[0];
  }
}
