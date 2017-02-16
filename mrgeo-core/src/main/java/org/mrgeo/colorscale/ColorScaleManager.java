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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.FilenameUtils;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

public class ColorScaleManager
{
private static final Logger log = LoggerFactory.getLogger(ColorScale.class);

private static Map<String, ColorScale> colorscales;

static
{
  try
  {
    initializeColorscales();
  }
  catch (ColorScale.ColorScaleException e)
  {
    throw new RuntimeException("Error initializing ColorScaleManager", e);
  }
}

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
  if (colorscales.containsKey(colorScaleName)) {
    return (ColorScale) colorscales.get(colorScaleName).clone();
  }
  return null;
}

public static ColorScale[] getColorScaleList() throws IOException
{
  // for convenience and testing, we'll sort the list...
  ColorScale[] list = new ColorScale[colorscales.size()];
  int cnt = 0;
  for (Map.Entry<String, ColorScale> entry: colorscales.entrySet())
  {
    list[cnt++] = entry.getValue();
  }

  return list;
  //return colorscales.values().toArray(new ColorScale[0]);
}

@SuppressFBWarnings(value = "WEAK_FILENAMEUTILS", justification = "Using Java 1.7+, weak filenames are fixed")
private static synchronized void initializeColorscales() throws ColorScale.ColorScaleException
{
  if (colorscales == null)
  {
    colorscales = new TreeMap<>();

    Properties props = MrGeoProperties.getInstance();

    final String colorScaleBase = HadoopUtils.getDefaultColorScalesBaseDirectory(props);
    if (colorScaleBase != null)
    {
      try
      {
        AdHocDataProvider provider = DataProviderFactory.getAdHocDataProvider(colorScaleBase,
            AccessMode.READ, HadoopUtils.createConfiguration());

        for (int i = 0; i < provider.size(); i++)
        {
          String name = provider.getName(i);
          if (name != null)
          {
          if (FilenameUtils.getExtension(name).toLowerCase().equals("xml"))
          {
            try (InputStream fdis = provider.get(i))
            {
              ColorScale cs = ColorScale.loadFromXML(fdis);

              colorscales.put(cs.getName(), cs);
            }
          }
          }
        }
      }
      catch (IOException e)
      {
        throw new ColorScale.BadSourceException(e);
      }
    }
    else
    {
      throw new ColorScale.ColorScaleException("No color scale base directory configured");
    }

    if (log.isInfoEnabled())
    {
      log.info("ColorScales:");
      for (String name : colorscales.keySet())
      {
        log.info("  - " + FilenameUtils.getBaseName(name));
      }
    }
  }
}

protected static void resetColorscales() throws ColorScale.ColorScaleException
{
  colorscales = null;
  initializeColorscales();
}

}
