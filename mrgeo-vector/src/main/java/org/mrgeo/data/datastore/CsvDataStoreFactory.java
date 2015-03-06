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

package org.mrgeo.data.datastore;

import org.geotools.data.AbstractDataStoreFactory;
import org.geotools.data.DataStore;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFactorySpi;
import org.mrgeo.data.csv.CsvGeometryInputStream;
import org.mrgeo.data.tsv.TsvGeometryInputStream;
import org.mrgeo.hdfs.vector.Column;
import org.mrgeo.hdfs.vector.ColumnDefinitionFile;
import org.mrgeo.utils.HadoopURLStreamHandlerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class CsvDataStoreFactory extends AbstractDataStoreFactory implements
FileDataStoreFactorySpi
{
  final private Param[] parameters;

  public CsvDataStoreFactory()
  {
    final List<Param> p = new LinkedList<Param>();
    Param param;
    param = new Param("url", URL.class, "url to the csv file", true);
    p.add(param);
    param = new Param("delimiter", String.class, "url to the csv file", false);
    p.add(param);

    parameters = p.toArray(new Param[0]);
  }


  private static boolean hasColumns(final URL url)
  {
    try
    {
      // if the .columns file has a "geometry" field, or lat/latitude and lon/longitude, or x & y,
      // we'll assume that is geometry...
      final URI colsUri = new URI(url.toString() + ".columns");
      final URL cols = HadoopURLStreamHandlerFactory.createHadoopURL(colsUri);

      final InputStream is = cols.openStream();
      try
      {
        final ColumnDefinitionFile cdf = new ColumnDefinitionFile(is);

        for (final Column column : cdf.getColumns())
        {
          final String col = column.getName();
          if (col.equalsIgnoreCase("geometry"))
          {
            return true;
          }
          else if (col.equalsIgnoreCase("x"))
          {
            for (final Column column2 : cdf.getColumns())
            {
              final String col2 = column2.getName();
              if (col2.equalsIgnoreCase("y"))
              {
                return true;
              }
            }
          }
          else if (col.equalsIgnoreCase("lat") || col.equalsIgnoreCase("latitude"))
          {
            for (final Column column2 : cdf.getColumns())
            {
              final String col2 = column2.getName();
              if (col2.equalsIgnoreCase("lon") || col2.equalsIgnoreCase("longitude"))
              {
                return true;
              }
            }
          }
        }
      }
      finally
      {
        is.close();
      }
    }
    catch (final MalformedURLException e)
    {
      e.printStackTrace();
    }
    catch (FileNotFoundException fnf)
    {
      // Ignore. This is expected when checking to see if the passed URL
      // has a columns file, but really doesn't.
    }
    catch (final IOException e)
    {
      e.printStackTrace();
    }
    catch (URISyntaxException e)
    {
      e.printStackTrace();
    }
    return false;
  }

  private static boolean hasGeometry(final URL url) throws IOException
  {
    boolean hasGeometry = false;

    InputStream is = url.openStream();

    CsvGeometryInputStream cgis = new CsvGeometryInputStream(is);
    hasGeometry = cgis.hasGeometry();
    is.close();

    if (!hasGeometry)
    {
      is = url.openStream();
      TsvGeometryInputStream tgis = new TsvGeometryInputStream(is);
      hasGeometry = tgis.hasGeometry();
      is.close();
    }

    return hasGeometry;
  }

  // @Override
  // public boolean canProcess(Map<String, Serializable> params)
  // {
  // return false;
  // }

  @Override
  public boolean canProcess(final URL url)
  {
    try
    {
      return (hasColumns(url) || hasGeometry(url));
    }
    catch (final IOException e)
    {
      e.printStackTrace();
    }

    return false;

  }

  @Override
  public DataStore createDataStore(final Map<String, Serializable> params) throws IOException
  {
    if (params.containsKey("url"))
    {
      URL url = (URL) params.get("url");
      return createDataStore(url);
    }
    return null;
  }

  @Override
  public FileDataStore createDataStore(final URL url) throws IOException
  {
    return new CsvDataStore(url);
  }

  @Override
  public DataStore createNewDataStore(final Map<String, Serializable> params) throws IOException
  {
    return null;
  }

  @Override
  public String getDescription()
  {
    return "CSV File containing geospatial vector column(s)";
  }

  @Override
  public String[] getFileExtensions()
  {
    return new String[] { "csv", "tsv" };
  }

  @Override
  public Param[] getParametersInfo()
  {
    return parameters;
  }

  @Override
  public String getTypeName(final URL url) throws IOException
  {
    return null;
  }

}
