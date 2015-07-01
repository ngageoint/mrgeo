/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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

import org.apache.commons.io.FilenameUtils;
import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.FileDataStore;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.NameImpl;
import org.mrgeo.data.csv.CsvGeometryInputStream;
import org.mrgeo.data.tsv.TsvGeometryInputStream;
import org.mrgeo.hdfs.vector.ColumnDefinitionFile;
import org.mrgeo.utils.HadoopURLStreamHandlerFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.opengis.filter.Filter;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.List;

public class CsvDataStore extends ContentDataStore implements FileDataStore
{

  private URL url = null;
  private boolean isCsv = false;
  private Name typename;

  public CsvDataStore(URL url)
  {

    try
    {
      InputStream columnsStream = getColumnsStream(url);
      InputStream is = url.openStream();
      try
      {
        TsvGeometryInputStream tis = new TsvGeometryInputStream(is, columnsStream);

        if (tis.hasGeometry())
        { 
          this.url = url;
          isCsv = false;
        }
      }
      finally
      {
        is.close();
        if (columnsStream != null)
        {
          columnsStream.close();
        }
      }

      if (this.url == null)
      {
        columnsStream = getColumnsStream(url);
        is = url.openStream();
        try
        {
          CsvGeometryInputStream cis = new CsvGeometryInputStream(is, columnsStream);

          if (cis.hasGeometry())
          { 
            this.url = url;
            isCsv = true;
          }
        }
        finally
        {
          is.close();
          if (columnsStream != null)
          {
            columnsStream.close();
          }
        }
      }
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }

    String path = FilenameUtils.removeExtension(url.toString());
    int slash = Math.max(0, path.lastIndexOf('/') + 1);
    int dot = path.indexOf('.', slash);

    if (dot < 0) {
      dot = path.length();
    }

    String local =  path.substring(slash, dot);

    typename = new NameImpl(namespaceURI, local);

  }

  @Override
  public void dispose()
  {
    super.dispose();
  }

  @Override
  public SimpleFeatureType getSchema() throws IOException
  {
    return null;
  }

  @Override
  public void updateSchema(SimpleFeatureType featureType) throws IOException
  {
  }

  @Override
  public SimpleFeatureSource getFeatureSource() throws IOException
  {
    ContentEntry entry = ensureEntry(typename);
    return new CsvFeatureSource(entry, openStream());
  }

  @Override
  public FeatureReader<SimpleFeatureType, SimpleFeature> getFeatureReader() throws IOException
  {
    return ((CsvFeatureSource)getFeatureSource()).getReader();
  }

  @Override
  public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriter(Filter filter,
    Transaction transaction) throws IOException
    {
    return null;
    }

  @Override
  public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriter(Transaction transaction)
      throws IOException
      {
    return null;
      }

  @Override
  public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriterAppend(
    Transaction transaction) throws IOException
    {
    return null;
    }

  @Override
  protected List<Name> createTypeNames() throws IOException
  {
    return Collections.singletonList(typename);
  }

  @Override
  protected ContentFeatureSource createFeatureSource(ContentEntry entry) throws IOException
  {
    return null;
  }

  private static InputStream getColumnsStream(URL base)
  {
    InputStream columnsStream = null;
    try
    {
    final URI uri = new URI(base.toString() + ".columns");
    final URL cols = HadoopURLStreamHandlerFactory.createHadoopURL(uri);

      columnsStream = cols.openStream();
      
      @SuppressWarnings("unused")
      ColumnDefinitionFile cdf = new ColumnDefinitionFile(columnsStream);
      
      columnsStream.close();
      columnsStream = cols.openStream();
    }
    catch (IOException e)
    {
      if (columnsStream != null)
      {
        try
        {
          columnsStream.close();
        }
        catch (IOException e1)
        {
          e1.printStackTrace();
        }
      }
      
      columnsStream = null;
    }
    catch (URISyntaxException e)
    {
      e.printStackTrace();
      columnsStream = null;
    }
    
    return columnsStream;
  }
  
  private CsvGeometryInputStream openStream() throws IOException
  {
    if (url != null)
    {
      InputStream columnsStream = getColumnsStream(url);
      if (isCsv)
      {
        return new CsvGeometryInputStream(url.openStream(), columnsStream);
      }

      return new TsvGeometryInputStream(url.openStream(), columnsStream);
    }

    return null;
  }

}
