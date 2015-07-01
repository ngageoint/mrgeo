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

package org.mrgeo.utils.geotools;

import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.kml.v22.KML;
import org.geotools.kml.v22.KMLConfiguration;
import org.geotools.xml.Configuration;
import org.geotools.xml.PullParser;
import org.mrgeo.utils.HadoopURLStreamHandlerFactory;
import org.opengis.feature.GeometryAttribute;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.AttributeType;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Set;


public abstract class GeotoolsVectorReader
{
  static final Logger log = LoggerFactory.getLogger(GeotoolsVectorReader.class);

  private static class SimpleFeatureIteratorReader extends GeotoolsVectorReader
  {
    final private SimpleFeatureIterator reader;
    final FileDataStore store;

    final private SimpleFeatureType schema;

    public SimpleFeatureIteratorReader(URL url) throws IOException
    {
      store = FileDataStoreFinder.getDataStore(url);
      SimpleFeatureSource featureSource = store.getFeatureSource();

      schema = featureSource.getSchema();

      SimpleFeatureCollection features = featureSource.getFeatures();

      reader = features.features();
    }

    @Override
    public boolean hasNext()
    {
      if (reader != null)
      {
        return reader.hasNext();
      }

      return false;
    }

    @Override
    public SimpleFeature next()
    {
      if (reader != null)
      {
        return reader.next();
      }

      return null;
    }

    @Override
    public void close()
    {
      reader.close();
      store.dispose();
    }

    @Override
    public String[] getAttributeNames()
    {
      if (schema != null)
      {
        List<AttributeDescriptor> attrs = schema.getAttributeDescriptors();
        String names[] = new String[attrs.size() - 1];

        String geom = schema.getGeometryDescriptor().getLocalName();
        int i = 0;
        for (AttributeDescriptor attr: attrs)
        {
          String name = attr.getLocalName();

          // skip the geometry attribute
          if (!name.equals(geom))
          {
            names[i++] = attr.getLocalName();
          }
        }

        return names;
      }

      return null;
    }

    @Override
    public Class<?>[] getAttributeClasses()
    {
      if (schema != null)
      {
        List<AttributeDescriptor> attrs = schema.getAttributeDescriptors();
        Class<?> types[] = new Class<?>[attrs.size() - 1];

        String geom = schema.getGeometryDescriptor().getLocalName();
        int i = 0;
        for (AttributeDescriptor attr: attrs)
        {
          String name = attr.getLocalName();

          // skip the geometry attribute
          if (!name.equals(geom))
          {
            AttributeType type = attr.getType();
            types[i++] = type.getBinding();
          }
        }

        return types;
      }

      return null;
    }

    static boolean canProcess(URL url)
    {
      FileDataStore store;
      try
      {
        store = FileDataStoreFinder.getDataStore(url);
        return store != null;
      }
      catch (IOException e)
      {
      }

      return false;
    }
  }

  private static class XMLParserReader extends GeotoolsVectorReader
  {
    final private PullParser parser;
    final private InputStream stream;
    private SimpleFeature feature;
    private boolean prereadFeature = false;

    static private Set<Class<? extends Configuration>> subTypes = null;

    public XMLParserReader(final URL url) throws IOException
    {
      stream = url.openStream();

      Configuration config = getConfig(url);
      if (config != null)
      {
        QName name = getQNameFromConfig(config);
        parser = new PullParser(config, stream, name);
      }
      else
      {
        throw new IOException("Could not find a parser to handle URL: " + url.toString());
      }
    }

    @Override
    public boolean hasNext()
    {
      if (prereadFeature)
      {
        return true;
      }

      try
      {
        feature = (SimpleFeature) parser.parse();
        return feature != null;
      }
      catch (XMLStreamException e)
      {
      }
      catch (IOException e)
      {
      }
      catch (SAXException e)
      {
      }

      return false;
    }

    @Override
    public SimpleFeature next()
    {
      prereadFeature = false;
      return feature;
    }

    @Override
    public void close() throws IOException
    {
      stream.close();
    }

    @Override
    public String[] getAttributeNames()
    {
      if (feature == null)
      {
        if (!hasNext())
        {
          return new String[]{};          
        }

        prereadFeature = true;
      }

      GeometryAttribute gattr = feature.getDefaultGeometryProperty();
      String geom = gattr.getName().getLocalPart();

      Collection<Property> attrs = feature.getProperties();

      String names[] = new String[attrs.size() - 1];
      int i = 0;
      for (Property attr : attrs)
      {
        String name = attr.getName().getLocalPart();
        if (!geom.equals(name))
        {
          names[i++] = name;
        }
      }

      return names;
    }

    @Override
    public Class<?>[] getAttributeClasses()
    {
      if (feature == null)
      {
        if (!hasNext())
        {
          return new Class<?>[]{};
        }

        prereadFeature = true;
      }

      GeometryAttribute gattr = feature.getDefaultGeometryProperty();
      String geom = gattr.getName().getLocalPart();

      Collection<Property> attrs = feature.getProperties();

      Class<?> types[] = new Class<?>[attrs.size() - 1];
      int i = 0;
      for (Property attr : attrs)
      {
        AttributeType type = (AttributeType) attr.getType();

        if (!geom.equals(type.getName().getLocalPart()))
        {
          types[i++] = type.getBinding();
        }
      }

      return types;
    }

    static boolean canProcess(URL url)
    {
      return (getConfig(url) != null);
    }

    private static Configuration getConfig(final URL url)
    {
      if (subTypes == null)
      {
        Reflections reflections = new Reflections("org.geotools");
        subTypes =  reflections.getSubTypesOf(Configuration.class);
      }

      for (Class<? extends Configuration> clazz : subTypes)
      {
        InputStream stream = null;

        try
        {
          Configuration config = clazz.newInstance();
          QName name = getQNameFromConfig(config);
          if (name != null)
          {
            stream = url.openStream();

            PullParser p = new PullParser(config, stream, name);
            SimpleFeature f = (SimpleFeature) p.parse();
            if (f != null)
            {
              return config;
            }
          }
        }
        catch (Exception e)
        {
          // eat any exceptions...
        } 
        finally
        {
          if (stream != null)
          {
            try
            {
              stream.close();
            }
            catch (IOException e)
            {
            }
          }
        }
      }

      return null;
    }

    private static QName getQNameFromConfig(final Configuration config)
    {
      if (config instanceof KMLConfiguration)
      {
        return KML.Placemark;
      }

      return null;
    }
  }

  static GeotoolsVectorReader open(URI uri) throws IOException, MalformedURLException
  {
    URL url = HadoopURLStreamHandlerFactory.createHadoopURL(uri);
    if (SimpleFeatureIteratorReader.canProcess(url))
    {
      return new SimpleFeatureIteratorReader(url);
    }
    else if (XMLParserReader.canProcess(url))
    {
      return new XMLParserReader(url);
    }

    return null;
  }

  abstract public boolean hasNext();
  abstract public SimpleFeature next();
  abstract public String[] getAttributeNames();
  abstract public Class<?>[] getAttributeClasses();

  abstract public void close() throws IOException;
}
