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

package org.mrgeo.utils;

import org.apache.hadoop.fs.Path;
import org.mrgeo.hdfs.utils.HadoopFileUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.*;
import java.util.HashMap;
import java.util.Map;

public class HadoopURLStreamHandlerFactory implements URLStreamHandlerFactory
{
  private static class HadoopStreamHandler extends URLStreamHandler
  {

    private final org.apache.hadoop.conf.Configuration conf;

    // HadoopStreamHandler()
    // {
    // this.conf = new Configuration();
    // }

    HadoopStreamHandler(final org.apache.hadoop.conf.Configuration conf)
    {
      this.conf = conf;
    }

    @Override
    protected HDFSConnection openConnection(final URL url) throws IOException
    {
      return new HDFSConnection(conf, url);
    }
  }

  private static class HDFSConnection extends URLConnection
  {

    private final org.apache.hadoop.conf.Configuration conf;

    private InputStream is;

    HDFSConnection(final org.apache.hadoop.conf.Configuration conf, final URL url)
    {
      super(url);
      this.conf = conf;
    }

    @Override
    public void connect() throws IOException
    {
//      is = HadoopFileUtils.open(conf, new Path(url.getPath())); // fs.open(new Path(url.getPath()));
      try
      {
        //final FileSystem fs = FileSystem.get(url.toURI(), conf);
        is = HadoopFileUtils.open(conf, new Path(url.toURI())); // fs.open(new Path(url.getPath()));
      }
      catch (final java.net.URISyntaxException e)
      {
        throw new IOException(e);
      }
    }

    @Override
    public InputStream getInputStream() throws IOException
    {
      if (is == null)
      {
        connect();
      }
      return is;
    }

  }

  // This map stores the protocols we understand
  private final Map<String, URLStreamHandler> protocols = new HashMap<String, URLStreamHandler>();

  public HadoopURLStreamHandlerFactory()
  {
    this(new org.apache.hadoop.conf.Configuration());
  }

  public HadoopURLStreamHandlerFactory(final org.apache.hadoop.conf.Configuration conf)
  {
    protocols.put("hdfs", new HadoopStreamHandler(conf));
  }

  @Override
  public java.net.URLStreamHandler createURLStreamHandler(final String protocol)
  {
    final String p = protocol.toLowerCase();
    if (protocols.containsKey(p))
    {
      return protocols.get(p);
    }

    // We don't know the protocol, let the VM handle this
    return null;
  }
  
  public static URL createHadoopURL(URI uri) throws MalformedURLException
  {
    // Construct the URL. Because this code runs both within a standalone JVM as well
    // as within a web server like Tomcat, we need to have special handling of url's
    // that use the HDFS protocol since it's not part of the standard protocol handlers
    // of java.
    URL url;
    try
    {
      url = uri.toURL();
    }
    catch(MalformedURLException e)
    {
      HadoopURLStreamHandlerFactory factory = new HadoopURLStreamHandlerFactory();
      url = new URL(uri.getScheme(), uri.getHost(), uri.getPort(), uri.getPath(),
          factory.createURLStreamHandler(uri.getScheme()));
    }
    return url;
  }
}
