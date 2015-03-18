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

package org.mrgeo.data.ingest;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

/**
 * Interface to be implemented by a data plugin that wishes to provide data
 * for ingesting imagery into MrGeo. MrGeo uses the Java ServiceLoader to discover
 * classes that implement this interface. In order for that to work, plugins need to
 * include a text file in their JAR file in
 * META-INF/services/org.mrgeo.data.ingest.ImageIngestDataProviderFactory. That file
 * must contain a single line with the full name of the class that implements this
 * interface - for example org.mrgeo.hdfs.ingest.HdfsImageIngestDataProviderFactory
 */
public interface ImageIngestDataProviderFactory
{
  /**
   * Provider implementations should perform any needed checks within this method
   * to determine if the other functions defined in this interface can be called
   * reliably. For example, if the implementation requires some configuration
   * settings in order to work properly, it can use this method to determine if
   * those settings have been defined. Returning false from this method will prevent
   * the provider from being called thereafter.
   * @return
   */
  public boolean isValid();

  public String getPrefix();

  /**
   * Create an instance of an ImageIngestDataProvider for the back-end data store
   * that is implementing this interface. The returned data provider is
   * associated with the requested "input" resource passed in.
   * 
   * @param input The name of the resource to be accessed by the back-end
   * data store. The format of the input is determined by the plugin itself. For
   * example, the HDFS plugin would translate the input to a path in its HDFS
   * instance.
   * @return
   */
  public ImageIngestDataProvider createImageIngestDataProvider(final String input);

  public boolean canOpen(final String name) throws IOException;
  public boolean canWrite(final String name) throws IOException;
  public boolean exists(final String name) throws IOException;
  public void delete(final String name) throws IOException;
}
