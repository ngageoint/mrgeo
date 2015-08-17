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

package org.mrgeo.data.image;

import org.apache.hadoop.conf.Configuration;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.ProviderProperties;

import java.io.IOException;
import java.util.Map;

/**
 * A data plugin that provides support for image data must provide a class
 * that implements this interface. The core framework will use the appropriate
 * factory based on the name of the resource being accessed. Resource names
 * can include a specific prefix in its name in order to reference a particular
 * data plugin (e.g. hdfs:my_resource).
 * 
 * The MrGeo core code identifies plugins that support images by using the
 * Java ServiceLoader capabilities to find these classes. In order to enable
 * that mechanism to find the implementation, the plugin must include a file
 * named META-INF/services/org.mrgeo.data.image.MrsImageDataProviderFactory
 * in its JAR file. That file must be a text file with a line in it that
 * specifies the full package and class name of the class that implements
 * this interface
 * (e.g. com.mycompany.data.accumulo.AccumuloMrsImageDataProviderFactory).
 */
public interface MrsImageDataProviderFactory
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

  /**
   * This method is called once when DataProviderFactory finds this factory.
   * The factory can perform whatever initialization functionality it needs
   * within this method.
   *
   * @param conf
   */
  public void initialize(Configuration conf) throws DataProviderException;

  /**
   * Returns the prefix to be used for this image data provider. All data plugin
   * implementations must return unique prefixes.
   * 
   * @return
   */
  public String getPrefix();

  /**
   * This method is called before a Spark job runs in order to get back a set of
   * properties that the data provider needs for instantiating providers on the
   * remote side of a Spark job. Since MrGeo is not installed across the cluster,
   * the configuration settings for the data provider are only available before
   * the job is submitted. The MrGeo framework will ensure that the returned
   * properties are passed along with the job, and on the remote side, the
   * setConfiguration() method will be invoked.
   *
   * Some providers require login credentials for connecting to the source data,
   * and this method would have to be implemented in that case to get those
   * credentials passed to the remote side.
   *
   * IMPORTANT: The implementor should prefix any settings returned from this
   * method so they do not interfere with other settings in Spark job configuration
   * (including configuration settings from other providers). We suggest using the
   * data provider class name as a prefix for example.
   */
  public Map<String, String> getConfiguration();

  /**
   * This method is called on the remote side of a Spark job to allow provider
   * factories to re-instantiate their configuration settings. This is required
   * because MrGeo is not installed across the cluster, and so configuration files
   * or environment variables containing their settings are not available on the
   * remote side.
   *
   * The properties passed in will include all the settings from the Spark job,
   * including settings from other providers. So each provider must filter based
   * on the prefix they used for their settings in the getConfiguration() method.
   *
   * @param properties
   */
  public void setConfiguration(Map<String, String> properties);

  /**
   * Return a temporary (ephemeral) image data provider. The MrGeo core code
   * will ensure that this method is invoked on the proper data plugin for the
   * resource passed in. This method should only be called from the server side,
   * not from mappers and reducers.
   *
   * @return
   */
  public MrsImageDataProvider createTempMrsImageDataProvider(final ProviderProperties providerProperties) throws IOException;

  /**
   * Return the image data provider for the specified input. The MrGeo core code
   * will ensure that this method is invoked on the proper data plugin for the
   * resource passed in. This method should only be called from the server side,
   * not from mappers and reducers.
   *
   * @param input
   * @return
   */
  public MrsImageDataProvider createMrsImageDataProvider(final String input,
      final ProviderProperties providerProperties);

  /**
   * Return a list of all of the images that the data plugin knows about. This method
   * should only be called from the server side, not from mappers or reducers.
   * 
   * @return
   * @throws IOException
   */
  public String[] listImages(final ProviderProperties providerProperties) throws IOException;

  /**
   * Return true if this data plugin is capable of opening the specified
   * resource, the resource exists, and it is an image pyramid. Return
   * false otherwise. Note that the MrGeo core will possibly invoke this method
   * on resources that are not managed by this data plugin, in which case
   * this method should return false. This method should only be called from
   * the server side, not from mappers or reducers.
   * 
   * @param input
   * @return
   * @throws IOException
   */
  public boolean canOpen(final String input,
      final ProviderProperties providerProperties) throws IOException;

  /**
   * Returns true if the data plugin is able to create the specified
   * resource and it does not already exist. Note that the MrGeo core will
   * possibly invoke this method on resources that are not managed by this
   * data plugin, in which case this method should return false. This method
   * should only be called from server side, not mappers or reducers.
   * 
   * @param input
   * @return
   * @throws IOException
   */
  public boolean canWrite(final String input,
      final ProviderProperties providerProperties) throws IOException;

  /**
   * Returns true if the data plugin determines that the specified resource
   * exists. This method should only be called from
   * the server side, not from mappers or reducers.
   * 
   * @param name
   * @return
   * @throws IOException
   */
  public boolean exists(final String name,
      final ProviderProperties providerProperties) throws IOException;

  /**
   * Deletes the specified resource. This method should only be called from
   * the server side, not from mappers or reducers.
   * 
   * @param name
   * @throws IOException
   */
  public void delete(final String name,
      final ProviderProperties providerProperties) throws IOException;
}
