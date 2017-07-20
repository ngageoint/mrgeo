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

package org.mrgeo.data.adhoc;

import org.apache.hadoop.conf.Configuration;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.ProviderProperties;

import java.io.IOException;
import java.util.Map;


/**
 * Interface to be implemented by a data plugin that wishes to provide ad hoc
 * data support. Ad hoc data is used for storing information that does not fit
 * into the other data provider categories (e.g. it's not a raw image used for
 * ingesting imagery, and it's not a MrsImage). This includes image metadata
 * stored with a MrsImage, statistics that MrGeo collects about an image during
 * processing, and color scales.
 * <p>
 * MrGeo uses the Java ServiceLoader to discover
 * classes that implement this interface. In order for that to work, plugins need to
 * include a text file in their JAR file in
 * META-INF/services/org.mrgeo.data.adhoc.AdHocDataProviderFactory. That file
 * must contain a single line with the full name of the class that implements this
 * interface - for example org.mrgeo.hdfs.adhoc.HdfsAdHocDataProviderFactory
 */
public interface AdHocDataProviderFactory
{
/**
 * Provider implementations should perform any needed checks within this method
 * to determine if the other functions defined in this interface can be called
 * reliably. For example, if the implementation requires some configuration
 * settings in order to work properly, it can use this method to determine if
 * those settings have been defined. Returning false from this method will prevent
 * the provider from being called thereafter.
 *
 * @return
 */
public boolean isValid(Configuration conf);

/**
 * This method is called once when DataProviderFactory finds this factory.
 * The factory can perform whatever initialization functionality it needs
 * within this method.
 *
 * @param conf
 */
public void initialize(Configuration conf) throws DataProviderException;

public String getPrefix();

/**
 * This method is called before a Spark job runs in order to get back a set of
 * properties that the data provider needs for instantiating providers on the
 * remote side of a Spark job. Since MrGeo is not installed across the cluster,
 * the configuration settings for the data provider are only available before
 * the job is submitted. The MrGeo framework will ensure that the returned
 * properties are passed along with the job, and on the remote side, the
 * setConfiguration() method will be invoked.
 * <p>
 * Some providers require login credentials for connecting to the source data,
 * and this method would have to be implemented in that case to get those
 * credentials passed to the remote side.
 * <p>
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
 * <p>
 * The properties passed in will include all the settings from the Spark job,
 * including settings from other providers. So each provider must filter based
 * on the prefix they used for their settings in the getConfiguration() method.
 *
 * @param properties
 */
public void setConfiguration(Map<String, String> properties);

/**
 * Give back an ad hoc data provider for a named resource. The name is interpreted
 * by the plugin itself. For example, in the HDFS plugin, that name is translated
 * to a path.
 *
 * @param name
 * @return
 */
public AdHocDataProvider createAdHocDataProvider(String name,
                                                 Configuration conf,
                                                 ProviderProperties providerProperties) throws IOException;

/**
 * Give back an ad hoc data provider for a resource that is named by this method.
 * Callers use this if they need to store ad hoc data, but don't care where that
 * data resides. The caller must keep the returned provider instance for as long
 * as they wish to access that ad hoc data.
 *
 * @return
 */
public AdHocDataProvider createAdHocDataProvider(
        Configuration conf,
        ProviderProperties providerProperties) throws IOException;

public boolean canOpen(String name, Configuration conf, ProviderProperties providerProperties) throws IOException;

public boolean canWrite(String name, Configuration conf, ProviderProperties providerProperties) throws IOException;

public boolean exists(String name, Configuration conf, ProviderProperties providerProperties) throws IOException;

public void delete(String name, Configuration conf, ProviderProperties providerProperties) throws IOException;
}
