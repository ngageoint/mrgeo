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

package org.mrgeo.data.vector;

import org.apache.hadoop.conf.Configuration;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.ProviderProperties;

import java.io.IOException;
import java.util.Map;

public interface VectorDataProviderFactory
{
/**
 * This function is called by MrGeo so that providers can make sure they have
 * the necessary information to be used within MrGeo. For example, if login
 * or connection data must be configured properly in order for this provider
 * to work properly, that should be verified within this function. If this
 * function returns false, the provider will not be used within MrGeo.
 * <p>
 * If this method is called from within map/reduce task execution, the
 * Configuration passed in will be that from the mapper or reducer
 * job context. A typical scenario for a data provider is to include settings
 * required for validating the data provider in the Configuration during
 * job setup so that it will be available in the mapper and reducer.
 */
public boolean isValid(final Configuration conf);

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

public VectorDataProvider createVectorDataProvider(final String prefix,
    final String input,
    final Configuration conf,
    final ProviderProperties providerProperties);

public String[] listVectors(final Configuration conf,
                            final ProviderProperties providerProperties) throws IOException;

public boolean canOpen(final String input,
                       final Configuration conf,
                       final ProviderProperties providerProperties) throws IOException;

public boolean canWrite(final String input,
                        final Configuration conf,
                        final ProviderProperties providerProperties) throws IOException;

public boolean exists(final String name,
                      final Configuration conf,
                      final ProviderProperties providerProperties) throws IOException;

public void delete(final String name,
                   final Configuration conf,
                   final ProviderProperties providerProperties) throws IOException;
}
