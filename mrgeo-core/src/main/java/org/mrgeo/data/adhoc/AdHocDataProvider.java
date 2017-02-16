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

package org.mrgeo.data.adhoc;

import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.data.DataProviderException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Abstract base class which a MrGeo data plugin will extend in order to
 * provide read/write access for a collection of ad hoc data. This provider
 * is used by MrGeo to read/write MrsImage metadata, image statistics during
 * processing, and color scales.
 */
public abstract class AdHocDataProvider
{
private String resourceName;

/**
 * This constructor should only be used by sub-classes in the case where
 * they don't know the resource name when they are constructed. They are
 * responsible for subsequently calling setResourceName.
 */
protected AdHocDataProvider()
{
  resourceName = null;
}

/**
 * Create a new instance of an ad hoc data provider that references
 * the named data source. The data plugin treats the name passed
 * in in way that makes sense to it. For example, the HDFS plugin
 * treats the name to a directory in the HDFS.
 *
 * @param resourceName
 */
public AdHocDataProvider(String resourceName)
{
  this.resourceName = resourceName;
}

/**
 * Give back the resource name that this provider was constructed with.
 *
 * @return
 */
public String getResourceName()
{
  return resourceName;
}

/**
 * Sub-classes which use the default constructor must subsequently call
 * this method to assign the resource name,
 *
 * @param resourceName
 */
protected void setResourceName(String resourceName)
{
  this.resourceName = resourceName;
}

/**
 * Perform Hadoop job setup that is required for using this ad hoc
 * resource in a map/reduce job. This base class implementation adds
 * the data plugin JAR to Hadoop's distributed cache to make it
 * available on the cluster nodes as part of the job. So subclasses
 * that override this method should always invoke this base class method.
 *
 * @param job
 * @throws DataProviderException
 */
public void setupJob(final Job job) throws DataProviderException
{
}

/**
 * Deletes the data source associated with this data provider.
 *
 * @throws IOException When any problem occurs that prevents a
 *                     successful deletion.
 */
public abstract void delete() throws IOException;

/**
 * Moves this data source to the specified resource location.
 *
 * @param toResource
 * @throws IOException When any problem occurs that prevents a
 *                     successful move.
 */
public abstract void move(String toResource) throws IOException;

/**
 * Add a new data resource to this ad hoc data provider. Callers should use
 * this add() signature in cases where they don't care what the resource name
 * is (it is randomly assigned). To access this data, the caller would use
 * size() and get(). The data can be retrieved in the same order it was
 * added.
 *
 * @return
 * @throws IOException
 */
public abstract OutputStream add() throws IOException;

/**
 * Add a new data resource to this ad hoc data provider. Callers should use
 * this add() signature in cases where they wish to assign a specific name
 * to the resource. To access this data, the caller would use the get(name)
 * method.
 *
 * @param name
 * @return
 * @throws IOException
 */
public abstract OutputStream add(final String name) throws IOException;

/**
 * Give back the number of data resources that have been added to this ad hoc
 * data provider.
 *
 * @return
 * @throws IOException
 */
public abstract int size() throws IOException;

/**
 * Give back an InputStream to the specified item in this provider. The index
 * is 0-based and depends on the order in which data resources were added to
 * the provider. This is mostly useful in the case where unnamed resources
 * were added.
 *
 * @param index
 * @return
 * @throws IOException
 */
public abstract InputStream get(final int index) throws IOException;

/**
 * Give back an InputStream to the named resource from this provider. This will
 * only work if the resource was added using add(name) in the first place.
 *
 * @param name
 * @return
 * @throws IOException
 */
public abstract InputStream get(final String name) throws IOException;

/**
 * Give back the name of the specified item in this provider. The index
 * is 0-based and depends on the order in which data resources were added to
 * the provider.
 *
 * @param index
 * @return
 * @throws IOException
 */
public abstract String getName(final int index) throws IOException;

}
