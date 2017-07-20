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

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.geometry.Geometry;

import java.io.IOException;

public interface VectorOutputFormatProvider
{
/**
 * Returns an instance of an OutputFormat for the data provider that
 * is responsible for translating the keys and values from the native
 * InputFormat to FeatureIdWritable keys and Geometry values.
 *
 * @return
 */
OutputFormat<FeatureIdWritable, Geometry> getOutputFormat(String input);

/**
 * Providers must perform any required Hadoop job setup when outputting
 * vector data.
 *
 * @param job
 * @throws IOException
 */
void setupJob(Job job) throws DataProviderException, IOException;

/**
 * Perform any processing required after the map/reduce has completed.
 *
 * @param job
 */
void teardown(Job job) throws DataProviderException;
}
