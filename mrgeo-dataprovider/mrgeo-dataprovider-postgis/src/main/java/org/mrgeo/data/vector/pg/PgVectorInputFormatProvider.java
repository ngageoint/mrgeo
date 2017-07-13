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

package org.mrgeo.data.vector.pg;

import org.apache.hadoop.mapreduce.InputFormat;
import org.mrgeo.data.vector.FeatureIdWritable;
import org.mrgeo.data.vector.VectorInputFormatContext;
import org.mrgeo.data.vector.VectorInputFormatProvider;
import org.mrgeo.geometry.Geometry;

public class PgVectorInputFormatProvider extends VectorInputFormatProvider
{
//  private PgVectorDataProvider dataProvider;
  private PgDbSettings dbSettings;

  public PgVectorInputFormatProvider(VectorInputFormatContext context,
                                     PgVectorDataProvider dataProvider,
                                     PgDbSettings dbSettings)
  {
    super(context);
//    this.dataProvider = dataProvider;
    this.dbSettings = dbSettings;
  }

  @Override
  public InputFormat<FeatureIdWritable, Geometry> getInputFormat(String input)
  {
    return new PgVectorInputFormat(dbSettings);
  }
}
