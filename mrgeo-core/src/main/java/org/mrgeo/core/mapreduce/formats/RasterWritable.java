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

package org.mrgeo.core.mapreduce.formats;

/**
 * This is a backward compatibility class only. There are existing MrGeo
 * outputs that reference this class. The actual class was moved to
 * org.mrgeo.data.raster package so in order for the existing sequence files
 * to be properly loaded, we include this class.
 */
@Deprecated
public class RasterWritable extends org.mrgeo.data.raster.RasterWritable
{
  public RasterWritable()
  {
    super();
  }

  public RasterWritable(final byte[] bytes)
  {
    super(bytes);
  }
}
