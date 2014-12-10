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

package org.mrgeo.data;

import org.mrgeo.geometry.Geometry;

import java.io.IOException;
import java.util.Iterator;

/**
 * The features returned by this iterator are owned by the caller. This means
 * they may be modified w/o modifying the underlying data. If necessary the
 * implementor of this interface should call clone before returning the result.
 */
@Deprecated
public interface FeatureIterator extends Iterator<Geometry>
{

  public void close() throws IOException;
}
