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

package org.mrgeo.hdfs.vector;

import org.mrgeo.geometry.WritableGeometry;

import java.io.Serializable;

/**
 * When serialized only the information necessary to read the data is saved.
 * E.g. If you're reading from a database, the connection info and query are
 * stored, not the actual data.
 * <p>
 * If applicable, it is advised that you implement HdfsResource as well.
 *
 * @author jason.surratt
 */
public interface ShapefileGeometryCollection extends Iterable<WritableGeometry>, Serializable
{

/**
 * The specified index starts at zero and increases up to size() - 1. This is
 * not associated with feature IDs.
 *
 * @param index
 * @return
 */
WritableGeometry get(int index);

/**
 * Returns the projection of the geometry collection as WKT.
 *
 * @return
 */
String getProjection();

int size();

void close();
}
