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

import java.io.Closeable;
import java.io.IOException;

public interface LineProducer extends Closeable
{
/**
 * Returns the next available line from the source. If no more lines are
 * available, it returns null.
 *
 * @return next available line or null when there are no more
 * @throws IOException
 */
public String nextLine() throws IOException;
}
