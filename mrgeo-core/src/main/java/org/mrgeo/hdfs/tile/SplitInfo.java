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

package org.mrgeo.hdfs.tile;

import java.io.Externalizable;

abstract public class SplitInfo implements Externalizable
{
  abstract boolean compareEQ(long tileId);
  abstract boolean compareLE(long tileId);
  abstract boolean compareLT(long tileId);
  abstract boolean compareGE(long tileId);
  abstract boolean compareGT(long tileId);

  public abstract long getTileId();
  public abstract int getPartition();
}
