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

package org.mrgeo.mapalgebra;

import org.mrgeo.mapreduce.formats.TileClusterInfo;

/**
 * Some MapOps require a neighborhood of tiles in order to do their job. Those
 * MapOps implement TileClusterInfoCalculator. Once all the MapOps in a tree have
 * been called to calculate the entire tile neighborhood required, all of the
 * TileClusterInfoConsumer implementors are called. These are the classes that
 * make use of the tile cluster info in order to produce their own output that
 * includes the required tile neighborhood.
 */
public interface TileClusterInfoConsumer
{
  /**
   * After the tile cluster info has been computed across the entire MapOp tree, the results are set
   * using this method. MapOp's that require neighborhood tiles to do their job should store the
   * passed tileClusterInfo for use during their execution.
   * 
   * @param tileClusterInfo
   */
  public void setOverallTileClusterInfo(final TileClusterInfo tileClusterInfo);
}
