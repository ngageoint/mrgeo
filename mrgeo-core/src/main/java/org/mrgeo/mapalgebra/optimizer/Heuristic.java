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

package org.mrgeo.mapalgebra.optimizer;

import org.mrgeo.mapalgebra.old.MapOpHadoop;

/**
 * Given a MapOp tree the Heuristic will estimate how long it will take to
 * execute. This should be a relatively fast operation.
 */
public class Heuristic
{

  /**
   * For now something dirt simple. Operations (nodes) cost 1 time unit and data
   * transfers (edges) cost 2 time units.
   * 
   * @param root
   *          The root node of the tree
   * @return An estimate of how long it will take to execute (ideally in
   *         seconds).
   */
  public double estimate(MapOpHadoop root)
  {
    double result = 1.0;
    
    if (root.getClass().getName().contains("Legion"))
    {
      result += 1;
    }

    for (MapOpHadoop child : root.getInputs())
    {
      result += 2.0;
      result += estimate(child);
    }

    return result;
  }
}
