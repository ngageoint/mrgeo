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

import java.util.ArrayList;

/**
 * A rule is simply a way in which the tree can be manipulated and be equivalent
 * with the previous graph.
 * 
 * For example. a - a = 0
 * 
 * A rule has no concept of whether it is more or less efficient.
 */
public interface Rule
{

  /**
   * Apply the rule to the subject in the MapOp tree. This will only be called
   * if isApplicable returns true. In some cases it is just as expensive to apply
   * the rule as to determine if it is applicable. In these cases it may be
   * appropriate to return an empty ArrayList if the rule is indeed
   * inapplicable. A null should never be returned.
   * 
   * @param root
   *          Root of the MapOp tree provided for context.
   * @param subject
   *          The subject of the rule manipulation.
   * @return Returns the root of zero or more completely new copies of the tree.
   */
  public ArrayList<MapOpHadoop> apply(MapOpHadoop root, MapOpHadoop subject);

  /**
   * @return Returns a list of MapOp classes that this rule can operate on as
   *         subjects. If this rule can operate on all classes it returns a
   *         null.
   */
  public ArrayList<Class<? extends MapOpHadoop>> getCandidates();

  /**
   * Determine if this rule is applicable in the given situation.
   * 
   * @param root
   *          The root of the operation tree. The may not be used but is
   *          provided for context.
   * @param subject
   *          The node that will be operated on.
   * @return True if the rule can be applied, false otherwise.
   */
  public boolean isApplicable(MapOpHadoop root, MapOpHadoop subject);
}
