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

import org.mrgeo.mapalgebra.MapOpHadoop;

import java.util.List;

/**
 * A basic rule base class with some convenience methods.
 */
public abstract class BasicRule implements Rule
{
  /**
   * Recursively look for the node that matches the position in the new tree
   * that it was in in the old tree.
   * 
   * @param oldAncestor
   * @param newAncestor
   * @param oldSubject
   * @return
   */
  protected MapOpHadoop _findNewSubject(MapOpHadoop oldAncestor, MapOpHadoop newAncestor, MapOpHadoop oldSubject)
  {
    MapOpHadoop result = null;

    if (oldAncestor == oldSubject)
    {
      result = newAncestor;
    }
    else
    {
      List<MapOpHadoop> oldChildren = oldAncestor.getInputs();
      List<MapOpHadoop> newChildren = newAncestor.getInputs();
      for (int i = 0; i < oldAncestor.getInputs().size(); i++)
      {
        result = _findNewSubject(oldChildren.get(i), newChildren.get(i), oldSubject);

        if (result != null)
        {
          break;
        }
      }
    }
    return result;
  }

  /**
   * Perform a depth first search looking for a child.
   * 
   * @param ancestor
   *          An ancestor of the child. Typically the root when first called.
   * @param child
   *          The child we're looking for.
   * @return Return the child's parent if it is found, otherwise return null.
   */
  protected MapOpHadoop _findParent(MapOpHadoop ancestor, MapOpHadoop child)
  {
    MapOpHadoop result = null;

    if (ancestor != child)
    {
      for (MapOpHadoop mo : ancestor.getInputs())
      {
        if (mo == child)
        {
          result = ancestor;
        }
        else
        {
          result = _findParent(mo, child);
        }

        if (result != null)
        {
          break;
        }
      }
    }

    return result;
  }

  /**
   * Finds a node in a tree and replaces it with a new node.
   * 
   * @param root
   *          Replace a node in this tree.
   * @param replacee
   *          Replace this old node.
   * @param replacer
   *          With this new node.
   * @return The newly modified tree. This may be the same as the old one unless
   *         the root has changed.
   */
  protected MapOpHadoop _replaceNode(MapOpHadoop root, MapOpHadoop replacee, MapOpHadoop replacer)
  {
    MapOpHadoop result = null;
    MapOpHadoop parent = _findParent(root, replacee);

    // if we're replacing the root node.
    if (parent == null)
    {
      result = replacer;
    }
    else
    {
      int i = 0;
      for (MapOpHadoop child : parent.getInputs())
      {
        if (child == replacee)
        {
          parent.setInput(i, replacer);
          result = root;
          break;
        }
        i++;
      }
    }
    return result;
  }
}
