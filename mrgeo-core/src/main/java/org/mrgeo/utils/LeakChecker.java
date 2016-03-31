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

package org.mrgeo.utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class LeakChecker
{
  private static LeakChecker checker = null;
  
  private final Map<Integer, String> leaks = Collections
      .synchronizedMap(new HashMap<Integer, String>());
  
  public static LeakChecker instance()
  {
    if (checker == null)
    {
      checker = new LeakChecker();
    }
    return checker;
  }
  
  public void add(final Object obj, final String stack)
  {
    int id = System.identityHashCode(obj);
    if (!leaks.containsKey(id))
    {
//      System.err.println("adding: " + Integer.toHexString(id));
      leaks.put(id, stack);
    }
  }
  
  public void remove(final Object obj)
  {
    int id = System.identityHashCode(obj);
    if (leaks.containsKey(id))
    {
//      System.err.println("removing:" +  Integer.toHexString(id));
      
      leaks.remove(id);
    }
  }
  
  public Map<Integer, String> getAll()
  {
    return leaks;
  }

}
