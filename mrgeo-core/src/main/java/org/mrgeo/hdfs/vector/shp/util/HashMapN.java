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

package org.mrgeo.hdfs.vector.shp.util;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

@SuppressWarnings("unchecked")
class HashMapN extends HashMapV
{
public static final int RELATION_ANCESTOR = 2;
public static final int RELATION_CHILD = 1;
public static final int RELATION_NONE = 0;
public static final int RELATION_OTHER = 4;
public static final int RELATION_PARENT = 5;
public static final int RELATION_SIBLING = 6;
private static final long serialVersionUID = 1L;

public static int getRelation(Object[] key1, Object[] key2)
{
  int depth = getRelationDepth(key1, key2);
  if (depth == 0)
    return RELATION_NONE;
  if (key1.length == key2.length && depth == (key1.length - 1))
    return RELATION_SIBLING;
  if ((key1.length - 1) == key2.length && depth == (key1.length - 1))
    return RELATION_CHILD;
  if (key1.length == (key2.length - 1) && depth == key1.length)
    return RELATION_PARENT;
  if (key1.length < key2.length && depth == key1.length)
    return RELATION_ANCESTOR;
  return RELATION_OTHER;
}

public static int getRelationDepth(Object[] key1, Object[] key2)
{
  if (key1 == null || key2 == null)
    return 0;
  for (int i = 0; i < key1.length; i++)
  {
    if (key1[i] != key2[i])
      return i;
  }
  return key1.length;
}

public static String getRelationEnum(int relation)
{
  switch (relation)
  {
  case RELATION_NONE:
    return "NONE";
  case RELATION_CHILD:
    return "CHILD";
  case RELATION_ANCESTOR:
    return "ANCESTOR";
  case RELATION_OTHER:
    return "OTHER";
  case RELATION_PARENT:
    return "PARENT";
  case RELATION_SIBLING:
    return "SIBLING";
  default:
    return null;
  }
}


private int level;
@SuppressWarnings("rawtypes")
private List listeners;
@SuppressWarnings("rawtypes")
private HashMap reverseTraversalBag = null;
private Object rootValue = null;
private Object[] route = null;

public HashMapN()
{
  super();
}

public HashMapN(int size)
{
  super(size);
}

@SuppressWarnings("rawtypes")
public void addListener(HashMapNListener listener)
{
  if (listeners == null)
    listeners = new ArrayList(1);
  listeners.add(listener);
}

@SuppressFBWarnings(value = "SIC_INNER_SHOULD_BE_STATIC_ANON", justification = "Anonymous, not refactoring")
public void debug()
{
  HashMapNAdapter adapter = new HashMapNAdapter()
  {
    @Override
    public void traversed(HashMapV map, Object key, int lvl)
    {
      String padding = StringUtils.pad(lvl * 4 - 4, ' ') + "|-";
      System.out.println(padding + key + " [" + map.getValue() + "]");
    }
  };
  addListener(adapter);
  traverse();
  removeListener(adapter);
}

@SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "API")
public Object[] dig(Object[] key)
{
  if (key == null)
    return null;
  HashMapV map = get(key);
  if (map == null)
    return null;
  level = 0;
  route = null;
  digImpl(key, map);
  return route.clone();
}

private void digImpl(Object[] key, HashMapV map)
{
  if (key.length > level)
  {
    level = key.length;
    route = key;
  }
  @SuppressWarnings("rawtypes")
  Iterator iter = map.keySet().iterator();
  while (iter.hasNext())
  {
    Object next = iter.next();
    Object[] temp = new Object[key.length + 1];
    System.arraycopy(key, 0, temp, 0, key.length);
    temp[key.length] = next;
    digImpl(temp, get(temp));
  }
}

public HashMapV get(Object key[])
{
  if (key == null)
    return null;
  HashMapV map = this;
  for (int i = 0; i < key.length; i++)
  {
    if (key[i] == null)
      return null;
    HashMapV temp = (HashMapV) map.get(key[i]);
    if (temp == null)
      return null;
    map = temp;
  }
  return map;
}

@SuppressWarnings("rawtypes")
public List getChildren(Object[] key)
{
  if (key.length <= 1)
    return null;
  HashMapV map = get(key);
  Iterator iter = map.keySet().iterator();
  Object[] temp = null;
  List list = new ArrayList();
  while (iter.hasNext())
  {
    Object next = iter.next();
    temp = new Object[key.length + 1];
    System.arraycopy(key, 0, temp, 0, key.length);
    temp[key.length - 1] = next;
    list.add(temp);
  }
  return list;
}

public HashMapV getParent(Object[] key)
{
  if (key.length == 0)
    return this;
  Object[] temp = new Object[key.length - 1];
  System.arraycopy(key, 0, temp, 0, temp.length);
  return get(temp);
}

@SuppressWarnings("rawtypes")
public List getSiblings(Object[] key)
{
  if (key.length < 1)
    return null;
  HashMapV map = getParent(key);
  if (map.size() == 0)
    return null;
  Iterator iter = map.keySet().iterator();
  Object[] temp = null;
  List list = new ArrayList();
  while (iter.hasNext())
  {
    Object next = iter.next();
    temp = new Object[key.length];
    System.arraycopy(key, 0, temp, 0, key.length);
    temp[key.length - 1] = next;
    if (next != key[key.length - 1])
      list.add(temp);
  }
  return list;
}

@Override
public Object getValue()
{
  return rootValue;
}

public void put(Object[] key, Object val)
{
  if (key == null)
    throw new NullPointerException();
  HashMapV map = this;
  for (int i = 0; i < key.length; i++)
  {
    if (key[i] == null)
      throw new NullPointerException();
    HashMapV temp = (HashMapV) map.get(key[i]);
    if (temp == null)
    {
      temp = new HashMapV();
      map.put(key[i], temp);
    }
    map = temp;
  }
  map.setValue(val);
  if (listeners != null)
  {
    for (int i = 0; i < listeners.size(); i++)
    {
      HashMapNListener listener = (HashMapNListener) listeners.get(i);
      listener.added(map, val);
    }
  }
}

public HashMapV remove(Object key[])
{
  if (key == null)
    return null;
  HashMapV map = this;
  for (int i = 0; i < key.length - 1; i++)
  {
    if (key[i] == null)
      return null;
    HashMapV temp = (HashMapV) map.get(key[i]);
    if (temp == null)
      return null;
    map = temp;
  }
  HashMapV removal = (HashMapV) map.get(key[key.length - 1]);
  map.remove(key[key.length - 1]);
  if (listeners != null)
  {
    for (int i = 0; i < listeners.size(); i++)
    {
      HashMapNListener listener = (HashMapNListener) listeners.get(i);
      listener.removed(removal);
    }
  }
  return removal;
}

public void removeListener(HashMapNAdapter adapter)
{
  if (listeners == null)
    return;
  listeners.remove(adapter);
}

@SuppressWarnings("rawtypes")
public void reverseTraverse()
{
  // report
  if (listeners != null)
  {
    for (int i = 0; i < listeners.size(); i++)
    {
      HashMapNListener listener = (HashMapNListener) listeners.get(i);
      listener.reverseTraversedStart(this);
    }
  }
  reverseTraversalBag = new HashMap();
  Iterator iter = this.keySet().iterator();
  Object[] temp = dig(ObjectUtils.pack(iter.next()));
  reverseTraverseImpl(get(temp), temp);
  reverseTraversalBag = null;
}

private void reverseTraverseImpl(HashMapV map, Object[] key)
{
  // visited?
  if (reverseTraversalBag.containsKey(ObjectUtils.debug(key)))
    return;

  // report & add to visited bag
  if (listeners != null)
  {
    for (int i = 0; i < listeners.size(); i++)
    {
      HashMapNListener listener = (HashMapNListener) listeners.get(i);
      listener.reverseTraversed(map, key);
    }
  }
  reverseTraversalBag.put(ObjectUtils.debug(key), true);

  // dig siblings
  @SuppressWarnings("rawtypes")
  List siblings = getSiblings(key);
  for (int i = 0; i < siblings.size(); i++)
  {
    Object[] next = (Object[]) siblings.get(i);
    if (!reverseTraversalBag.containsKey(ObjectUtils.debug(next)))
    {
      Object[] temp = dig(next);
      reverseTraverseImpl(get(temp), temp);
    }
  }

  // visit parent
  HashMapV parent = getParent(key);
  Object[] temp = new Object[key.length - 1];
  if (temp.length == 0 && !reverseTraversalBag.containsKey(""))
  {
    reverseTraversalBag.put("", true);
    if (listeners != null)
    {
      for (int i = 0; i < listeners.size(); i++)
      {
        HashMapNListener listener = (HashMapNListener) listeners.get(i);
        listener.reverseTraversedRoot(this);
      }
    }
    return;
  }
  System.arraycopy(key, 0, temp, 0, temp.length);
  reverseTraverseImpl(parent, temp);
}

@Override
public void setValue(Object obj)
{
  this.rootValue = obj;
}

public void traverse()
{
  traverseImpl(this, "<ROOT>", 1);
}

@SuppressFBWarnings(value = "WMI_WRONG_MAP_ITERATOR", justification = "I don't know how to improve this")
private void traverseImpl(HashMapV map, Object key, int lvl)
{
  if (listeners != null)
  {
    for (int i = 0; i < listeners.size(); i++)
    {
      HashMapNListener listener = (HashMapNListener) listeners.get(i);
      listener.traversed(map, key, lvl);
    }
  }

  for (Object key2 : map.keySet())
  {
    HashMapV temp = (HashMapV) map.get(key2);
    traverseImpl(temp, key2, lvl + 1);
  }
}
}
