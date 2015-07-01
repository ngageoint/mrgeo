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

package org.mrgeo.mapreduce.formats;

import java.util.HashMap;
import java.util.Map;

public class TileCollection<T>
{
  public class Cluster extends HashMap<Long, T>
  {    
    private static final long serialVersionUID = 1L;

    Cluster()
    {}
  }

  private final Map<String, Cluster> clusters = new HashMap<String, Cluster>();
  private long tileid;

  public TileCollection()
  {
    clear();
  }

  public void clear()
  {
    tileid = -1;
    clusters.clear();
  }

  public Cluster getCluster(final String key)
  {
    return clusters.get(key);
  }

  public T get(final String key)
  {
    Cluster cluster = clusters.get(key);
    if (cluster != null)
    {
      return cluster.get(tileid);
    }
    
    return null;
  }
  
  public T get()
  {
    for (Cluster cluster: clusters.values())
    {
      return cluster.get(tileid);
    }
    
    return null;
  }


  public long getTileid()
  {
    return tileid;
  }

  public void set(final String key, final long id, final T tileData)
  {
    Cluster cluster = clusters.get(key);
    if (cluster == null)
    {
      cluster = new Cluster();
      clusters.put(key, cluster);
    }

    cluster.put(id, tileData);
  }

  // convenience function to add to a cluster the "main" tile for a cluster
  public void set(final String key, final T tileData)
  {
    Cluster cluster = clusters.get(key);
    if (cluster == null)
    {
      cluster = new Cluster();
      clusters.put(key, cluster);
    }

    cluster.put(tileid, tileData);
  }


  public void setTileid(final long id)
  {
    tileid = id;
  }

}
