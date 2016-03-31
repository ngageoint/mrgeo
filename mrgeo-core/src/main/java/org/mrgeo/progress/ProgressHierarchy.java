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

package org.mrgeo.progress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Vector;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class ProgressHierarchy implements Progress, ProgressListener
{
  private static final Logger log = LoggerFactory.getLogger(ProgressHierarchy.class);

  Progress reportTo = null;
  // The locks are used to control access to children and weights since they
  // are parallel vectors, we want to ensure that adding an element to both
  // is atomic.
  private ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private Lock readLock = readWriteLock.readLock();
  private Lock writeLock = readWriteLock.writeLock();
  private Vector<ProgressHierarchy> children = new Vector<ProgressHierarchy>();
  private Vector<Float> weights = new Vector<Float>();
  LinkedList<ProgressListener> listeners = new LinkedList<ProgressListener>();
  float progress = 0.0f;
  boolean failed = false;

  public ProgressHierarchy()
  {
  }

  public ProgressHierarchy(Progress reportTo)
  {
    if (reportTo != null && reportTo instanceof ProgressHierarchy) {
      ProgressHierarchy r = (ProgressHierarchy)reportTo;
      r.addChild(this, 1f);
      return;
    }
    this.reportTo = reportTo;
  }

  public ProgressHierarchy addChild(ProgressHierarchy child, float weight)
  {
    writeLock.lock();
    try
    {
      children.add(child);
      weights.add(weight);
      child.registerListener(this);
    }
    finally
    {
      writeLock.unlock();
    }
    return child;
  }
  
  public ProgressHierarchy createChild(float weight)
  {
    return addChild(new ProgressHierarchy(), weight);
  }

  @Override
  public void complete()
  {
    progress = 100.0f;
    notifyListeners();
    if (reportTo != null)
    {
      reportTo.complete();
    }
  }

  @Override
  public void complete(String result, String kml)
  {
    progress = 100.0f;
    notifyListeners();
    if (reportTo != null)
    {
      reportTo.complete(result, kml);
    }
  }

  @Override
  public void failed(String result)
  {
    failed = true;
    notifyListeners();
    if (reportTo != null)
    {
      reportTo.failed(result);
    }
  }

  @Override
  public float get()
  {
    readLock.lock();
    try
    {
      if (children.size() != 0)
      {
        float totalWeight = 0.0f;
        progress = 0.0f;
        for (int i = 0; i < children.size(); i++)
        {
          if (weights.size() > i && children.size() > i)
          {
            totalWeight += weights.get(i);
            progress += children.get(i).get() * weights.get(i);
          }
          else
          {
            // Should never get here!
            log.error("Children has " + children.size() + " elements, but weights has "
                + weights.size() + " and ProgressHierarchy is trying to access element " + i);
          }
        }
        progress /= totalWeight;
      }
    }
    finally
    {
      readLock.unlock();
    }
    return progress;
  }

  @Override
  public boolean isFailed()
  {
    if (reportTo != null)
    {
      return reportTo.isFailed();
    }
    return failed;
  }

  public final ProgressHierarchy getChild(int i)
  {
    readLock.lock();
    try
    {
      return children.get(i);
    }
    finally
    {
      readLock.unlock();
    }
  }

  @Override
  public synchronized String getResult()
  {
    if (reportTo != null)
    {
      return reportTo.getResult();
    }
    return "";
  }

  @Override
  public synchronized void set(float progress)
  {
    this.progress = progress;
    notifyListeners();
    if (reportTo != null)
    {
      reportTo.set(progress);
    }
  }

  @Override
  public synchronized void starting()
  {
  }

  @Override
  public synchronized void change()
  {
    set(get());
  }

  public void notifyListeners()
  {
    for (ProgressListener pl : listeners)
    {
      pl.change();
    }
  }

  public synchronized void registerListener(ProgressListener l)
  {
    listeners.add(l);
  }

  @Override
  public void cancelled()
  {
    notifyListeners();
    if (reportTo != null)
    {
      reportTo.cancelled();
    }    
  }

}
