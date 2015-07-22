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

package org.mrgeo.rasterops;

import org.apache.hadoop.conf.Configuration;
import org.mrgeo.utils.ClassLoaderUtil;
import org.mrgeo.utils.LoggingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import javax.media.jai.JAI;
import javax.media.jai.OperationDescriptorImpl;
import javax.media.jai.OperationRegistry;
import javax.media.jai.registry.RIFRegistry;
import javax.vecmath.Vector3d;
import java.awt.image.renderable.RenderedImageFactory;
import java.io.IOException;
import java.net.URL;
import java.util.List;

/**
 * @author jason.surratt
 * 
 */
public class OpImageRegistrar
{
  private static final Logger log = LoggerFactory.getLogger(OpImageRegistrar.class);
  static boolean done = false;

  static
  {
    // lower some log levels.
    LoggingUtils.setLogLevel(Configuration.class, LoggingUtils.WARN);
    LoggingUtils.setLogLevel("org.apache.hadoop.io.compress.CodecPool", LoggingUtils.WARN);
    LoggingUtils.setLogLevel("org.apache.hadoop.hdfs.DFSClient", LoggingUtils.ERROR);
  }

  public static synchronized void registerMrGeoOps()
  {
    if (done)
    {
      return;
    }
    log.info("Registering image ops.");

    ImageIO.scanForPlugins();

    // increase the tile cache size to speed things up, 256MB
    long memCapacity = 268435456;
    if (JAI.getDefaultInstance().getTileCache().getMemoryCapacity() < memCapacity)
    {
      JAI.getDefaultInstance().getTileCache().setMemoryCapacity(memCapacity);
    }

    try
    {
      @SuppressWarnings("unused")
      Vector3d v = new Vector3d();
    }
    catch (Throwable e)
    {
      System.out.println("Vector3d class path error encountered.");
    }

    try
    {
      registerResources("org/mrgeo/rasterops");
      registerResources("org/mrgeo/opimage");
      registerResources("org/mrgeo/opimage/geographickernel");
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    catch (ClassNotFoundException e)
    {
      e.printStackTrace();
    }
    catch (InstantiationException e)
    {
      e.printStackTrace();
    }
    catch (IllegalAccessException e)
    {
      e.printStackTrace();
    }

    done = true;
  }

  static void registerResources(String parent) throws IOException, ClassNotFoundException,
      InstantiationException, IllegalAccessException
  {
    OperationRegistry registry = JAI.getDefaultInstance().getOperationRegistry();

    //ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    List<URL> urls = ClassLoaderUtil.getChildResources(parent);
    for (URL u : urls)
    {
      try
      {
        // TODO There is probably a way to load this based on the URL instead of name
        // mangling.
        String className = (parent.replace("/", ".")) + "."
            + u.toString().replaceAll(".*\\/", "").replaceAll("\\.class", "");
  
        Class<?> c = Class.forName(className);
  
        if (c.getSuperclass() == OperationDescriptorImpl.class)
        {
          // check to see if the operation is already registered. This avoids
          // problems with a single jar in the path multiple times.
          if (registry.getOperationDescriptor(className) == null)
          {
            registry.registerDescriptor((OperationDescriptorImpl) c.newInstance());
          }
        }
      }
      catch (NoClassDefFoundError e)
      {
        //no op
      }
      catch (ClassNotFoundException e)
      {
        // pass
      }
    }

    for (URL u : urls)
    {
      try
      {
        String className = (parent.replace("/", ".")) + "."
            + u.toString().replaceAll(".*\\/", "").replaceAll("\\.class", "");
  
        Class<?> c = Class.forName(className);
        Class<?>[] interfaces = c.getInterfaces();
        for (Class<?> i : interfaces)
        {
          if (i == RenderedImageFactory.class)
          {
            RIFRegistry.register(registry, className, "MrGeo", (RenderedImageFactory) c
                .newInstance());
          }
        }
      }
      catch (NoClassDefFoundError e)
      {
        //no op
      }
      catch (ClassNotFoundException e)
      {
        // pass
      }
    }
  }
}
