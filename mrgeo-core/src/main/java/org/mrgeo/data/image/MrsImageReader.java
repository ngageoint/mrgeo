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

package org.mrgeo.data.image;

import org.mrgeo.data.KVIterator;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.LongRectangle;

import java.awt.image.Raster;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;


public abstract class MrsImageReader
{

  /**
   * Recursively find Classes from a given directory
   *
   * @param directory
   *          directory to look at
   * @param packageName
   *          package to look into
   * @return list of classes in the package
   * @throws ClassNotFoundException
   */
  protected static List<Class<?>> findClasses(final File directory, final String packageName)
    throws ClassNotFoundException
  {
    final List<Class<?>> classes = new ArrayList<Class<?>>();
    if (!directory.exists())
    {
      return classes;
    }

    final File[] files = directory.listFiles();
    for (final File file : files)
    {
      if (file.isDirectory())
      {
        assert !file.getName().contains(".");
        classes.addAll(findClasses(file, packageName + "." + file.getName()));

      }
      else if (file.getName().endsWith(".class"))
      {
        classes.add(Class.forName(packageName + '.' +
          file.getName().substring(0, file.getName().length() - 6)));
      }
    }

    return classes;
  } // end findClasses

  /**
   * This will pull in a list of classes under the package name given.
   *
   * @param packageName
   *          where to look in the java space
   * @return an array of classes found
   * @throws IOException
   * @throws ClassNotFoundException
   */
  protected static Class<?>[] getClasses(final String packageName) throws IOException,
    ClassNotFoundException
  {

    // list all the classes in org.mrgeo.core.mrsimage.reader
    // ClassLoader cl = ClassLoader.getSystemClassLoader();
    final ClassLoader cl = Thread.currentThread().getContextClassLoader();

    final String pkgP = packageName.replace('.', '/');

    // get the list of items in the package space
    final Enumeration<URL> resources = cl.getResources(pkgP);

    // go through the resources and add to the list of classes
    final List<File> dirs = new ArrayList<File>();
    while (resources.hasMoreElements())
    {
      final URL resource = resources.nextElement();
      dirs.add(new File(resource.getFile()));
    }

    final ArrayList<Class<?>> classes = new ArrayList<Class<?>>();
    for (final File directory : dirs)
    {
      classes.addAll(findClasses(directory, packageName));
    }

    return classes.toArray(new Class[classes.size()]);
  } // end getReader

  /**
   * Retrieve an tile from the data
   * 
   * @param key
   *          item to retrieve
   * @return the result of the query
   */
  public abstract Raster get(TileIdWritable key);

  /**
   * Need to know the zoom level of the data being used
   * 
   * @return the zoom level
   */
  public abstract int getZoomlevel(); // gets the proper zoom level for this image...
  public abstract int getTileSize();

  /**
   * All readers need to close off connections out to data
   */
  public abstract void close();

  public abstract long calculateTileCount();

  /**
   * Varify if an item exists in the data.
   * 
   * @param key
   *          item to find
   * @return result of search
   */
  public abstract boolean exists(TileIdWritable key);

  public abstract KVIterator<TileIdWritable, Raster> get();

  public abstract KVIterator<TileIdWritable, Raster> get(final LongRectangle tileBounds);

  public abstract KVIterator<Bounds, Raster> get(final Bounds bounds);

  /**
   * Need to be able to pull a series of items from the data store
   * 
   * @param startKey
   *          where to start
   * @param endKey
   *          where to end (inclusive)
   * @return an Iterator through the data
   */
  public abstract KVIterator<TileIdWritable, Raster> get(TileIdWritable startKey,
    TileIdWritable endKey);

  /**
   * Return true if this reader can be cached by the caller. Implementors should
   * return false if this reader requires a resource that is limited, like
   * a connection to a backend data source.
   *
   * @return
   */
  public abstract boolean canBeCached();
}
