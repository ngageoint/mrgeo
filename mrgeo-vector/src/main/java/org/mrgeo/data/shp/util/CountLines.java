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

package org.mrgeo.data.shp.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class CountLines extends java.lang.Object
{
  public static void main(String[] args)
  {
    if (args.length == 0)
    {
      // usage
      System.out.println("USAGE: CountLines {-r} {-v} <directory>");
      System.exit(0);
    }
    // create object
    CountLines cl = new CountLines();
    cl.count(args);
  }

  private boolean firstpass;
  private int n_files;
  private int n_lines;
  private boolean recurse;

  private boolean verbose;

  public synchronized int count(String[] args)
  {
    try
    {
      String dir = null;
      if (args == null)
      {
        return 0;
      }
      // init defaults
      recurse = false;
      verbose = false;
      // check args
      for (int i = 0; i < args.length; i++)
      {
        if (args[i].equalsIgnoreCase("-r"))
        {
          recurse = true;
        }
        else if (args[i].equalsIgnoreCase("-v"))
        {
          verbose = true;
        }
        else if (args[i].equalsIgnoreCase("-vr"))
        {
          verbose = true;
          recurse = true;
        }
        else if (args[i].equalsIgnoreCase("-rv"))
        {
          recurse = true;
          verbose = true;
        }
        else
        {
          dir = args[i];
        }
      }
      // last check
      if (dir == null)
        return 0;

      // check root dir
      File file = new File(dir);
      if (!file.exists())
        throw new Exception("Directory '" + dir + "' does not exist!");
      // reset
      n_files = 0;
      n_lines = 0;
      firstpass = true;
      // go!
      StopWatch t = new StopWatch();
      t.start();
      traverse(file);
      t.stop();
      // report
      System.out.println("--- Complete ---");
      System.out.println("Exec. Time (ms): " + t.elapsed());
      System.out.println("# Lines: " + n_lines);
      System.out.println("# Files: " + n_files);
      System.out.println("Avg. Lines/File: " + ((n_files > 0) ? "~" + n_lines / n_files : "" + 0));
      // return
      return n_lines;
    }
    catch (Exception e)
    {
      e.printStackTrace();
      return -1;
    }
  }

  private int process(File f)
  {
    // verbose
    if (verbose)
      System.out.print("Processing File [" + f.getPath() + "]...");
    // init
    int count = 0;
    BufferedReader in = null;
    // count lines
    try
    {
      in = new BufferedReader(new FileReader(f.getPath()));
      while ((in.readLine()) != null)
      {
        count++;
      }
      in.close();
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    finally
    {
      in = null;
    }
    // return count
    if (verbose)
      System.out.println(count);
    return count;
  }

  private void traverse(File f)
  {
    // process
    if (f.isFile())
    {
      // filter-in desired file types
      if (f.getName().endsWith(".java"))
      {
        n_lines += process(f);
        n_files++;
      }
      else if (f.getName().endsWith(".jsp"))
      {
        n_lines += process(f);
        n_files++;
      }
      else if (f.getName().endsWith(".html"))
      {
        n_lines += process(f);
        n_files++;
      }
      else if (f.getName().endsWith(".htm"))
      {
        n_lines += process(f);
        n_files++;
      }
      else if (f.getName().endsWith(".xml"))
      {
        n_lines += process(f);
        n_files++;
      }
      else if (f.getName().endsWith(".txt"))
      {
        n_lines += process(f);
        n_files++;
      }
      else if (f.getName().endsWith(".css"))
      {
        n_lines += process(f);
        n_files++;
      }
    }
    else
    {
      // filter-out undesired directories
      if (f.getPath().endsWith("CVS"))
        return;
      // proceed
      if (verbose)
        System.out.println("Traversing Directory [" + f.getPath() + "]...");
      // traverse children recursively
      if (recurse || firstpass)
      {
        firstpass = false;
        String[] children = f.list();
        for (int i = 0; i < children.length; i++)
        {
          traverse(new File(f, children[i]));
        }
      }
    }
  }
}
