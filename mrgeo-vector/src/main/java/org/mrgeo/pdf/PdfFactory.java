/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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

package org.mrgeo.pdf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.mrgeo.hdfs.utils.HadoopFileUtils;

import java.io.DataInput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Vector;

public class PdfFactory
{
  public static PdfCurve loadPdf(Path pdfPath, Configuration conf)
      throws IOException, ClassNotFoundException, SecurityException, NoSuchMethodException,
      IllegalArgumentException, IllegalAccessException, InvocationTargetException
  {
    FileSystem fs = HadoopFileUtils.getFileSystem(conf, pdfPath);
    FileStatus[] children = fs.listStatus(pdfPath);
    Path[] childPaths = new Path[children.length];
    int index = 0;
    for (FileStatus child : children)
    {
      childPaths[index++] = child.getPath();
    }
    return _loadPdf(childPaths, pdfPath, conf);
  }

//  public static PdfCurve loadPdfFromDistributedCache(Configuration conf)
//      throws IOException, ClassNotFoundException, SecurityException, NoSuchMethodException,
//      IllegalArgumentException, IllegalAccessException, InvocationTargetException
//  {
//    Path[] files = DistributedCache.getLocalCacheFiles(conf);
//    return _loadPdf(files, conf);
//  }

  private static PdfCurve _loadPdf(Path[] files, Path pdfPath, Configuration conf)
      throws IOException, ClassNotFoundException, SecurityException, NoSuchMethodException,
      IllegalArgumentException, IllegalAccessException, InvocationTargetException
  {
    Vector<Path> pdfFiles = new Vector<Path>();
    Path metadataPath = null;
    for (Path f : files)
    {
      String name = f.getName();
      if (name.equals("metadata"))
      {
        metadataPath = f;
      }
      else if (name.startsWith("part"))
      {
        pdfFiles.add(f);
      }
    }
    if (metadataPath == null)
    {
      throw new IOException("Cannot load PDF " + pdfPath.toString() + ". Missing metadata file.");
    }

    FSDataInputStream is = null;
    try
    {
      FileSystem dfs = metadataPath.getFileSystem(conf);
      is = dfs.open(metadataPath);
      String className = is.readUTF();
      Class<?> c = Class.forName(className);
      Path[] passPdfFiles = new Path[pdfFiles.size()];
      pdfFiles.toArray(passPdfFiles);
      Method m = c.getMethod("load", DataInput.class, Path[].class, Configuration.class);
      Object result = m.invoke(null, is, passPdfFiles, conf);
      if (result instanceof PdfCurve)
      {
        return (PdfCurve)result;
      }
      throw new IllegalArgumentException("PdfFactory expected an instance of PdfCurve, not: " + result.getClass().getName());
    }
    finally
    {
      IOUtils.closeStream(is);
    }
  }
}
