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

package org.mrgeo.test;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.mapalgebra.MapAlgebra;
import org.mrgeo.mapalgebra.parser.ParserException;
import org.mrgeo.job.JobCancelledException;
import org.mrgeo.job.JobFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class MapOpTestVectorUtils extends TestUtils
{
private static final Logger log = LoggerFactory.getLogger(MapOpTestVectorUtils.class);

public MapOpTestVectorUtils(final Class testClass) throws IOException
{
  super(testClass);
}


public void generateBaselineVector(final Configuration conf, final String testName,
    final String ex)
    throws IOException, ParserException, JobFailedException, JobCancelledException
{
  runMapAlgebraExpression(conf, testName, ex);

  final Path src = new Path(outputHdfs, testName);
  final FileSystem srcfs = src.getFileSystem(conf);
  if (srcfs.exists(src))
  {
    final Path dst = new Path(inputLocal, testName);
    final FileSystem fs = dst.getFileSystem(conf);
    fs.copyToLocalFile(src, dst);
  }
}



//public void runVectorExpression(final Configuration conf, final String testName, final String ex)
//    throws IOException, ParserException, JobFailedException, JobCancelledException
//{
//  runMapAlgebraExpression(conf, testName, ex);
//
//
//  // read in the output file
//  Path srcVector = new Path(new Path(outputHdfs, testName), "part*");
//  final FileSystem fs = HadoopFileUtils.getFileSystem(conf, srcVector);
//
//  FileStatus files[] = fs.globStatus(srcVector);
//  if (files.length > 0)
//  {
//    srcVector = files[0].getPath();
//  }
//  else
//  {
//    Assert.fail("Output file missing: " + srcVector.toString());
//  }
//
//  //    if (fs.exists(p) == false)
//  //    {
//  //      p = new Path(new Path(outputHdfs, testName), "part*");
//  //    }
//  final long l = fs.getFileStatus(srcVector).getLen();
//  final byte[] testBuffer = new byte[(int) l];
//  final FSDataInputStream fdis = fs.open(srcVector);
//  fdis.read(testBuffer);
//  fdis.close();
//
//  File baselineVector = new File(inputLocal + testName);
//
//  if (!baselineVector.exists() || baselineVector.isDirectory())
//  {
//    FileFilter fileFilter = new WildcardFileFilter("part*");
//    File[] f = baselineVector.listFiles(fileFilter);
//    if (f.length > 0)
//    {
//      baselineVector = f[0].getCanonicalFile();
//    }
//    else
//    {
//      Assert.fail("Golden test file missing: " + baselineVector.toString());
//    }
//  }
//
//  // read in the baseline
//  final byte[] baselineBuffer = new byte[(int) baselineVector.length()];
//
//  final FileInputStream fis = new FileInputStream(baselineVector);
//  fis.read(baselineBuffer);
//  fis.close();
//
//  //String[] baseline = (new String(baselineBuffer)).split("\n");
//
//  Assert.assertEquals("Output is different!", new String(baselineBuffer), new String(testBuffer));
//  Assert.assertEquals(true, fs.exists(new Path(outputHdfs, testName + ".columns")));
//}

public List readVectorOutputAsText(final Configuration conf,
    final Path vectorPath) throws IOException
{
  // read in the output file
  final FileSystem fs = HadoopFileUtils.getFileSystem(conf, vectorPath);

  ArrayList results = new ArrayList();
  if (fs.isFile(vectorPath))
  {
    final FSDataInputStream fdis = fs.open(vectorPath);
    final BufferedReader br = new BufferedReader(new InputStreamReader(fdis));
    try
    {
      String line = br.readLine();
      while (line != null)
      {
        results.add(line);
        line = br.readLine();
      }
    }
    finally
    {
      br.close();
      if (fdis != null)
      {
        fdis.close();
      }
    }
  }
  else
  {
    Path srcVector = new Path(vectorPath, "part*");
    FileStatus files[] = fs.globStatus(srcVector);
    for (FileStatus fileStat : files)
    {
      final FSDataInputStream fdis = fs.open(fileStat.getPath());
      final BufferedReader br = new BufferedReader(new InputStreamReader(fdis));
      try
      {
        String line = br.readLine();
        while (line != null)
        {
          results.add(line);
          line = br.readLine();
        }
      }
      finally
      {
        br.close();
        if (fdis != null)
        {
          fdis.close();
        }
      }
    }
  }
  return results;
}


/**
 * Runs the map algebra expression and stores the results to outputHdfs in  a
 * subdirectory that matches the testName. No comparison against expected
 * output is done. See other methods in this class like runVectorExpression and
 * runRasterExpression for that capability.
 *
 * @param conf
 * @param testName
 * @param ex
 * @return
 * @throws java.io.IOException
 * @throws org.mrgeo.mapreduce.job.JobFailedException
 * @throws org.mrgeo.mapreduce.job.JobCancelledException
 * @throws ParserException
 */
public void runMapAlgebraExpression(final Configuration conf, final String testName,
    final String ex)
    throws IOException, JobFailedException, JobCancelledException, ParserException
{

  Path output = new Path(outputHdfs, testName);
  HadoopFileUtils.delete(output);

  log.info(ex);

  MapAlgebra.mapalgebra(ex, output.toString(), conf, ProviderProperties.fromDelimitedString(""), null);
}


public void compareVectors(Configuration conf, String testName) throws IOException
{

  Path output = new Path(outputHdfs, testName);
  FileSystem fs = HadoopFileUtils.getFileSystem(conf, output);

  Path[] srcFiles;
  if (fs.isDirectory(output))
  {
    FileStatus[] files = fs.listStatus(output);
    if (files == null || files.length == 0)
    {
      Assert.fail("No files founds: " + output.toString());
    }
    srcFiles = new Path[files.length];

    int cnt = 0;
    for (FileStatus file: files)
    {
      srcFiles[cnt++] = file.getPath();
    }
  }
  else
  {
    srcFiles = new Path[] {output} ;
  }

  for (Path file: srcFiles)
  {
    // read in the output file
    final long l = fs.getFileStatus(file).getLen();
    final byte[] testBuffer = new byte[(int) l];
    final FSDataInputStream fdis = fs.open(file);
    fdis.read(testBuffer);
    fdis.close();

    File baselineVector = new File(inputLocal + testName + "/" + file.getName());

    if (!baselineVector.exists())
    {
      Assert.fail("Golden test file missing: " + baselineVector.toString());
    }

    // read in the baseline
    final byte[] baselineBuffer = new byte[(int) baselineVector.length()];

    final FileInputStream fis = new FileInputStream(baselineVector);
    fis.read(baselineBuffer);
    fis.close();

    Assert.assertEquals("Output is different!", new String(baselineBuffer), new String(testBuffer));
  }

}
}
