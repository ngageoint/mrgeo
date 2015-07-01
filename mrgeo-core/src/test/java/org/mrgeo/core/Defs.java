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

package org.mrgeo.core;

import org.apache.hadoop.fs.Path;
import org.mrgeo.hdfs.utils.HadoopFileUtils;

/**
 * @author jason.surratt
 * 
 */
public class Defs
{
  public final static String CWD = "file://" + System.getProperty("user.dir");
  
  public final static String OUTPUT = "testFiles/output/";
  public final static String OUTPUT_HDFS = new Path("/mrgeo/test-files/output/").
      makeQualified(HadoopFileUtils.getFileSystem()).toString();
  
  public final static String INPUT = "testFiles/";
  public final static String INPUT_HDFS = new Path("/mrgeo/test-files/").
      makeQualified(HadoopFileUtils.getFileSystem()).toString();
}
