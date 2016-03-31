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

package org.mrgeo.mapalgebra.vector;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.HadoopVectorUtils;

import java.io.IOException;
import java.util.Map;

/**
 * @author jason.surratt
 * 
 */
@SuppressWarnings("static-method")
public class PgQueryInputFormatTest 
{    

  private static String input;

  @BeforeClass
  public static void init()
  {
    input = TestUtils.composeInputDir(PgQueryInputFormatTest.class);
  }

  public void equals(double v1, double v2)
  {
    Assert.assertEquals(v1, v2, Math.max(Math.abs(v1 * 1e-5), 1e-5));
  }

  public RecordReader<LongWritable, Geometry> openReader(Path p) throws IOException
  {
    FileSystem fs = new RawLocalFileSystem();
    try
    {
      Job j = new Job(new Configuration());
      Configuration c = j.getConfiguration();
      fs.setConf(c);
      Path testFile = fs.makeQualified(p);

      c.set("mapred.input.dir", testFile.toString());
      PgQueryInputFormat format = new PgQueryInputFormat();
      HadoopVectorUtils.setupPgQueryInputFormat(j, "anthro", "anthro4server", "jdbc:postgresql://localhost:5432/anthro");
      InputSplit split = null;
      try
      {
        split = format.getSplits(j).get(0);
        return format.createRecordReader(split, HadoopUtils.createTaskAttemptContext(c, new TaskAttemptID()));
      }
      catch (Exception e)
      {
        return null;
      }
    }
    finally
    {
      fs.close();
    }
  }

  @Before public void setUp()
  {
  }

  @Test 
  @Category(UnitTest.class) 
  public void testComplexPolygonRead() throws Exception
  {

    RecordReader<LongWritable, Geometry> r = openReader(new Path(input, "sql_test.sql"));

    if (r != null)
    {
      String[] base = {
        "value:10.0 GEOMETRY:POLYGON ((48.513333329928 14.795000002082, 48.513333329928 14.794166668749, 48.519999996592 14.794166668749, 48.519999996592 14.795000002082, 48.522499996591 14.795000002082, 48.522499996591 14.794166668749, 48.524166663257 14.794166668749, 48.524166663257 14.795833335415, 48.52499999659 14.795833335415, 48.52499999659 14.796666668748, 48.524166663257 14.796666668748, 48.524166663257 14.799166668747, 48.523333329924 14.799166668747, 48.523333329924 14.80000000208, 48.522499996591 14.80000000208, 48.522499996591 14.803333335412, 48.520833329925 14.803333335412, 48.520833329925 14.805000002078, 48.518333329926 14.805000002078, 48.518333329926 14.805833335411, 48.517499996593 14.805833335411, 48.517499996593 14.806666668744, 48.515833329927 14.806666668744, 48.515833329927 14.807500002077, 48.514999996594 14.807500002077, 48.514999996594 14.810000002076, 48.514166663261 14.810000002076, 48.514166663261 14.810833335409, 48.513333329928 14.810833335409, 48.513333329928 14.811666668742, 48.503333329932 14.811666668742, 48.503333329932 14.810833335409, 48.495833329935 14.810833335409, 48.495833329935 14.80833333541, 48.496666663268 14.80833333541, 48.496666663268 14.807500002077, 48.498333329934 14.807500002077, 48.498333329934 14.806666668744, 48.500833329933 14.806666668744, 48.500833329933 14.805833335411, 48.502499996599 14.805833335411, 48.502499996599 14.805000002078, 48.504166663265 14.805000002078, 48.504166663265 14.804166668745, 48.504999996598 14.804166668745, 48.504999996598 14.801666668746, 48.505833329931 14.801666668746, 48.505833329931 14.799166668747, 48.506666663264 14.799166668747, 48.506666663264 14.798333335414, 48.509166663263 14.798333335414, 48.509166663263 14.797500002081, 48.509999996596 14.797500002081, 48.509999996596 14.798333335414, 48.511666663262 14.798333335414, 48.511666663262 14.796666668748, 48.512499996595 14.796666668748, 48.512499996595 14.795000002082, 48.513333329928 14.795000002082)) ",
      "value:100.0 GEOMETRY:POLYGON ((48.586666663232 14.7500000021, 48.586666663232 14.750833335433, 48.589999996564 14.750833335433, 48.589999996564 14.754166668765, 48.589166663231 14.754166668765, 48.589166663231 14.757500002097, 48.588333329898 14.757500002097, 48.588333329898 14.75833333543, 48.587499996565 14.75833333543, 48.587499996565 14.759166668763, 48.585833329899 14.759166668763, 48.585833329899 14.75833333543, 48.582499996567 14.75833333543, 48.582499996567 14.757500002097, 48.581666663234 14.757500002097, 48.581666663234 14.755000002098, 48.582499996567 14.755000002098, 48.582499996567 14.754166668765, 48.5833333299 14.754166668765, 48.5833333299 14.752500002099, 48.584166663233 14.752500002099, 48.584166663233 14.751666668766, 48.584999996566 14.751666668766, 48.584999996566 14.750833335433, 48.585833329899 14.750833335433, 48.585833329899 14.7500000021, 48.586666663232 14.7500000021)) " };

      int index = 0;
      while (r.nextKeyValue())
      {
        Geometry f = r.getCurrentValue();
        String row = "";
        for (Map.Entry attr : f.getAllAttributes().entrySet())
        {
          row += attr.getKey() + ":" + attr.getValue() + " ";
        }
        Assert.assertEquals(base[index++], row);
      }
    }
  }
}
