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

package org.mrgeo.format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DirectoryInputFormat extends InputFormat<Text, Text>
{
  @Override
  public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException
  {
    return new SubdirRecordReader();
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
  {
    Path parent = getParentDirectory(context);
    final String[] subdirs = getSubdirNames(context);
    // generate splits
    List<InputSplit> splits = new ArrayList<InputSplit>();
    FileSystem fs = parent.getFileSystem(context.getConfiguration());
    // Only look at subdirs requested by the caller (if specified)
    PathFilter pathFilter = null;
    if (subdirs != null && subdirs.length > 0)
    {
      pathFilter = new PathFilter() {
        @Override
        public boolean accept(Path p)
        {
          String name = p.getName();
          for (String subdir : subdirs)
          {
            if (subdir.equals(name))
            {
              return true;
            }
          }
          return false;
        }
      };
    }
    FileStatus[] children = (pathFilter == null) ? fs.listStatus(parent) : fs.listStatus(parent, pathFilter);
    for (FileStatus child : children)
    {
      // Return each subdirectory as a separate split. This means that entire subdirs will
      // be processed by a single mapper.
      if (child.isDir())
      {
//        BlockLocation[] blkLocations = fs.getFileBlockLocations(child, 0L, child.getLen());
//        splits.add(new DirectorySplit(child.getPath(), (blkLocations.length == 0) ? new String[0] : blkLocations[0].getHosts()));
        splits.add(new DirectorySplit(child.getPath(), new String[0]));
      }
    }
    return splits;
  }

  public static void setParentDirectory(Job job, Path parent) throws IOException
  {
    Configuration conf = job.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    Path path = parent.makeQualified(fs);
    conf.set("mapred.input.dir", StringUtils.escapeString(path.toString()));
  }

  public static Path getParentDirectory(JobContext context)
  {
    String parent = context.getConfiguration().get("mapred.input.dir", "");
    return new Path(StringUtils.unEscapeString(parent));
  }
  
  public static void setSubdirNames(Job job, String[] subdirNames)
  {
    if (subdirNames != null && subdirNames.length > 0)
    {
      Configuration conf = job.getConfiguration();
      StringBuffer str = new StringBuffer();
      str.append(subdirNames[0]);
      for (int ii=1; ii < subdirNames.length; ii++)
      {
        str.append(",");
        str.append(subdirNames[ii]);
      }
      conf.set("subdir.names", str.toString());
    }
  }
  
  public static String[] getSubdirNames(JobContext context)
  {
    String subdirs = context.getConfiguration().get("subdir.names");
    if (subdirs != null && subdirs.length() > 0)
    {
      String[] result = subdirs.split(",");
      return result;
    }
    return null;
  }
}
