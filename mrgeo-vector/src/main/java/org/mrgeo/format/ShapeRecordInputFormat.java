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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.mrgeo.hdfs.vector.ReprojectedShapefileGeometryCollection;
import org.mrgeo.hdfs.vector.ShapefileGeometryCollection;
import org.mrgeo.data.shp.ShapefileReader;
import org.mrgeo.utils.GDALUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class ShapeRecordInputFormat extends ShpInputFormat implements RecordInputFormat
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(ShapeRecordInputFormat.class);

  private int recordsPerSplit = 100;
  public long getRecordsPerSplit() { return recordsPerSplit; }
  @Override
  public void setRecordsPerSplit(long numRecords) { recordsPerSplit = (int)numRecords; }
  
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
  { 
    List<InputSplit> splits = new LinkedList<InputSplit>();
    Path[] paths = FileInputFormat.getInputPaths(context);
    if (paths.length > 1)
    {
      //originally planned on supporting multiple shape files within a directory but holding
      //off on implementing that for now
      throw new IOException("Only one input shape file allowed per job.");
    }
    
    for (Path path : paths)
    {
      ShapefileGeometryCollection geometryCollection = new ShapefileReader(path);
      //System.out.println(WellKnownProjections.WGS84);
      //System.out.println(geometryCollection.getProjection());
      if (!geometryCollection.getProjection().contains("WGS 84") && 
          !geometryCollection.getProjection().contains("GCS_WGS_1984"))
      {
        geometryCollection = 
          new ReprojectedShapefileGeometryCollection(geometryCollection, GDALUtils.EPSG4326);
      }
      int size = geometryCollection.size();
      int begin = 0;
      int end = -1;
      if (size < recordsPerSplit)
      {
        end = size;
      }
      else
      {
        end = recordsPerSplit;
      }
      while (end <= size)
      {
        splits.add(new GeometryInputSplit(begin, end));
        begin = end;
        if ((size - end) < recordsPerSplit && (size - end) > 0)
        {
          end += (size - end);
        }
        else
        {
          end += recordsPerSplit;
        }
      }
    }
    
    return splits;
  }
  
  @Override
  public long getRecordCount(JobContext context) throws IOException
  {
    Path[] paths = FileInputFormat.getInputPaths(context);
    if (paths.length > 1)
    {
      //originally planned on supporting multiple shape files within a directory but holding
      //off on implementing that for now
      throw new IOException("Only one input shape file allowed per job.");
    }
    long recordCtr = 0;
    for (Path path : paths)
    {
      ShapefileReader shapefileReader = new ShapefileReader(path);
      recordCtr += shapefileReader.size();
      shapefileReader.close();
    }
    return recordCtr;
  }
}
