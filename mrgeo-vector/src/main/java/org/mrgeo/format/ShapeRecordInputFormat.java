package org.mrgeo.format;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.mrgeo.geometry.WellKnownProjections;
import org.mrgeo.geometryfilter.ReprojectedGeometryCollection;
import org.mrgeo.data.GeometryCollection;
import org.mrgeo.data.shp.ShapefileReader;
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
      GeometryCollection geometryCollection = new ShapefileReader(path);
      //System.out.println(WellKnownProjections.WGS84);
      //System.out.println(geometryCollection.getProjection());
      if (!geometryCollection.getProjection().contains("WGS 84") && 
          !geometryCollection.getProjection().contains("GCS_WGS_1984"))
      {
        geometryCollection = 
          new ReprojectedGeometryCollection(geometryCollection, WellKnownProjections.WGS84);
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
        splits.add(new GeometryInputSplit(geometryCollection, begin, end));
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
