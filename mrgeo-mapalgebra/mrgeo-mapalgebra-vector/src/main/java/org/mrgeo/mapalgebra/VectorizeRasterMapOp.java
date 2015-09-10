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

package org.mrgeo.mapalgebra;

import java.io.IOException;
import java.util.Vector;

import org.apache.commons.lang3.NotImplementedException;
import org.mrgeo.mapalgebra.old.MapOpHadoop;
import org.mrgeo.mapalgebra.old.ParserAdapterHadoop;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.progress.Progress;

public class VectorizeRasterMapOp extends VectorMapOp
{
//  private static final Logger _log = LoggerFactory.getLogger(VectorizeRasterMapOp.class);
//  private String _outputType = "shp";
//  private double _threshold = 0.0;
 
  @Override
  public void addInput(MapOpHadoop n) throws IllegalArgumentException
  {
//    if (_inputs.size() == 0)
//    {
//      if (n.getOutputType() != DataType.Raster)
//      {
//        throw new IllegalArgumentException("The input parameter must be a raster input.");
//      }
//      _inputs.add(n);
//    }
//    else
//    {
//      throw new IllegalArgumentException("Only one input is supported.");
//    }
  }

//  protected MrsPyramidv1 _flushImageOutput(MapOp op, ProgressHierarchy ph)
//      throws IOException, JobFailedException, JobCancelledException
//  {
//    MrsPyramidv1 result;
//
//    // if our source is a file, then use it directly.
//    if (op.getOutputPath() != null)
//    {
//      result = MrsPyramidv1.loadPyramid(op.getOutputPath());
//    }
//    // if the source isn't a file
//    else
//    {
//      // make a copy of our source inputs raster
//      RenderedImage sourceRaster = (RenderedImage) _inputs.get(0).getOutput();
//      
//      MrsPyramidv1 sourcePyramid = createEmptyPyramid(_inputs.get(0));
//
//      OpChainToolv1 opChainTool = new OpChainToolv1();
//      Configuration conf = new Configuration(getConf());
//      opChainTool.run(sourceRaster, sourcePyramid.getImage(0), sourcePyramid.getBounds(), conf, ph, jobListener);
//      _outputName = sourcePyramid.getPath();
//      sourcePyramid.reload();
//      result = sourcePyramid;
//    }
//    ph.complete();
//
//    return result;
//  }
  
  @Override
  public void build(Progress p) throws IOException, JobFailedException, JobCancelledException
  {
//    ProgressHierarchy ph = new ProgressHierarchy(p);
//    ph.createChild(1.0f);
//    ph.createChild(1.0f);
//    
//    MrsPyramidv1 image = _flushImageOutput(_inputs.get(0), ph.getChild(0));
//   
//    Path inputPath = null;
//    if (_outputName != null)
//    {
//      inputPath = new Path(_outputName);
//    }
//    
//    FileSystem fs = HadoopFileUtils.getFileSystem(_outputName);
//    VectorizeRasterTool tool = new VectorizeRasterTool();
//    tool.setOutputType(_outputType);
//    Configuration vrtConf = new Configuration(getConf());
//    tool.run(image, _outputName, _threshold, vrtConf, p, jobListener);
//    
//    _output = new BasicInputFormatDescriptor(_outputName);
//    if (inputPath != null)
//    {
//      fs.delete(inputPath, true);
//    }
  }

  @Override
  public void moveOutput(String toName) throws IOException
  {
//    super.moveOutput(toName);
//    _outputName = toName;
//    _output = new BasicInputFormatDescriptor(_outputName);
  }

  @Override
  public Vector<ParserNode> processChildren(final Vector<ParserNode> children, final ParserAdapterHadoop parser)
  {
    throw new NotImplementedException("VectorizeRasterMapOp not implemented for MrsImagePyramid v2");

//    Vector<ParserNode> result = new Vector<ParserNode>();
//
//    if (children.size() > 3)
//    {
//      throw new IllegalArgumentException(
//          "VectorizeRasterMapOp takes one or two or three arguments. (source raster or output type or threshold)");
//    }
//
//    result.add(children.get(0));
//    if (children.size() > 2)
//    {
//        _outputType =parseChildString(children.get(1), "output type", parser);
//    }
//    if (children.size() == 3)
//    {        
//      _threshold = parseChildDouble(children.get(2), "threshold", parser);
//    }
//    
//    return result;
  }
  
  @Override
  public String toString()
  {
    return String.format("VectorizeRasterMapOp %s",
       _outputName == null ? "null" : _outputName );
  }
}
