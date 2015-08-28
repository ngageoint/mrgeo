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

import com.vividsolutions.jts.geom.Envelope;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.CloseableKVIterator;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.GeometryInputStream;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.shp.ShapefileReader;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.data.vector.VectorMetadata;
import org.mrgeo.data.vector.VectorMetadataReader;
import org.mrgeo.data.vector.VectorReader;
import org.mrgeo.format.FeatureInputFormatFactory;
import org.mrgeo.format.InlineCsvInputFormat;
import org.mrgeo.format.PgQueryInputFormat;
import org.mrgeo.format.ShpInputFormat;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.WritableGeometry;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.mapalgebra.parser.ParserAdapter;
import org.mrgeo.mapalgebra.parser.ParserConstantNode;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.mapreduce.RasterizeVectorDriver;
import org.mrgeo.mapreduce.RasterizeVectorPainter;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.opimage.MrsPyramidDescriptor;
import org.mrgeo.progress.Progress;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.LatLng;
import org.mrgeo.utils.StringUtils;
import org.mrgeo.utils.TMSUtils;
import org.mrgeo.vector.mrsvector.mapalgebra.HdfsMrsVectorPyramidInputFormatDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

public class RasterizeVectorMapOp extends RasterMapOp
    implements InputsCalculator, BoundsCalculator, TileSizeCalculator, MaximumZoomLevelCalculator
{
  private static final Logger log = LoggerFactory.getLogger(RasterizeVectorMapOp.class);

  private String column = null;
  private double cellSize = -1;
  private int zoomlevel = 0;
  private int tilesize = 0;

  // we keep our own bounds, and not use MapOp.bounds as that gets overwritten
  Bounds bounds = null;

  RasterizeVectorPainter.AggregationType _aggregationType;

  @Override
  public void addInput(final MapOpHadoop n) throws IllegalArgumentException
  {
    if (!(n instanceof VectorMapOp))
    {
      throw new IllegalArgumentException("Only vector inputs are supported.");
    }
    if (_inputs.size() != 0)
    {
      throw new IllegalArgumentException("Only one input is supported.");
    }
    _inputs.add(n);
    InputFormatDescriptor ifd = ((VectorMapOp)n).getVectorOutput();
    // Until MrsVector buld pyramids is complete, we only support using the data at
    // the zoom level to which it was ingested.
    if (ifd instanceof HdfsMrsVectorPyramidInputFormatDescriptor)
    {
      HdfsMrsVectorPyramidInputFormatDescriptor vfd = (HdfsMrsVectorPyramidInputFormatDescriptor)ifd;
      if (vfd.getZoomLevel() != zoomlevel)
      {
        throw new IllegalArgumentException("Only zoom level " + vfd.getZoomLevel() +
            " is supported for MrsVector " + ((VectorMapOp)n).getOutputName());
      }
    }
  }

  @Override
  public Bounds calculateBounds() throws IOException
  {
    // cant get bounds until the output is computed
    if (_output != null)
    {
      final MrsImagePyramid mp = MrsImagePyramid.open(_outputName, getProviderProperties());
      log.info("Rasterize vector map op bounds " + mp.getBounds().toString());
      return mp.getBounds();
    }
    else if (bounds != null)
    {
      return bounds;
    }
    else
    {
      InputFormatDescriptor ifd = ((VectorMapOp)_inputs.get(0)).getVectorOutput();
      if (ifd instanceof BasicInputFormatDescriptor)
      {
        final BasicInputFormatDescriptor bfd = (BasicInputFormatDescriptor) ifd;

        VectorDataProvider dp = bfd.getVectorDataProvider();
        if (dp != null)
        {
          VectorMetadataReader metadataReader = dp.getMetadataReader();
          if (metadataReader != null)
          {
            VectorMetadata metadata = metadataReader.read();
            if (metadata != null)
            {
              Bounds b = metadata.getBounds();
              if (b != null)
              {
                bounds = b;
                return bounds;
              }
            }
          }
          // The provider does not give back bounds, so we have to run through
          // the features ourselves computing the bounds.
          VectorReader reader = dp.getVectorReader();
          if (reader != null)
          {
            CloseableKVIterator<LongWritable, Geometry> iter = reader.get();
            if (iter != null)
            {
              try
              {
                bounds = new Bounds();
                while (iter.hasNext())
                {
                  Geometry geom = iter.next();
                  if (geom != null)
                  {
                    bounds.expand(geom.getBounds());
                  }
                }
                return bounds;
              }
              finally
              {
                iter.close();
              }
            }
          }
        }
        else
        {
          // Run the old HDFS-specific code
          String input = bfd.getPath();
  
          InputFormat<LongWritable, org.mrgeo.geometry.Geometry> format =
              FeatureInputFormatFactory.getInstance().createInputFormat(input);
  
          GeometryInputStream stream = null;
          Path inputPath = new Path(input);
          // make sure to test for TSV first (it is derived from CSV)
          if (format instanceof ShpInputFormat)
          {
            stream = new ShapefileReader(inputPath);
          }
          else if (format instanceof PgQueryInputFormat)
          {
            throw new IOException("PostGIS query not supported yet.");
          }
  
          if (stream != null)
          {
            bounds = new Bounds();
            while (stream.hasNext())
            {
              WritableGeometry geom = stream.next();
              if (geom != null)
              {
                bounds.expand(geom.getBounds());
              }
            }
  
            stream.close();
  
            return bounds;
          }
        }
      }
      else if (ifd instanceof InlineCsvInputFormatDescriptor)
      {
        InlineCsvInputFormatDescriptor cfd = (InlineCsvInputFormatDescriptor) ifd;
        // Set up a reader to be able to stream features from the input source
        InlineCsvInputFormat.InlineCsvReader csvReader = new InlineCsvInputFormat.InlineCsvReader();
        csvReader.initialize(cfd._columns, cfd._values);
        bounds = new Bounds();
        while (csvReader.nextFeature())
        {
          org.mrgeo.geometry.Geometry feature = csvReader.getCurrentFeature();
          if (feature != null)
          {
            Envelope envelope = feature.toJTS().getEnvelopeInternal();
            if (envelope != null)
            {
              bounds.expand(envelope.getMinX(), envelope.getMinY(),
                  envelope.getMaxX(), envelope.getMaxY());
            }
          }
        }
        return bounds;
      }
      else if (ifd instanceof HdfsMrsVectorPyramidInputFormatDescriptor)
      {
        HdfsMrsVectorPyramidInputFormatDescriptor vfd = (HdfsMrsVectorPyramidInputFormatDescriptor)ifd;
        bounds = vfd.calculateBounds();
        return bounds;
      }
    }
    throw new IOException("Method called too soon, bounds cannot be computed at this time.");
  }

  @Override
  public Set<String> calculateInputs()
  {
    Set<String> inputPyramids = new HashSet<String>();
    if (_outputName != null)
    {
      inputPyramids.add(_outputName.toString());
    }
    return inputPyramids;
  }

  @Override
  public int calculateMaximumZoomlevel()
  {
    return zoomlevel;
  }

  @Override
  public void moveOutput(final String toName) throws IOException
  {
    super.moveOutput(toName);
    _outputName = toName;
    MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(_outputName,
        AccessMode.READ, getProviderProperties());
    _output = MrsPyramidDescriptor.create(dp);
  }

  @Override
  public int calculateTileSize()
  {
    return tilesize;
  }

  @Override
  public Vector<ParserNode> processChildren(final Vector<ParserNode> children, final ParserAdapter parser)
  {
    final Vector<ParserNode> result = new Vector<ParserNode>();

    if (!(children.size() == 3 || children.size() == 4 || children.size() == 7 || children.size() == 8))
    {
      throw new IllegalArgumentException(
          "RasterizeVector takes these arguments. (source vector, aggregation type, cellsize, optionally column, optionally bounds)");
    }

    result.add(children.get(0));

    final String str = MapOpHadoop.parseChildString(children.get(1), "aggregation type", parser);
    try
    {
      _aggregationType = RasterizeVectorPainter.AggregationType.valueOf(str.toUpperCase());
    }
    catch (final IllegalArgumentException e)
    {
      throw new IllegalArgumentException("Aggregation type must be one of: "
          + StringUtils.join(RasterizeVectorPainter.AggregationType.values(), ", "));
    }

    tilesize = Integer.parseInt(MrGeoProperties.getInstance().getProperty(
        MrGeoConstants.MRGEO_MRS_TILESIZE, MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT));

    String cs = MapOpHadoop.parseChildString(children.get(2), "cell size", parser);
    if (cs.endsWith("m"))
    {
      cs = cs.replace("m", "");
      cellSize = Double.parseDouble(cs) / LatLng.METERS_PER_DEGREE;
    }
    else if (cs.endsWith("z"))
    {
      cs = cs.replace("z", "");

      cellSize = TMSUtils.resolution(Integer.parseInt(cs), tilesize);
    }
    else
    {
      if (cs.endsWith("d"))
      {
        cs = cs.replace("d", "");
      }
      cellSize = Double.parseDouble(cs);
    }

    if (_aggregationType == RasterizeVectorPainter.AggregationType.MASK)
    {
      if (children.size() == 4 || children.size() == 8)
      {
        throw new IllegalArgumentException("A column name must not be specified with MASK");
      }
    }
    if (_aggregationType == RasterizeVectorPainter.AggregationType.SUM)
    {
      if (children.size() == 4 || children.size() == 8)
      {
        column = ((ParserConstantNode) children.get(3)).getValue().toString();
      }
    }
    if (_aggregationType == RasterizeVectorPainter.AggregationType.MAX)
    {
      if (children.size() == 4 || children.size() == 8)
      {
        column = MapOpHadoop.parseChildString(children.get(3), "column", parser);
      }
    }
    if (_aggregationType == RasterizeVectorPainter.AggregationType.MIN)
    {
      if (children.size() == 4 || children.size() == 8)
      {
        column = MapOpHadoop.parseChildString(children.get(3), "column", parser);
      }
    }
    if (_aggregationType == RasterizeVectorPainter.AggregationType.AVERAGE)
    {
      if (children.size() == 4 || children.size() == 8)
      {
        column = MapOpHadoop.parseChildString(children.get(3), "column", parser);
      }
    }
    if (_aggregationType == RasterizeVectorPainter.AggregationType.LAST)
    {
      if (children.size() == 3 || children.size() == 7)
      {
        throw new IllegalArgumentException(
            "A column must be specified with LAST. (try putting the value in double quotes)");
      }
      column = MapOpHadoop.parseChildString(children.get(3), "column", parser);
    }

    if (children.size() > 4)
    {
      int position;
      if (_aggregationType == RasterizeVectorPainter.AggregationType.LAST)
      {
        position = 4;
      }
      else if (_aggregationType == RasterizeVectorPainter.AggregationType.SUM &&
          children.size() == 8)
      {
        position = 4;
      }
      else
      {
        position = 3;
      }
      final double[] b = new double[4];
      for (int i = 0; i < 4; i++)
      {
        b[i] = MapOpHadoop.parseChildDouble(children.get(i + position), "bounds", parser);
      }
      bounds = new Bounds(b[0], b[1], b[2], b[3]);
    }

    zoomlevel = TMSUtils.zoomForPixelSize(cellSize, tilesize);

    return result;
  }

  @Override
  public String toString()
  {
    return String.format("RasterizeVectorMapOp");
  }


  @Override
  public void build(final Progress p) throws IOException, JobFailedException,
      JobCancelledException
  {
    if (p != null)
    {
      p.starting();
    }

    final InputFormatDescriptor ifd = ((VectorMapOp)_inputs.get(0)).getVectorOutput();

    if (bounds == null)
    {
      bounds = calculateBounds();
    }

    // TODO:  MrsVector rasterize is commented out, because of a circular dependency...
//    if (ifd instanceof HdfsMrsVectorPyramidInputFormatDescriptor)
//    {
//      final RasterizeMrsVectorDriver rvd = new RasterizeMrsVectorDriver();
//      rvd.setValueColumn(column);
//      rvd.run(getConf(), ((VectorMapOp)_inputs.get(0)).getOutputName(), _outputName,
//          _aggregationType, zoomlevel, bounds, p, jobListener);
//    }
//    else
    {
      final RasterizeVectorDriver rvd = new RasterizeVectorDriver();

      final Job job = new Job(createConfiguration());

      ifd.populateJobParameters(job);
      rvd.setValueColumn(column);

      rvd.run(job, _outputName, _aggregationType, zoomlevel, bounds, p, jobListener,
          getProtectionLevel(), getProviderProperties());
    }
    MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(_outputName,
        AccessMode.READ, getProviderProperties());
    _output = MrsPyramidDescriptor.create(dp);

    if (p != null)
    {
      p.complete();
    }
  }

}
