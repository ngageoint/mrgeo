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

package org.mrgeo.mapalgebra;

import org.apache.hadoop.conf.Configuration;
import org.mrgeo.aggregators.MeanAggregator;
import org.mrgeo.buildpyramid.BuildPyramidSpark;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.ProtectionLevelUtils;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.mapalgebra.parser.ParserException;
import org.mrgeo.mapreduce.job.*;
import org.mrgeo.progress.Progress;
import org.mrgeo.progress.ProgressHierarchy;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class MapAlgebraJob implements RunnableJob
{
  private static final Logger _log = LoggerFactory.getLogger(MapAlgebraJob.class);
  String _expression;
  String _output;
  Progress _progress;
  JobListener jobListener = null;
  private String protectionLevel;
  private Properties providerProperties;

  public MapAlgebraJob(String expression, String output,
      final String protectionLevel,
      final Properties providerProperties)
  {
    _expression = expression;
    _output = output;
    this.protectionLevel = protectionLevel;
    this.providerProperties = providerProperties;
  }
  
  @Override
  public void setProgress(Progress p) {
    _progress = p;
  }
  
  @Override
  public void run()
  {
    try
    {
      _progress.starting();
      OpImageRegistrar.registerMrGeoOps();

      Configuration conf = HadoopUtils.createConfiguration();
      MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(_output, AccessMode.OVERWRITE, conf);
      String useProtectionLevel = ProtectionLevelUtils.getAndValidateProtectionLevel(dp, protectionLevel);
      MapAlgebraParser parser = new MapAlgebraParser(conf, useProtectionLevel,
          providerProperties);
      MapOp op = null;
      try
      {
        op = parser.parse(_expression);
      }
      catch (ParserException e)
      {
        throw new IOException(e);
      }

      ProgressHierarchy ph = new ProgressHierarchy(_progress);
      ph.starting();
      ph.createChild(2.0f);
      ph.createChild(1.0f);
      MapAlgebraExecutioner exec = new MapAlgebraExecutioner();
      exec.setJobListener(jobListener);
      exec.setOutputName(_output);
      exec.setRoot(op);

      //do not build pyramids now
      exec.execute(conf, ph.getChild(0), false);

      if (_progress != null && _progress.isFailed())
      {
        throw new JobFailedException(_progress.getResult());
      }

//      if (op.getOutputType() == MapOp.DataType.Raster)
//      {
//        String wms = /* props.getProperty("base.url") + */"WmsGenerator?LAYERS="
//            + _output.getName();
//        updateFactor(_expression, _output.toString(), wms);
//      }
      ph.complete();
      _progress.complete();

      //Build pyramid is a post processing step and should happen
      //at the end, the job should be marked complete, but the 
      //build pyramid processing will still go on.
      buildPyramid(op, conf, providerProperties);
    }
    catch (JobCancelledException j)
    {
      _log.error("JobCancelledException occurred while processing mapalgebra job " + j.getMessage(), j);
      _progress.cancelled();      
      cancel();
    }
    catch (JobFailedException j) {
      _log.error("JobFailedException occurred while processing mapalgebra job " + j.getMessage(), j);
      _progress.failed(j.getMessage());
    }
    catch (Exception e)
    {
      _log.error("Exception occurred while processing mapalgebra job " + e.getMessage(), e);
      _progress.failed(e.getMessage());      
    }
    catch (Throwable e)
    {
      _log.error("Throwable error occurred while processing mapalgebra job " + e.getMessage(), e);
      _progress.failed(e.getMessage());
    }
  }
  
  private void buildPyramid(MapOp op, Configuration conf,
      Properties providerProperties) throws Exception
  {
    TaskProgress taskProg = new TaskProgress(_progress);
    try {
      if (op instanceof RasterMapOp)
      {        
        taskProg.starting();
        BuildPyramidSpark.build(_output, new MeanAggregator(),
            conf, taskProg, null, providerProperties);
        taskProg.complete();
      }
      else
      {
        taskProg.notExecuted();
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
      _log.error("Exception occurred while processing mapalgebra job " + e.getMessage());      
      taskProg.cancelled();     
    }
  }
  
//  private void updateFactor(String expression, String output, String wms)  throws SQLException, 
//  IOException
//{
//  float min = 0;
//  float max = 0;
//  int tilesize = 0;
//  String pixelsize = "";
//  String bounds = "";
//  final MrsImagePyramid pyramid = MrsImagePyramid.loadPyramid(output);
//  if (pyramid != null)
//  {
//    if(pyramid.getStats() != null && !Double.isNaN(pyramid.getStats().min) && 
//        !Double.isNaN(pyramid.getStats().max))
//    {
//      min = (float)pyramid.getStats().min;
//      max = (float)pyramid.getStats().max;
//    }
//    if (!Double.isNaN(min))
//    {
//      min = 0;  //something bad happened
//    }
//    if (!Double.isNaN(max))
//    {
//      max = 0;
//    }
//    
//    tilesize = pyramid.getMetadata().getTilesize();
//    double minX = pyramid.getBounds().getMinX();
//    double minY = pyramid.getBounds().getMinY();
//    double maxX = pyramid.getBounds().getMaxX();
//    double maxY = pyramid.getBounds().getMaxY();
//    bounds = minX + " " + minY + " " + maxX + " " + maxY;
//    double degreePerPixelX = Math.abs(maxX - minX)/tilesize;
//    double degreePerPixelY = Math.abs(maxY - minY)/tilesize;
//    pixelsize = degreePerPixelX + " " + degreePerPixelY;
//  }
//  
//  String dbPath = MrGeoProperties.getInstance().getProperty("db.path");
//  String protocol = "jdbc:derby:";
//  /**
//   * Get information from factor table
//   */
//  Connection conn = DriverManager.getConnection(protocol + dbPath + ";create=true");
//  try {
//    Statement s = conn.createStatement();
//    
//    DatabaseMetaData metadata = null;
//    metadata = conn.getMetaData();
//    String[] names = { "TABLE"};
//    ResultSet tableNames = metadata.getTables( null, null, null, names);
//
//    boolean tableExist = false;
//    while( tableNames.next())
//    {
//      String tab = tableNames.getString( "TABLE_NAME");
//      if (tab.equalsIgnoreCase("factor"))
//      {
//        tableExist = true;
//        break;
//      }
//    }
//    
//    int expressionFieldSize = 8192;
//    if (!tableExist)
//    {
//      s.execute("CREATE TABLE factor(factor_id INT NOT NULL GENERATED ALWAYS AS IDENTITY, "
//          + "name VARCHAR(128), expression VARCHAR(" + expressionFieldSize + "), minvalue FLOAT, maxvalue FLOAT, "
//          + "bounds VARCHAR(256), pixelsize VARCHAR(128), tilesize INTEGER, "
//          + "metadata VARCHAR(8192), ispermanent SMALLINT DEFAULT 1)");
//
//      conn.commit();
//    }
//    
//    ResultSet rs = s.executeQuery("SELECT * FROM factor ORDER BY factor_id DESC");
//    
//    Map<String, String> mapFromTable = new HashMap<String, String>();
//    while (rs.next())
//    {
//      mapFromTable.put(rs.getString(2), rs.getString(3)); 
//    }
//    
//    if (mapFromTable.containsKey(output))
//    {
//      PreparedStatement updateStatus = conn
//          .prepareStatement("UPDATE factor SET expression=?, "
//              + "minvalue=?, maxvalue=?, bounds=?, pixelsize=?, tilesize=? "
//              + "WHERE name=?");
//    
//      updateStatus.setString(1, truncate(expression, expressionFieldSize));
//      updateStatus.setFloat(2, min);
//      updateStatus.setFloat(3, max);
//      updateStatus.setString(4, bounds);
//      updateStatus.setString(5, pixelsize);
//      updateStatus.setFloat(6, tilesize);
//      updateStatus.setString(7, output);
//      updateStatus.execute();
//      conn.commit();
//    }
//    else // new file found in MrGeo image dir but not in the table
//    {
//      PreparedStatement ps = conn
//          .prepareStatement("INSERT INTO factor (name, expression, minvalue, maxvalue, bounds, "
//              + "pixelsize, tilesize, metadata) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
//      
//      ps.setString(1, output);
//      ps.setString(2, truncate(expression, expressionFieldSize));
//      _log.debug("min: " + String.valueOf(min));
//      ps.setFloat(3, min);
//      _log.debug("max: " + String.valueOf(max));
//      ps.setFloat(4, max);
//      ps.setString(5, bounds);
//      ps.setString(6, pixelsize);
//      ps.setFloat(7, tilesize);
//      ps.setString(8, "");
//      ps.executeUpdate();
//      conn.commit();
//    }    
//  }
//  finally {
//    if (conn != null) {
//      conn.close();
//    }
//  }
//}
//
//  private String truncate(String value, int desiredLength)
//  {
//    return (value.length() > desiredLength) ? value.substring(0, desiredLength) : value;
//  }

  private void cancel()
  {
    try
    {
      jobListener.cancelAll();
    }
    catch (JobCancelFailedException j)
    {
      _log.error("Cancel failed due to " + j.getMessage());
    }
  }

  @Override
  public void setJobListener(JobListener jListener)
  {
    jobListener = jListener;   
  }
  
}

