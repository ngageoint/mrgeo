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

package org.mrgeo.data.accumulo.image;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.accumulo.utils.AccumuloConnector;
import org.mrgeo.data.accumulo.utils.AccumuloUtils;
import org.mrgeo.data.accumulo.utils.MrGeoAccumuloConstants;
import org.mrgeo.data.image.MrsPyramidWriterContext;
import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.image.MrsImageWriter;
import org.mrgeo.data.tile.TileIdWritable;

import java.io.IOException;
import java.util.Properties;

public class AccumuloMrsImageWriter implements MrsImageWriter
{

  final private AccumuloMrsImageDataProvider provider;
  final private MrsPyramidWriterContext context;
  private Connector conn;
  private Properties mrgeoAccProps;
  private ColumnVisibility cv;
  //private String pl;
  private BatchWriter bw;
  protected long memBuf = 1000000L; // bytes to store before sending a batch
  //protected long timeout = 1000L; // milliseconds to wait before sending
  //protected int numThreads = 10;

  /**
   * 
   * 
   * @param provider - the provider that invoked this
   * @param context - the context of the job
   * @param pl - the protection level of the image to be stored
   */
  public AccumuloMrsImageWriter(AccumuloMrsImageDataProvider provider,
                                MrsPyramidWriterContext context, String pl){

    this.provider = provider;
    this.context = context;
    //this.pl = pl;
    try{
      mrgeoAccProps = AccumuloConnector.getAccumuloProperties();

      BatchWriterConfig bwc = new BatchWriterConfig();
      bwc.setMaxMemory(memBuf);
      this.conn = AccumuloConnector.getConnector();

      String resource = provider.getResourceName();
      if(resource.startsWith(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX)){
        resource = resource.replace(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX, "");
      }
      
      bw = this.conn.createBatchWriter(resource, bwc);
      
    } catch(DataProviderException dpe){
      dpe.printStackTrace();
    } catch(TableNotFoundException tnfe){
      tnfe.printStackTrace();
    }

    if(pl == null){
    //if(provider.getColumnVisibility() == null){
    	if(mrgeoAccProps.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_VIZ) != null){
    		cv = new ColumnVisibility(mrgeoAccProps.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_VIZ));
    	} else {
    		cv = new ColumnVisibility();
    	}
    } else {
    	cv = new ColumnVisibility(pl);
    }
    
  } // end constructor

  @Override
  public void append(final TileIdWritable k, final MrGeoRaster raster) throws IOException
  {
	  if(cv == null){
		  cv = provider.getColumnVisibility();
	  }
    Mutation m = new Mutation(AccumuloUtils.toRowId(k.get()));
    RasterWritable rw = RasterWritable.toWritable(raster);
    // We only want the actual bytes for the value, not the full array of bytes
    Value value = new Value(rw.copyBytes());
    m.put("" + context.getZoomlevel(), "" + k.get(), cv, value);
    try{
      bw.addMutation(m);
    } catch(MutationsRejectedException mre){
      throw new IOException(mre.getCause());
    }
  } // end append
  
  @Override
  public void close() throws IOException
  {
    try{
      bw.flush();
      bw.close();
    } catch(MutationsRejectedException mre){
      throw new IOException(mre.getCause());
    }
  } // end close

  @Override
  public String getName() throws IOException
  {
    return provider.getResourceName();
  }

} // end AccumuloMrsImageWriter
