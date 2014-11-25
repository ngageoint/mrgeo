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

package org.mrgeo.data.accumulo.image;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.data.accumulo.tile.AccumuloMrsTileReader;
import org.mrgeo.data.accumulo.utils.MrGeoAccumuloConstants;
import org.mrgeo.data.image.MrsImagePyramidReaderContext;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.utils.LongRectangle;

import java.awt.image.Raster;
import java.io.IOException;
import java.util.Properties;

public class AccumuloMrsImageReader extends AccumuloMrsTileReader<Raster, RasterWritable>
{

  private AccumuloMrsImageDataProvider provider;
  private MrsImagePyramidReaderContext context;
  private int tileSize = -1;
  private Properties queryProps;
  //private int zoomLevel = -1;
  
  public AccumuloMrsImageReader(int z, String table)
  {
    super(z, table);
    // TODO Auto-generated constructor stub
  }

  public AccumuloMrsImageReader(AccumuloMrsImageDataProvider provider,
      MrsImagePyramidReaderContext context) throws IOException {
    super(context.getZoomlevel(), provider.getResolvedName());
    this.provider = provider;
    this.context = context;
    this.tileSize = provider.getMetadataReader().read().getTilesize();
    zoomLevel = this.context.getZoomlevel();

  }

  public AccumuloMrsImageReader(Properties props, AccumuloMrsImageDataProvider provider,
      MrsImagePyramidReaderContext context) throws IOException {
    super(props, context.getZoomlevel(), provider.getResolvedName());
    this.provider = provider;
    this.context = context;
    this.tileSize = provider.getMetadataReader().read().getTilesize();
    zoomLevel = this.context.getZoomlevel();
    queryProps = new Properties();
    queryProps.setProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_AUTHS, MrGeoAccumuloConstants.MRGEO_ACC_NOAUTHS);
    queryProps.putAll(props);

  }

  
//  @Override
//  public MrsImagePyramidMetadata loadMetadata()
//  {
//    // TODO Auto-generated method stub
//    return null;
//  }

  @Override
  public int calculateTileCount(){
    int zl = context.getZoomlevel();
    try{
      MrsImagePyramidMetadata meta = provider.getMetadataReader().read();
      LongRectangle lr = meta.getOrCreateTileBounds(zl);
      long count = (lr.getMaxX() - lr.getMinX() + 1) *
          (lr.getMaxY() - lr.getMinY() + 1);
      
      return (int) count;
    } catch(IOException ioe){
      return -1;
    }
  } // end calculateTileCount
  
  @Override
  protected Raster toNonWritable(byte[] val, CompressionCodec codec, Decompressor decompressor)
      throws IOException
  {
    if(codec == null || decompressor == null){
      return RasterWritable.toRaster(new RasterWritable(val));
    }
    return RasterWritable.toRaster(new RasterWritable(val), codec, decompressor);
  }

  @Override
  public int getTileSize()
  {
    return tileSize;
  }
  
 

  
  
} // end AccumuloMrsImageReader
