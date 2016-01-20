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

package org.mrgeo.image;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.TMSUtils;
import org.mrgeo.utils.TMSUtils.Bounds;
import org.mrgeo.utils.TMSUtils.TileBounds;

import java.io.IOException;

public abstract class TestFiles
{
  private int cols,rows;
  private Bounds[][] boundsOfTile;
  private TileBounds[] tileBoundsOfRow;
  
  private MrsPyramidMetadata metadata;
  
  protected void setup(String testFile,
      final ProviderProperties providerProperties) throws JsonGenerationException, JsonMappingException, IOException {
    
    MrsImageDataProvider provider = DataProviderFactory.getMrsImageDataProvider(testFile,
        AccessMode.READ, providerProperties);
    metadata = provider.getMetadataReader().read();

    int zoom = metadata.getMaxZoomLevel();
    int tileSize = metadata.getTilesize();
        
    LongRectangle tb = metadata.getTileBounds(zoom);

    cols = (int)(tb.getMaxX() - tb.getMinX() + 1);
    rows = (int)(tb.getMaxY() - tb.getMinY() + 1);
    
    tileBoundsOfRow = new TileBounds[rows];
    boundsOfTile = new Bounds[rows][cols];    
    for(long ty = tb.getMinY(); ty <= tb.getMaxY(); ty++) {
      int y = (int)(ty - tb.getMinY());
      tileBoundsOfRow[y] = new TileBounds(tb.getMinX(), ty, tb.getMaxX(), ty); 
      
      for(long tx = tb.getMinX(); tx <= tb.getMaxX(); tx++) {
        Bounds bounds = TMSUtils.tileBounds(tx, ty, zoom, tileSize);
        
        int x = (int)(tx - tb.getMinX());
        boundsOfTile[y][x] = bounds;
      }   
    }    
  }
  
  public Bounds getBounds(int row, int col) 
  {
    return boundsOfTile[row][col];
  }
  
  public Bounds getBoundsBottomRow() 
  {
    return (getBounds(0, 0)).union(getBounds(0, cols-1));
  }
  
  public Bounds getBoundsTopRow() 
  {
    return (getBounds(rows-1, 0)).union(getBounds(rows-1, cols-1));
  }
  
  public Bounds getBoundsInteriorRows() 
  {
    return (getBounds(1, 0)).union(getBounds(1, cols-1))
     .union(getBounds(rows-2, 0)).union(getBounds(rows-2, cols-1));    
  }
  
  public TileBounds getTileBoundsTopRow() {
    return tileBoundsOfRow[rows-1];
  }
  
  public TileBounds getTileBoundsBottomRow() {
    return tileBoundsOfRow[0];
  }
  
  public TileBounds getTileBoundsInteriorRows() {    
    return new TileBounds(tileBoundsOfRow[0].w, 
                          tileBoundsOfRow[0].s, 
                          tileBoundsOfRow[rows-1].e, 
                          tileBoundsOfRow[rows-1].n);
  }
  
  public int getCols()
  {
    return cols;
  }

  public int getRows()
  {
    return rows;
  }

  public MrsPyramidMetadata getMetadata()
  {
    return metadata;
  }  
}
