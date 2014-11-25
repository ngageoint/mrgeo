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

package org.mrgeo.image;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.TMSUtils;

import java.io.IOException;
import java.util.Properties;

public class AllOnes extends TestFiles
{
  public AllOnes() throws JsonGenerationException, JsonMappingException, IOException {
    super.setup(org.mrgeo.core.Defs.CWD + "/" + org.mrgeo.core.Defs.INPUT + "/" + "all-ones",
        (Properties)null);
  }
  
  public static void main(String args[]) throws JsonGenerationException, JsonMappingException, IOException {
    AllOnes allOnes = new AllOnes();
    int zoom = allOnes.getMetadata().getMaxZoomLevel();
    
    LongRectangle tb = allOnes.getMetadata().getTileBounds(zoom);
    final long minX = tb.getMinX();
    final long minY = tb.getMinY();
    
    for(int ty=0; ty < allOnes.getRows(); ty++) {
      for(int tx=0; tx < allOnes.getCols(); tx++) {
          System.out.println(String.format("Tile %d has bounds %s", 
                                          TMSUtils.tileid(tx+minX, ty+minY, zoom),
                                          allOnes.getBounds(ty, tx)));
      }    
    }
  }
}
