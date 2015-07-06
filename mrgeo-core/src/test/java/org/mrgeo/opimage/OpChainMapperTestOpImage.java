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

package org.mrgeo.opimage;

import org.mrgeo.utils.TMSUtils;

import javax.media.jai.OpImage;
import javax.media.jai.PlanarImage;
import java.awt.*;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.io.Serializable;
import java.util.Vector;

@SuppressWarnings("unchecked")
public class OpChainMapperTestOpImage extends OpImage implements Serializable, TileLocator
{
  private static final long serialVersionUID = 1L;

  static final double _trueThreshold = 0.0001;
  
  private long tx;
  private long ty;
  private int zoom;
  @SuppressWarnings("unused")
  private int tileSize;

  public static OpChainMapperTestOpImage create(RenderedImage src1)
  {
    @SuppressWarnings("rawtypes")
    Vector sources = new Vector();
    sources.add(src1);
    return new OpChainMapperTestOpImage(sources);
  }

  @SuppressWarnings("rawtypes")
  private OpChainMapperTestOpImage(Vector sources)
  {
    super(sources, null, null, false);

    RenderedImage src = (RenderedImage) sources.get(0);

    colorModel = src.getColorModel();
    sampleModel = src.getSampleModel();
  }

  @Override
  protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect)
  {
//    final Raster r1 = sources[0].getData(destRect);

    for (int y = destRect.y; y < destRect.y + destRect.height; y++)
    {
      for (int x = destRect.x; x < destRect.x + destRect.width; x++)
      {
        long tileId = TMSUtils.tileid(tx, ty, zoom);
        dest.setSample(x, y, 0, (double)tileId);
      }
    }
  }

  @Override
  public Rectangle mapDestRect(Rectangle destRect, int sourceIndex)
  {
    return destRect;
  }

  @Override
  public Rectangle mapSourceRect(Rectangle sourceRect, int sourceIndex)
  {
    return sourceRect;
  }

  @Override
  public String toString()
  {
    return String.format("TestOpImage");
  }

  @Override
  public void setTileInfo(long tx, long ty, int zoom, int tileSize)
  {
    this.tx = tx;
    this.ty = ty;
    this.zoom = zoom;
    this.tileSize = tileSize;
  }
}
