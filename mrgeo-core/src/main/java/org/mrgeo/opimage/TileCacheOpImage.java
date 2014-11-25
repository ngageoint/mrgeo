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

package org.mrgeo.opimage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.media.jai.ImageLayout;
import javax.media.jai.OpImage;
import javax.media.jai.PlanarImage;
import java.awt.*;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.io.Serializable;
import java.util.Vector;

@SuppressWarnings("unchecked")
final public class TileCacheOpImage extends OpImage implements Serializable
{
  @SuppressWarnings("unused")
  private static final Logger _log = LoggerFactory.getLogger(TileCacheOpImage.class);

  private static final long serialVersionUID = 1L;
  private int tw, th;

  public static TileCacheOpImage create(RenderedImage source)
  {
    return create(source, -1, null);
  }

  public static TileCacheOpImage create(RenderedImage source, int maxTileSize)
  {
    return create(source, maxTileSize, null);
  }

  public static TileCacheOpImage create(RenderedImage source, int maxTileSize, RenderingHints hints)
  {
    @SuppressWarnings("rawtypes")
    Vector tmp = new Vector();
    tmp.add(source);
    return new TileCacheOpImage(tmp, maxTileSize, hints);
  }

  RenderedImage src;


  @SuppressWarnings({ "rawtypes", "unused" })
  private TileCacheOpImage(Vector sources, int maxTileSize, RenderingHints hints)
  {
    super(sources, null, null, false);

//    if (hints == null)
//    {
//      hints = (RenderingHints)JAI.getDefaultInstance().getRenderingHints().clone();
//    }
//
//    if (hints.containsKey(JAI.KEY_TILE_CACHE))
//    {
//      setTileCache((TileCache) hints.get(JAI.KEY_TILE_CACHE));
//    }
//    else
//    {
//      setTileCache(JAI.getDefaultInstance().getTileCache());
//    }

    src = (RenderedImage) sources.get(0);
    tw = src.getTileWidth();
    th = src.getTileHeight();

    if (maxTileSize > 0)
    {
      while (tw > maxTileSize && tw % 2 == 0)
      {
        tw /= 2;
      }

      while (th > maxTileSize && th % 2 == 0)
      {
        th /= 2;
      }
    }
    
    colorModel = src.getColorModel();
    sampleModel = src.getSampleModel();
    tileWidth = tw;
    tileHeight = th;

    ImageLayout layout = new ImageLayout(src.getTileGridXOffset(), src.getTileGridYOffset(),
        tw, th, sampleModel, colorModel);
    setImageLayout(layout);
  }

  @Override
  protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect)
  {
    sources[0].copyData(dest);
  }

  @Override
  final public Raster computeTile(int tx, int ty)
  {
    WritableRaster result = createTile(tx, ty);
    
    src.copyData(result);
    
    return result;
  }

  @Override
  public synchronized void dispose()
  {
    cache.removeTiles(this);
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.media.jai.OpImage#mapDestRect(java.awt.Rectangle, int)
   */
  @Override
  public Rectangle mapDestRect(Rectangle destRect, int sourceIndex)
  {
    return destRect;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.media.jai.OpImage#mapSourceRect(java.awt.Rectangle, int)
   */
  @Override
  public Rectangle mapSourceRect(Rectangle sourceRect, int sourceIndex)
  {
    return sourceRect;
  }
  
  @Override
  public String toString()
  {
    return getClass().getSimpleName();
  }

}
