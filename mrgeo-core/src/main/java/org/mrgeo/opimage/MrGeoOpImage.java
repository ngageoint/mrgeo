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

import org.mrgeo.rasterops.OpImageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.media.jai.OpImage;
import java.awt.*;
import java.awt.image.RenderedImage;
import java.util.Vector;

@SuppressWarnings("unchecked")
public abstract class MrGeoOpImage extends OpImage
{
  private static final Logger log = LoggerFactory.getLogger(MrGeoOpImage.class);

  protected double nullValue = Double.NaN;
  protected boolean isNullValueNaN = true;

  @SuppressWarnings("rawtypes")
  protected MrGeoOpImage(Vector sources, RenderingHints hints)
  {
    super(sources, null, null, false);
    
    RenderedImage src = (RenderedImage) sources.get(0);

    // The output of this OpImage should use the same NoData value as the first input
    nullValue = OpImageUtils.getNoData(src, Double.NaN);
    isNullValueNaN = Double.isNaN(nullValue);
    
    // set the nodata, in case we used the default in the above getNodata()
    OpImageUtils.setNoData(this, nullValue);
    
  }
  


  public double getNull()
  {
    return nullValue;
  }


  public final boolean isNull(double v)
  {
    return OpImageUtils.isNoData(v, nullValue, isNullValueNaN);
  }

  public final boolean isNull(double[] v)
  {
    if (v.length == 4)
    {
      if (v[3] < 0.5)
      {
        return true;
      }
      else if (v[0] < 0.5 && v[1] < 0.5 && v[2] < 0.5)
      {
        return true;
      }
      else
      {
        return false;
      }
    }
    return isNull(v[0]);
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
    return getClass().getSimpleName();
  }

}
