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

package org.mrgeo.mapalgebra.vector.paint;

import java.awt.*;
import java.awt.image.ColorModel;

public class WeightedComposite implements Composite
{
protected double weight = 1.0;
protected double nodata = Double.NaN;
protected boolean isNodataNaN = true;

public WeightedComposite()
{
}

public WeightedComposite(final double weight)
{
  this.weight = weight;
}

public WeightedComposite(final double weight, final double nodata)
{
  this.weight = weight;
  setNodata(nodata);
}

public double getWeight()
{
  return weight;
}

public void setWeight(double weight)
{
  this.weight = weight;
}

public double getNodata()
{
  return nodata;
}

public void setNodata(double nodata)
{
  this.nodata = nodata;
  this.isNodataNaN = Double.isNaN(nodata);
}

@Override
public CompositeContext createContext(ColorModel srcColorModel, ColorModel dstColorModel,
    RenderingHints hints)
{
  return null;
}

}
