package org.mrgeo.paint;

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

  public void setWeight(double weight)
  {
    this.weight = weight;
  }
  
  public double getWeight()
  {
    return weight;
  }
  
  public void setNodata(double nodata)
  {
    this.nodata = nodata;
    this.isNodataNaN = Double.isNaN(nodata);
  }
  
  public double getNodata()
  {
    return nodata;
  }
  
  @Override
  public CompositeContext createContext(ColorModel srcColorModel, ColorModel dstColorModel,
      RenderingHints hints)
  {
    return null;
  }

}
