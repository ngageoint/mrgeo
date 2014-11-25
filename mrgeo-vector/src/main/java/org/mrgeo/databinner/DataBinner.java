package org.mrgeo.databinner;

/**
 * Classes that implement this interface should be configured at construction
 * time with all the supplementary data required for computing bins for
 * individual data values.
 */
public interface DataBinner
{
  /**
   * Computes the bin number for a specific data value.
   * @param value
   * @return
   */
  public int calculateBin(double value);

  /**
   * Returns the total number of bins.
   * @return
   */
  public int getBinCount();
}
