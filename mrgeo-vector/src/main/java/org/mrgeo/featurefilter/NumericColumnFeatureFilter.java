/**
 * 
 */
package org.mrgeo.featurefilter;

import org.mrgeo.geometry.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allows for filtering vector features by numeric relations
 */
public class NumericColumnFeatureFilter extends ColumnFeatureFilter
{
  private static final Logger log = LoggerFactory.getLogger(NumericColumnFeatureFilter.class);
  
  @Override
  public void setFilterValue(String filterValue) 
  { 
    Double.parseDouble(filterValue.trim());   //throws an exception if filter not a number
    this.filterValue = filterValue; 
  } 
  
  protected ComparisonMethod comparisonMethod = ComparisonMethod.EQUAL_TO;
  public ComparisonMethod getComparisonMethod() { return comparisonMethod; }
  public void setComparisonMethod(ComparisonMethod comparisonMethod) 
  { this.comparisonMethod = comparisonMethod; } 
  public enum ComparisonMethod 
  {
    EQUAL_TO, 
    GREATER_THAN,
    GREATER_THAN_OR_EQUAL_TO,
    LESS_THAN, 
    LESS_THAN_OR_EQUAL_TO,
    NOT_EQUAL_TO
  }
  
  /* (non-Javadoc)
   * @see org.mrgeo.featurefilter.BaseFeatureFilter#filterInPlace(com.vividsolutions.jump.feature.Feature)
   */
  @Override
  public Geometry filterInPlace(Geometry feature)
  {
    feature = super.filterInPlace(feature);
    if (feature == null)
    {
      return null;
    }
    
    double filterValueNumeric = Double.parseDouble(filterValue.trim()); 
    try
    {
      double featureAttributeValueNumeric = Double.parseDouble(featureAttributeValue.trim());
      if (numberPassesFilter(featureAttributeValueNumeric, filterValueNumeric))
      {
        return feature;
      }
    }
    catch (NumberFormatException e)
    {
      log.debug("Invalid number: " + featureAttributeValue + " for column: " + 
        filterColumn);
      return null;
    }
    return null;
  }
  
  private boolean numberPassesFilter(double number, double numberFilter)
  {
    return
      ((comparisonMethod.equals(ComparisonMethod.EQUAL_TO) && (number == numberFilter)) ||
       (comparisonMethod.equals(ComparisonMethod.GREATER_THAN) && (number > numberFilter)) ||
       (comparisonMethod.equals(ComparisonMethod.GREATER_THAN_OR_EQUAL_TO) && (number >= numberFilter)) ||
       (comparisonMethod.equals(ComparisonMethod.LESS_THAN) && (number < numberFilter)) ||
       (comparisonMethod.equals(ComparisonMethod.LESS_THAN_OR_EQUAL_TO) && (number <= numberFilter)) ||
       (comparisonMethod.equals(ComparisonMethod.NOT_EQUAL_TO) && (number != numberFilter)));
  }
}
