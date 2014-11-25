/**
 *
 */
package org.mrgeo.featurefilter;

import org.mrgeo.geometry.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class ColumnFeatureFilter extends BaseFeatureFilter
{

  private static final Logger log = LoggerFactory.getLogger(ColumnFeatureFilter.class);

  protected String filterColumn;
  public String getFilterColumn() { return filterColumn; }
  public void setFilterColumn(String filterColumn) { this.filterColumn = filterColumn; }

  protected String filterValue;
  public String getFilterValue() { return filterValue; }
  public void setFilterValue(String filterValue) { this.filterValue = filterValue; }

  protected String featureAttributeValue;

  protected FilterType filterType;
  public FilterType getFilterType() { return filterType; }
  public void setFilterType(FilterType filterType) { this.filterType = filterType; }
  public enum FilterType
  {
    TEXT, NUMERIC, DATE
  }

  /* (non-Javadoc)
   * @see org.mrgeo.featurefilter.BaseFeatureFilter#filterInPlace(com.vividsolutions.jump.feature.Feature)
   */
  @Override
  public Geometry filterInPlace(Geometry feature)
  {
    assert(filterColumn != null);
    assert(filterValue != null);
    assert(filterType != null);

    if (!feature.hasAttribute(filterColumn))
    {
      //error checking for this case should be done before creating the filter, so just return
      //nulls here
      log.debug("Column: " + filterColumn + " not found for feature: " + feature.toString());
      return null;
    }
    featureAttributeValue = feature.getAttribute(filterColumn);

    return feature;
  }
}
