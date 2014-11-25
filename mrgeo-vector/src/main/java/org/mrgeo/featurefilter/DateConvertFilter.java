package org.mrgeo.featurefilter;


import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.WritableGeometry;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Uses the java.text.SimpleDateFormat class to reformat a date field.
 */
public class DateConvertFilter extends BaseFeatureFilter
{
  private static final long serialVersionUID = 1L;
  SimpleDateFormat inputFormat;
  SimpleDateFormat outputFormat;
  String attributeName;

  public DateConvertFilter(String inputFormat, String outputFormat, String attributeName)
  {
    this.inputFormat = new SimpleDateFormat(inputFormat);
    this.outputFormat = new SimpleDateFormat(outputFormat);
    this.attributeName = attributeName;
  }

  @Override
  public Geometry filterInPlace(Geometry f)
  {

    WritableGeometry result = f.createWritableClone();

    String inDate = result.getAttribute(attributeName);
    
    Date d;
    try
    {
      d = inputFormat.parse(inDate);
    }
    catch (ParseException e)
    {
      d = null;
    }
    if (d == null)
    {
      result.setAttribute(attributeName, null);
    }
    else
    {
      result.setAttribute(attributeName, outputFormat.format(d));
    }
    
    return result;
  }
}
