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
