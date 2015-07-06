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

/**
 * 
 */
package org.mrgeo.featurefilter;

import org.mrgeo.geometry.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

/**
 * Allows for filtering vector features with a regular expression text matching filter
 */
public class TextColumnFeatureFilter extends ColumnFeatureFilter
{

  protected ParsingMethod parsingMethod = ParsingMethod.EXACT;
  public ParsingMethod getParsingMethod() { return parsingMethod; }
  public void setParsingMethod(ParsingMethod parsingMethod) { this.parsingMethod = parsingMethod; } 
  public enum ParsingMethod 
  {
    EXACT, //exact match of the input string; special wildcard/regex strings are interpreted literally
    WILDCARD, //exact match + supports '*' char only for wildcard string matching
    REGEX //regular expression string; special chars must be \ escaped
  }
  
  @Override
  public void setFilterValue(String filterValue) 
  { 
    this.filterValue = filterValue; 
    wildcardFilterConvertedToRegex = false;
  } 
  
  private boolean wildcardFilterConvertedToRegex = false;
  
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
    
    if ((parsingMethod.equals(ParsingMethod.EXACT) && filterValue.equals(featureAttributeValue)) ||
        (parsingMethod.equals(ParsingMethod.REGEX) && 
          Pattern.matches(filterValue, featureAttributeValue)))
    {
      return feature;
    }
    else if (parsingMethod.equals(ParsingMethod.WILDCARD))
    {
      if (!wildcardFilterConvertedToRegex)
      {
        //this will force all chars except, '*' (the only special char we support for wildcards) to
        //be interpreted literally
        filterValue = Pattern.quote(filterValue).replaceAll("\\*", "\\\\E.*\\\\Q");
        wildcardFilterConvertedToRegex = true;
      }
      if (Pattern.matches(filterValue, featureAttributeValue))
      {
        return feature;
      }
    }
    return null;
  }
}
