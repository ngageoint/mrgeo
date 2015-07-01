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

package org.mrgeo.featurefilter;
import org.apache.commons.lang.NotImplementedException;
import org.mrgeo.databinner.EqualSizeDataBinner;
import org.mrgeo.geometry.Geometry;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

// TODO:  Refactor to use Geometry class, if still needed
public class BinFilter extends BaseFeatureFilter
{
  private static final long serialVersionUID = 1L;
  private int bins;
  Set<String> _copySet = null;
  private EqualSizeDataBinner binner = new EqualSizeDataBinner(false);

  /**
   * 
   * @param bins
   *          The number of bins numeric factors should be converted into
   * @param copySet
   *          The set of columns that should be copied before being nominalized.
   */
  public BinFilter(int bins, String[] copySet)
  {
    this.bins = bins;
    _copySet = new TreeSet<String>(Arrays.asList(copySet));

    throw new NotImplementedException("BinFilter is not Implemented!");
  }

  public BinFilter(int bins)
  {
    this.bins = bins;
  }

  @Override
  public Geometry filter(Geometry f)
  {
    // this filter in place creates a new feature, so we're doing this for
    // performance.
    return filterInPlace(f);
  }

  @Override
  public Geometry filterInPlace(Geometry f)
  {
//    int i = 0;
//    for (int j = 0; j < _inSchema.getAttributeCount(); j++)
//    {
//      if (_inSchema.getAttributeType(j) == AttributeType.DOUBLE
//          || _inSchema.getAttributeType(j) == AttributeType.INTEGER)
//      {
//        if (f.getString(j) == null || f.getString(j).isEmpty())
//        {
//          result.setAttribute(i, null);
//        }
//        else
//        {
//          double min = _inSchema.getAttributeMin(j);
//          double max = _inSchema.getAttributeMax(j);
//
//          if (min >= max)
//          {
//            //min > max, if all values for the field were null; min = max if all values for the
//            //field were equal
//            result.setAttribute(i, 0);
//          }
//          else
//          {
//            binner.update(this.bins, min, max);
//            try
//            {
//              double d = Double.parseDouble(f.getString(j));
//              int bin = binner.calculateBin(d);
//              result.setAttribute(i, bin);
//            }
//            catch (NumberFormatException e)
//            {
//            }
//          }
//        }
//      }
//      else
//      {
//        result.setAttribute(i, f.getAttribute(j));
//      }
//      i++;
//      if (_copySet != null && _copySet.contains(_inSchema.getAttributeName(j)))
//      {
//        result.setAttribute(i, f.getAttribute(j));
//        i++;
//      }
//    }
//
//    return result;
    return f.createWritableClone();
  }

//  private FeatureSchemaStats getSchema(FeatureSchema inSchema)
//  {
//    if (inSchema != _inSchema)
//    {
//      _inSchema = (FeatureSchemaStats) inSchema;
//      _outSchema = new FeatureSchemaStats();
//
//      for (int i = 0; i < inSchema.getAttributeCount(); i++)
//      {
//        if (inSchema.getAttributeType(i) == AttributeType.DOUBLE
//            || inSchema.getAttributeType(i) == AttributeType.INTEGER)
//        {
//          _outSchema.addAttribute(inSchema.getAttributeName(i), AttributeType.STRING);
//          _outSchema.setAttributeCount(i, _inSchema.getAttributeCount(i));
//          if (_copySet != null && _copySet.contains(inSchema.getAttributeName(i)))
//          {
//            String name = inSchema.getAttributeName(i) + "_numeric";
//            _outSchema.addAttribute(name, inSchema.getAttributeType(i));
//            _outSchema.setAttributeMin(_outSchema.getAttributeIndex(name), _inSchema
//                .getAttributeMin(i));
//            _outSchema.setAttributeMax(_outSchema.getAttributeIndex(name), _inSchema
//                .getAttributeMax(i));
//            _outSchema.setAttributeCount(_outSchema.getAttributeIndex(name), _inSchema
//                .getAttributeCount(i));
//          }
//        }
//        else
//        {
//          _outSchema.addAttribute(inSchema.getAttributeName(i), inSchema.getAttributeType(i));
//          if (_copySet != null && _copySet.contains(inSchema.getAttributeName(i)))
//          {
//            _outSchema.addAttribute(inSchema.getAttributeName(i) + "_numeric", inSchema
//                .getAttributeType(i));
//          }
//        }
//      }
//    }
//
//    return _outSchema;
//  }

}
