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

package org.mrgeo.mapalgebra;

import java.io.IOException;
import java.util.Vector;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.mrgeo.featurefilter.ColumnFeatureFilter;
import org.mrgeo.featurefilter.DateColumnFeatureFilter;
import org.mrgeo.featurefilter.FeatureFilter;
import org.mrgeo.featurefilter.NumericColumnFeatureFilter;
import org.mrgeo.featurefilter.TextColumnFeatureFilter;
import org.mrgeo.hdfs.vector.ColumnDefinitionFile;
import org.mrgeo.mapalgebra.parser.ParserAdapter;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterByColumnMapOp extends FeatureFilterMapOp
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(FilterByColumnMapOp.class);
  
  private ColumnFeatureFilter filter;
  
  /* (non-Javadoc)
   * @see org.mrgeo.mapreduce.FeatureFilterMapOp#getFilter()
   */
  @Override
  public FeatureFilter getFilter() { return filter; }

  @Override
  public void addInput(MapOpHadoop n) throws IllegalArgumentException
  {
    //determine if the input actually contains the column name
    if (n instanceof VectorReaderMapOp)
    {
      String inputFile = ((VectorReaderMapOp)n).getOutputName();
      String colName = filter.getFilterColumn();
      Path colsFile = new Path(inputFile + ".columns");
      try
      {
        new ColumnDefinitionFile(colsFile).getColumn(colName);
      }
      catch (IllegalArgumentException e)
      {
        throw new IllegalArgumentException(
          "Input data at " + inputFile + " does not contain column: " + colName);
      }
      catch (IOException e)
      {
        throw new IllegalArgumentException(
          "Unable to open column definition file at " + colsFile.toString());
      }
    }
    
    super.addInput(n);
  }
  
  @Override
  public Vector<ParserNode> processChildren(final Vector<ParserNode> children, final ParserAdapter parser)
  {
    Vector<ParserNode> result = new Vector<ParserNode>();
    if (children.size() != 4 && children.size() != 5 && children.size() != 6)
    {
      throw new IllegalArgumentException(
        "FilterByColumn takes either four, five, or six arguments: " +
        "  Inputs for all filter types: " +
        "    source delimited file path, " +
        "    filter column name, " +
        "    filter value, " +
        "    filter type," +
        "  Additionally, optional for text filters: " +
        "    parse type (defaults to EXACT)" +
        "  Additionally, optional for numeric filters: " +
        "    comparison type (defaults to EQUALS)" +
        "  Additionally, optional for date filters: " +
        "    date format (defaults to MM-dd-YYYY)," +
        "    date filter granularity (defaults to SAME_INSTANT)");
    }
    
    result.add(children.get(0));
    
    String str = parseChildString(children.get(3), "filter type", parser);
    if (StringUtils.isEmpty(str))
    {
      throw new IllegalArgumentException("Empty filter type");
    }
    
    ColumnFeatureFilter.FilterType filterType = 
      ColumnFeatureFilter.FilterType.valueOf(str.toUpperCase());
    filter = featureFilterForFilterType(filterType);
    filter.setFilterType(filterType);
    
    str = parseChildString(children.get(1), "filter column", parser);
    if (StringUtils.isEmpty(str))
    {
      throw new IllegalArgumentException("Empty filter column");
    }
    filter.setFilterColumn(str);
    
    str = parseChildString(children.get(2), "filter value", parser);
    if (StringUtils.isEmpty(str))
    {
      throw new IllegalArgumentException("Empty filter value");
    }
    filter.setFilterValue(str);
    
    
    if (children.size() == 5)
    {
      //TODO: if the logic for selecting filters gets more complex, make a real factory to handle 
      //this instead
      if (filter.getFilterType().equals(ColumnFeatureFilter.FilterType.TEXT))
      {
        TextColumnFeatureFilter textFilter = (TextColumnFeatureFilter)filter;
        str = parseChildString(children.get(4), "text filter parsing method", parser);
        if (StringUtils.isEmpty(str))
        {
          throw new IllegalArgumentException("Empty text filter parsing method");
        }
        textFilter.setParsingMethod(
            TextColumnFeatureFilter.ParsingMethod.valueOf(str.toUpperCase()));
      }
      else if (filter.getFilterType().equals(ColumnFeatureFilter.FilterType.NUMERIC))
      {
        NumericColumnFeatureFilter textFilter = (NumericColumnFeatureFilter)filter;
        str = parseChildString(children.get(4), "numeric filter parsing method", parser);
        if (StringUtils.isEmpty(str))
        {
          throw new IllegalArgumentException("Empty numeric filter parsing method");
        }
        textFilter.setComparisonMethod(
            NumericColumnFeatureFilter.ComparisonMethod.valueOf(str.toUpperCase()));
      }
      else
      {
        DateColumnFeatureFilter dateFilter = (DateColumnFeatureFilter)filter;
        str = parseChildString(children.get(4), "date format parsing method", parser);
        if (StringUtils.isEmpty(str))
        {
          throw new IllegalArgumentException("Empty date format parsing method");
        }
        dateFilter.setDateFormat(str);
      }
    }
    else if (children.size() == 6)
    {
      if (!filter.getFilterType().equals(ColumnFeatureFilter.FilterType.DATE))
      {
        throw new IllegalArgumentException("Incorrect number of arguments: " + 
          String.valueOf(children.size()) + " for filter type: " + 
          filter.getFilterType().toString());
      }
      DateColumnFeatureFilter dateFilter = (DateColumnFeatureFilter)filter;
      str = parseChildString(children.get(4), "date format parsing method", parser);
      if (StringUtils.isEmpty(str))
      {
        throw new IllegalArgumentException("Empty date format parsing method");
      }
      dateFilter.setDateFormat(str);
      
      str = parseChildString(children.get(5), "date filter granularity", parser);
      if (StringUtils.isEmpty(str))
      {
        throw new IllegalArgumentException("Empty date filter granularity method");
      }
      dateFilter.setDateFilterGranularity(
          DateColumnFeatureFilter.DateGranularity.valueOf(str.toUpperCase()));
    }
    return result;
  }
  
  public static ColumnFeatureFilter featureFilterForFilterType(
    ColumnFeatureFilter.FilterType filterType)
  {
    if (filterType == null)
    {
      throw new IllegalArgumentException("Invalid column filter type.");
    }
    if (filterType.equals(ColumnFeatureFilter.FilterType.DATE))
    {
      return new DateColumnFeatureFilter();
    }
    else if (filterType.equals(ColumnFeatureFilter.FilterType.NUMERIC))
    {
      return new NumericColumnFeatureFilter();
    }
    else if (filterType.equals(ColumnFeatureFilter.FilterType.TEXT))
    {
      return new TextColumnFeatureFilter();
    }
    throw new IllegalArgumentException("Invalid column filter type.");
  }
  
  @Override
  public String toString()
  {
    assert(filter != null);
    String msg = 
      String.format("FilterByColumnMapOp: input: %s column filter: %s, value: %s, " +
        "filter type: %s",
        _outputName == null ? "null" : _outputName.toString(),
        filter.getFilterColumn(), filter.getFilterValue(), filter.getFilterType());
    if (filter instanceof TextColumnFeatureFilter)
    {
      msg += " parse type: " + ((TextColumnFeatureFilter)filter).getParsingMethod().toString();
    }
    else if (filter instanceof NumericColumnFeatureFilter)
    {
      msg += " comparison type: " + 
        ((NumericColumnFeatureFilter)filter).getComparisonMethod().toString();
    }
    else if (filter instanceof DateColumnFeatureFilter)
    {
      DateColumnFeatureFilter dateFilter = (DateColumnFeatureFilter)filter;
      msg += " date format: " + dateFilter.getDateFormat();
      msg += " date filter granularity: " + 
        dateFilter.getDateFilterGranularity().toString();
    }
    return msg;
  }
}
