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

package org.mrgeo.data.accumulo.image;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.image.MrsImageInputFormatProvider;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.data.tile.TiledInputFormatContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

@Deprecated
public class AccumuloMrsImagePyramidInputFormatProvider extends MrsImageInputFormatProvider
{
  private static final Logger log = LoggerFactory.getLogger(AccumuloMrsImagePyramidInputFormatProvider.class);

  private ArrayList<Integer> zoomLevelsInPyramid;
  
  public AccumuloMrsImagePyramidInputFormatProvider(TiledInputFormatContext context){
    super(context);
  } // end constructor
  
  @Override
  public InputFormat<TileIdWritable, RasterWritable> getInputFormat(String input)
  {
    if(context.getBounds() == null){
      //return new AccumuloMrsImagePyramidInputFormat(input, context.getZoomLevel());
      return null;
    } else {
      return new AccumuloMrsImagePyramidInputFormat();
    }
    
  } // end getInputFormat

  @Override
  public void setupJob(Job job,
      final Properties providerProperties) throws DataProviderException{
      super.setupJob(job, providerProperties);

      zoomLevelsInPyramid = new ArrayList<Integer>();
      
      // get the input table
      for(final String input : context.getInputs()){
        MrsImagePyramid pyramid;
        try{
          //pyramid = MrsImagePyramid.open(input, (Properties)null);
          pyramid = MrsImagePyramid.open(input, providerProperties);
        } catch(IOException ioe){
          throw new DataProviderException("Failure opening input image pyramid: " + input, ioe);
        }

        try
        {
          final MrsImagePyramidMetadata metadata = pyramid.getMetadata();

          //ImageMetadata[] im = metadata.getImageMetadata();
          // im[0] - should be null
          //im[0].name; // string that is the zoom level

          log.debug("In setupJob(), loading pyramid for " + input +
              " pyramid instance is " + pyramid + " metadata instance is " + metadata);

          int z = context.getZoomLevel();
          // check to see if the zoom level is in the list of zoom levels
          if(! zoomLevelsInPyramid.contains(z)){
            z = metadata.getMaxZoomLevel();
          }
        }
        catch (IOException e)
        {
          throw new DataProviderException("Failure opening input image: " + input, e);
        }

      } // end for loop
    
    
  } // end setupJob

  
  
  
  @Override
  public void teardown(Job job) throws DataProviderException
  {
  } // end teardown
  
  
  
  
} // end AccumuloMrsImagePyramidInputFormatProvider
