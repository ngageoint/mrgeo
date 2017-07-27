/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.services.mrspyramid;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.mrgeo.colorscale.ColorScale;
import org.mrgeo.colorscale.ColorScaleManager;
import org.mrgeo.colorscale.applier.ColorScaleApplier;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.image.MrsPyramid;
import org.mrgeo.job.JobManager;
import org.mrgeo.services.SecurityUtils;
import org.mrgeo.services.mrspyramid.rendering.ImageHandlerFactory;
import org.mrgeo.services.mrspyramid.rendering.ImageRenderer;
import org.mrgeo.services.mrspyramid.rendering.ImageResponseWriter;
import org.mrgeo.services.mrspyramid.rendering.KmlResponseBuilder;
import org.mrgeo.utils.tms.Bounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.activation.MimetypesFileTypeMap;
import javax.inject.Singleton;
import javax.ws.rs.core.Response;
import java.awt.image.DataBuffer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@SuppressWarnings("static-method")
@Singleton
public class MrsPyramidService
{

private static final Logger log = LoggerFactory.getLogger(MrsPyramidService.class);
private static final MimetypesFileTypeMap mimeTypeMap = new MimetypesFileTypeMap();

private JobManager jobManager = JobManager.getInstance();
private Properties config;

public MrsPyramidService(Properties configuration)
{
  config = configuration;
}

public JobManager getJobManager()
{
  return jobManager;
}

public ColorScale getColorScaleFromName(String colorScaleName) throws MrsPyramidServiceException
{
  try
  {
    return ColorScaleManager.fromName(colorScaleName);
  }
  catch (ColorScale.ColorScaleException e)
  {
    throw new MrsPyramidServiceException(e);
  }
}

public ColorScale getColorScaleFromJSON(String colorScaleJSON) throws MrsPyramidServiceException
{
  try
  {
    return ColorScaleManager.fromJSON(colorScaleJSON);
  }
  catch (ColorScale.ColorScaleException e)
  {
    throw new MrsPyramidServiceException(e);
  }

}

public List<String> getColorScales() throws MrsPyramidServiceException
{
  try
  {
    ColorScale[] colorScales = ColorScaleManager.getColorScaleList();
    List<String> result = new ArrayList<>(colorScales.length);
    for (ColorScale cs : colorScales)
    {
      result.add(cs.getName());
    }
    return result;
  }
  catch (IOException e)
  {
    throw new MrsPyramidServiceException(e);
  }
}

public MrGeoRaster createColorScaleSwatch(String name, String format, int width, int height)
    throws MrsPyramidServiceException
{
  try
  {
    ColorScale cs = getColorScaleFromName(name);
    return createColorScaleSwatch(cs, format, width, height);
  }
  catch (Exception e)
  {
    log.error("Exception thrown", e);
    throw new MrsPyramidServiceException("Error creating color scale " + name, e);
  }
}

public MrGeoRaster createColorScaleSwatch(ColorScale cs, String format, int width, int height)
    throws MrsPyramidServiceException
{
  double[] extrema = {0, 0};

  try
  {
    MrGeoRaster wr = MrGeoRaster.createEmptyRaster(width, height, 1, DataBuffer.TYPE_FLOAT);

    if (width > height)
    {
      extrema[1] = width - 1.0;
      for (int w = 0; w < width; w++)
      {
        for (int h = 0; h < height; h++)
        {
          wr.setPixel(w, h, 0, w);
        }
      }
    }
    else
    {
      extrema[1] = height - 1.0;
      for (int h = 0; h < height; h++)
      {
        for (int w = 0; w < width; w++)
        {
          wr.setPixel(w, h, 0, extrema[1] - h);
        }
      }
    }

    ColorScaleApplier applier = (ColorScaleApplier) ImageHandlerFactory.getHandler(format, ColorScaleApplier.class);

    return applier.applyColorScale(wr, cs, extrema, new double[]{-9999, 0},null);
  }
  catch (IllegalAccessException | InstantiationException | ColorScale.ColorScaleException | MrGeoRaster.MrGeoRasterException e)
  {
    throw new MrsPyramidServiceException("Error creating color scale swatch", e);
  }
}

public boolean isZoomLevelValid(String pyramid, ProviderProperties providerProperties, int zoomLevel)
    throws MrsPyramidServiceException
{
  try
  {
    MrsPyramid mp = getPyramid(pyramid, providerProperties);
    return (mp.getMetadata().getName(zoomLevel) != null);
  }
  catch (IOException e)
  {
    throw new MrsPyramidServiceException(e);
  }

}

public ImageRenderer getImageRenderer(String format) throws MrsPyramidServiceException
{
  try
  {
    return (ImageRenderer) ImageHandlerFactory.getHandler(format, ImageRenderer.class);
  }
  catch (Exception e)
  {
    throw new MrsPyramidServiceException(e);
  }
}

public MrGeoRaster applyColorScaleToImage(String format, MrGeoRaster result, ColorScale cs,
    ImageRenderer renderer, double[] extrema) throws MrsPyramidServiceException
{
  try
  {
    ColorScaleApplier applier = (ColorScaleApplier) ImageHandlerFactory.getHandler(format,
        ColorScaleApplier.class);

    return applier.applyColorScale(result, cs, extrema, renderer.getDefaultValues(),
            renderer.getQuantiles());
  }
  catch (ColorScale.ColorScaleException | InstantiationException | IllegalAccessException e)
  {
    throw new MrsPyramidServiceException(e);
  }
}

public ImageResponseWriter getImageResponseWriter(String format) throws MrsPyramidServiceException
{
  try
  {
    return (ImageResponseWriter) ImageHandlerFactory.getHandler(format, ImageResponseWriter.class);
  }
  catch (Exception e)
  {
    throw new MrsPyramidServiceException(e);
  }
}

//public Response renderKml(String pyramidPathStr, Bounds bounds, int width, int height, ColorScale cs,
//    int zoomLevel, ProviderProperties providerProperties)
//{
//  String baseUrl = config.getProperty("base.url");
//  KmlResponseBuilder kmlGenerator = new KmlResponseBuilder();
//  return kmlGenerator.getResponse(
//      pyramidPathStr, bounds, width, height, cs, baseUrl, zoomLevel,
//      providerProperties);
//}

public Properties getConfig()
{
  return config;
}

//    private ImageWriter getWriterFor(String format) throws IOException {
//        if ( StringUtils.isNotEmpty(format) ) {
//            if ( format.equalsIgnoreCase("JPG") || format.equalsIgnoreCase("JPEG") )
//                return ImageUtils.createImageWriter("image/jpeg");
//            else if ( format.equalsIgnoreCase("PNG") )
//                return ImageUtils.createImageWriter("image/png");
//            else if ( format.equalsIgnoreCase("TIFF"))
//                return ImageUtils.createImageWriter("image/tiff");
//        }
//        throw new IOException("No writer found for format [" + format + "]");
//    }

public byte[] getEmptyTile(int width, int height, String format) throws MrsPyramidServiceException
{
  //return an empty image

  try
  {
    ImageResponseWriter writer = getImageResponseWriter(format);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    int bands;
    double[] nodatas;
    if (format.equalsIgnoreCase("jpg") || format.equalsIgnoreCase("jpeg"))
    {
      bands = 3;
      nodatas = new double[]{0.0, 0.0, 0.0};
    }
    else
    {
      bands = 4;
      nodatas = new double[]{0.0, 0.0, 0.0, 0.0};
    }

    MrGeoRaster raster = MrGeoRaster.createEmptyRaster(width, height, bands, DataBuffer.TYPE_BYTE, nodatas);
    writer.writeToStream(raster, nodatas, baos);
    byte[] imageData = baos.toByteArray();
    IOUtils.closeQuietly(baos);
    return imageData;
  }
  catch (MrsPyramidServiceException | IOException e)
  {
    throw new MrsPyramidServiceException(e);
  }

}

public String getContentType(String format)
{
  return mimeTypeMap.getContentType("output." + format);
}

/**
 * Get Pyramid metadata from a String imgName
 *
 * @param imgName pyramid name to retrieve metadata for
 * @return String metadata for pyramid
 */
public String getMetadata(String imgName) throws MrsPyramidServiceException
{
  try
  {
    MrsPyramid pyramid = MrsPyramid.open(imgName,
        SecurityUtils.getProviderProperties());
    ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(pyramid.getMetadata());
  }
  catch (IOException e)
  {
    throw new MrsPyramidServiceException(e);
  }
}

public MrsPyramid getPyramid(String name,
    ProviderProperties providerProperties) throws MrsPyramidServiceException
{
  try
  {
    return MrsPyramid.open(name, providerProperties);
  }
  catch (IOException e)
  {
    throw new MrsPyramidServiceException(e);
  }
}

public String formatValue(Double value, String units)
{
  String formatted = String.valueOf(value) + units;
  if (units.equalsIgnoreCase("seconds"))
  {
    formatted = formatElapsedTime(value);
  }
  else if (units.equalsIgnoreCase("meters"))
  {
    formatted = Math.round(value) + "m";
  }
  else if (units.equalsIgnoreCase("degrees"))
  {
    formatted = Math.round(value) + "deg";
  }
  else if (units.equalsIgnoreCase("percent"))
  {
    formatted = Math.round(value * 100) + "%";
  }
  return formatted;
}

// Function to format the elapsed time (in sec) of the completed WPS job
public String formatElapsedTime(Double elapsedTime)
{
  List<String> output = new ArrayList<>();
  int secPerMinute = 60;
  int minPerHour = 60;
  int hourPerDay = 24;

  if (elapsedTime == null)
  {
    elapsedTime = 0d;
  }
  if (elapsedTime == 0)
  {
    return ("0s");
  }

  double seconds = elapsedTime % secPerMinute;
  if (seconds > 0)
  {
    output.add(0, (int) seconds + "s");
  }
  elapsedTime = Math.floor(elapsedTime / secPerMinute);
  double minutes = elapsedTime % minPerHour;
  if (minutes > 0)
  {
    output.add(0, (int) minutes + "m");
  }
  elapsedTime = Math.floor(elapsedTime / minPerHour);
  double hours = elapsedTime % hourPerDay;
  if (hours > 0)
  {
    output.add(0, (int) hours + "h");
  }
  double days = Math.floor(elapsedTime / hourPerDay);
  if (days > 0)
  {
    output.add(0, (int) days + "d");
  }

  return StringUtils.join(output, ":");
}

}
