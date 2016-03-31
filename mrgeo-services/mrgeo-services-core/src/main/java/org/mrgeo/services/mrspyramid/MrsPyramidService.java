/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.services.mrspyramid;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.mrgeo.colorscale.ColorScale;
import org.mrgeo.colorscale.ColorScaleManager;
import org.mrgeo.colorscale.applier.ColorScaleApplier;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.image.MrsPyramid;
import org.mrgeo.mapreduce.job.JobManager;
import org.mrgeo.services.SecurityUtils;
import org.mrgeo.services.mrspyramid.rendering.ImageHandlerFactory;
import org.mrgeo.services.mrspyramid.rendering.ImageRenderer;
import org.mrgeo.services.mrspyramid.rendering.ImageResponseWriter;
import org.mrgeo.services.mrspyramid.rendering.KmlResponseBuilder;
import org.mrgeo.utils.Bounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.activation.MimetypesFileTypeMap;
import javax.ws.rs.core.Response;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author Steve Ingram
 *         Date: 10/26/13
 */
@SuppressWarnings("static-method")
public class MrsPyramidService {

    private static final Logger log = LoggerFactory.getLogger(MrsPyramidService.class);
    private static final MimetypesFileTypeMap mimeTypeMap = new MimetypesFileTypeMap();

    private JobManager jobManager = JobManager.getInstance();
    private Properties config;

    public MrsPyramidService(Properties configuration) {
        config = configuration;

    }

    public JobManager getJobManager() { return jobManager; }

    public ColorScale getColorScaleFromName(String colorScaleName) throws Exception {
        return ColorScaleManager.fromName(colorScaleName, config);
      }

    public ColorScale getColorScaleFromJSON(String colorScaleJSON) throws Exception {
      return ColorScaleManager.fromJSON(colorScaleJSON);
    }

    public List<String> getColorScales() throws Exception
    {
      ColorScale[] colorScales = ColorScaleManager.getColorScaleList();
      List<String> result = new ArrayList<String>(colorScales.length);
      for (ColorScale cs : colorScales)
      {
        result.add(cs.getName());
      }
      return result;
    }

    public Raster createColorScaleSwatch(String name, String format, int width, int height) throws Exception
    {
        ColorScale cs = getColorScaleFromName(name);
        return createColorScaleSwatch(cs, format, width, height);
    }

    public Raster createColorScaleSwatch(ColorScale cs, String format, int width, int height) throws Exception
    {
        double[] extrema = {0, 0};

        WritableRaster wr = RasterUtils.createEmptyRaster(width, height, 1, DataBuffer.TYPE_FLOAT);

        if (width > height) {
            extrema[1] = width-1;
            for (int w=0; w < width; w++) {
                for (int h=0; h < height; h++) {
                    wr.setSample(w, h, 0, w);
                }
            }
        } else {
            extrema[1] = height-1;
            for (int h=0; h < height; h++) {
                for (int w=0; w < width; w++) {
                    wr.setSample(w, h, 0, extrema[1] - h);
                }
            }
        }

      ColorScaleApplier applier = (ColorScaleApplier)ImageHandlerFactory.getHandler(format, ColorScaleApplier.class);

      return applier.applyColorScale(wr, cs, extrema, new double[]{-9999,0});
    }

    public boolean isZoomLevelValid(String pyramid,
                                    ProviderProperties providerProperties,
                                    int zoomLevel) throws IOException
    {
        MrsPyramid mp = getPyramid(pyramid, providerProperties);
        return (mp.getMetadata().getName(zoomLevel) != null);
    }

    public ImageRenderer getImageRenderer(String format) throws Exception {
        return (ImageRenderer)ImageHandlerFactory.getHandler(format, ImageRenderer.class);
    }

    public Raster applyColorScaleToImage(String format, Raster result, ColorScale cs, ImageRenderer renderer, double[] extrema) throws Exception {
       ColorScaleApplier applier = (ColorScaleApplier)ImageHandlerFactory.getHandler(format,
               ColorScaleApplier.class);

        return applier.applyColorScale(result, cs, extrema, renderer.getDefaultValues());
    }

    public ImageResponseWriter getImageResponseWriter(String format) throws Exception {
        return (ImageResponseWriter)ImageHandlerFactory.getHandler(format, ImageResponseWriter.class);
    }

    public Response renderKml(String pyramidPathStr, Bounds bounds, int width, int height, ColorScale cs,
        int zoomLevel, final ProviderProperties providerProperties)
    {
        String baseUrl = config.getProperty("base.url");
        KmlResponseBuilder kmlGenerator = new KmlResponseBuilder();
        return kmlGenerator.getResponse(
                pyramidPathStr, bounds, width, height, cs, baseUrl, zoomLevel,
                providerProperties);
    }

    public Properties getConfig() { return config; }

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

    public byte[] getEmptyTile(int width, int height, String format) throws Exception
    {
      //return an empty image

      ImageResponseWriter writer = getImageResponseWriter(format);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();

      int bands;
      Double[] nodatas;
      if (format.equalsIgnoreCase("jpg") || format.equalsIgnoreCase("jpeg") )
      {
        bands = 3;
        nodatas = new Double[]{0.0,0.0,0.0};
      } else
      {
        bands = 4;
        nodatas = new Double[]{0.0,0.0,0.0,0.0};
      }

      Raster raster = RasterUtils.createEmptyRaster(width, height, bands, DataBuffer.TYPE_BYTE, nodatas);
      writer.writeToStream(raster, ArrayUtils.toPrimitive(nodatas), baos);
      byte[] imageData = baos.toByteArray();
      IOUtils.closeQuietly(baos);
      return imageData;

    }

    public String getContentType(String format) {
        return mimeTypeMap.getContentType("output." + format);
    }

    /**
     * Get Pyramid metadata from a String imgName
     *
     * @param imgName pyramid name to retrieve metadata for
     * @return String metadata for pyramid
     * @throws IOException
     */
    public String getMetadata(String imgName) throws IOException
    {
      MrsPyramid pyramid = MrsPyramid.open(imgName,
                                           SecurityUtils.getProviderProperties());
      ObjectMapper mapper = new ObjectMapper();
      return mapper.writeValueAsString(pyramid.getMetadata());
    }

    public MrsPyramid getPyramid(String name,
                                 ProviderProperties providerProperties) throws IOException
    {
        return MrsPyramid.open(name, providerProperties);
    }

    public String formatValue(Double value, String units) {
        String formatted = String.valueOf(value) + units;
        if (units.equalsIgnoreCase("seconds")) {
            formatted = formatElapsedTime(value);
        } else if (units.equalsIgnoreCase("meters")) {
            formatted = Math.round(value) + "m";
        } else if (units.equalsIgnoreCase("degrees")) {
            formatted = Math.round(value) + "deg";
        } else if (units.equalsIgnoreCase("percent")) {
            formatted = Math.round(value*100) + "%";
        }
        return formatted;
    }

    // Function to format the elapsed time (in sec) of the completed WPS job
    public String formatElapsedTime(Double elapsedTime) {
        List<String> output = new ArrayList<String>();
        int secPerMinute = 60;
        int minPerHour = 60;
        int hourPerDay = 24;

        if (elapsedTime == null) {
            elapsedTime = 0d;
        }
        if (elapsedTime == 0) {
            return("0s");
        }

        double seconds = elapsedTime%secPerMinute;
        if (seconds > 0) output.add(0,(int)seconds + "s");
        elapsedTime = Math.floor(elapsedTime/secPerMinute);
        double minutes = elapsedTime%minPerHour;
        if (minutes > 0) output.add(0,(int)minutes + "m");
        elapsedTime = Math.floor(elapsedTime/minPerHour);
        double hours = elapsedTime%hourPerDay;
        if (hours > 0) output.add(0,(int)hours + "h");
        double days = Math.floor(elapsedTime/hourPerDay);
        if (days > 0) output.add(0,(int)days + "d");

        return StringUtils.join(output, ":");
    }

    /**
     * Ingest raster stream in-memory
     *
     * @param input stream containing the raster to ingest
     * @param output pyramid name ingest raster to
     * @return Boolean success status of ingest
     * @throws IOException
     */
    public String ingestImage(InputStream input, String output,
                              String protectionLevel,
                              ProviderProperties providerProperties) throws Exception {
//        try {
//            String pyramidOutput = HadoopUtils.getDefaultImageBaseDirectory() + output;
//            byte[] bytes = IOUtils.toByteArray(input);
//            ByteArraySeekableStream seekableInput = new ByteArraySeekableStream(bytes);
//            IngestImage.quickIngest(seekableInput, pyramidOutput, false, null,
//                false, protectionLevel, 0d);
//          BuildPyramid.build(pyramidOutput, new MeanAggregator(),
//                HadoopUtils.createConfiguration(), providerProperties);
//            return pyramidOutput;
//        } catch (Exception e) {
            log.error("Ingest image to " + output + " failed.");//e);
            return null;
//        }
    }
}
