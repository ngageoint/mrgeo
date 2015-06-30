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

package org.mrgeo.services.mrspyramid;

import com.sun.media.jai.codec.ByteArraySeekableStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.geotools.referencing.CRS;
import org.mrgeo.aggregators.MeanAggregator;
import org.mrgeo.buildpyramid.BuildPyramidSpark;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.image.geotools.GeotoolsRasterUtils;
import org.mrgeo.ingest.IngestImageSpark;
import org.mrgeo.mapreduce.job.JobManager;
import org.mrgeo.rasterops.ColorScale;
import org.mrgeo.services.SecurityUtils;
import org.mrgeo.services.mrspyramid.rendering.*;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.ImageUtils;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.activation.MimetypesFileTypeMap;
import javax.imageio.ImageWriter;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import javax.media.jai.FloatDoubleColorModel;
import javax.ws.rs.core.Response;
import java.awt.*;
import java.awt.color.ColorSpace;
import java.awt.image.BufferedImage;
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

    private CoordinateReferenceSystem coordSys;
    private JobManager jobManager = JobManager.getInstance();
    private Properties config;

    public MrsPyramidService(Properties configuration) {
        config = configuration;

        try {
            GeotoolsRasterUtils.addMissingEPSGCodes();
            coordSys = CRS.decode("EPSG:4326", true);
        } catch (NoSuchAuthorityCodeException e) {
            log.error("Unable to load CoordinateReferenceSystem", e);
        } catch (FactoryException e) {
            log.error("Unable to load CoordinateReferenceSystem", e);
        }
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

        FloatDoubleColorModel colorModel = new FloatDoubleColorModel(ColorSpace
                .getInstance(ColorSpace.CS_GRAY), false, false, Transparency.OPAQUE,
                DataBuffer.TYPE_FLOAT);
        WritableRaster wr = colorModel.createCompatibleWritableRaster(width, height);

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

      ColorScaleApplier applier = (ColorScaleApplier)ImageHandlerFactory.getHandler(format,
          ColorScaleApplier.class);

      return applier.applyColorScale(wr, cs, extrema, new double[]{-9999,0});
    }

    public boolean isZoomLevelValid(String pyramid,
        Properties providerProperties, int zoomLevel) throws IOException
    {
        MrsImagePyramid mp = getPyramid(pyramid, providerProperties);
        return (mp.getMetadata().getName(zoomLevel) != null);
    }

    public ImageRenderer getImageRenderer(String format) throws Exception {
        return (ImageRenderer)ImageHandlerFactory.getHandler(format, ImageRenderer.class,
                new Object[] { coordSys }, new Class<?>[] { CoordinateReferenceSystem.class });
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
        int zoomLevel, final Properties providerProperties)
    {
        String baseUrl = config.getProperty("base.url");
        KmlResponseBuilder kmlGenerator = new KmlResponseBuilder(coordSys);
        return kmlGenerator.getResponse(
                pyramidPathStr, bounds, width, height, cs, baseUrl, zoomLevel,
                providerProperties);
    }

    public Properties getConfig() { return config; }

    private ImageWriter getWriterFor(String format) throws IOException {
        if ( StringUtils.isNotEmpty(format) ) {
            if ( format.equalsIgnoreCase("JPG") || format.equalsIgnoreCase("JPEG") )
                return ImageUtils.createImageWriter("image/jpeg");
            else if ( format.equalsIgnoreCase("PNG") )
                return ImageUtils.createImageWriter("image/png");
            else if ( format.equalsIgnoreCase("TIFF"))
                return ImageUtils.createImageWriter("image/tiff");
        }
        throw new IOException("No writer found for format [" + format + "]");
    }

    public byte[] getEmptyTile(int width, int height, String format) throws IOException
    {
      //return an empty image

      ImageWriter writer = getWriterFor(format);

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      int dataType;
      if (format.equalsIgnoreCase("jpg") || format.equalsIgnoreCase("jpeg") ) {
        dataType = BufferedImage.TYPE_INT_RGB;
      } else {
        dataType = BufferedImage.TYPE_INT_ARGB;
      }
      BufferedImage bufImg = new BufferedImage(width, height, dataType);
      Graphics2D g = bufImg.createGraphics();
      g.setColor( new Color ( 0, 0, 0, 0 ));
      g.fillRect(0, 0, width, height);
      g.dispose();

      MemoryCacheImageOutputStream imageStream = new MemoryCacheImageOutputStream(baos);
      writer.setOutput(imageStream);
      writer.write(bufImg);
      imageStream.close();
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
      MrsImagePyramid pyramid = MrsImagePyramid.open(imgName,
          SecurityUtils.getProviderProperties());
      ObjectMapper mapper = new ObjectMapper();
      return mapper.writeValueAsString(pyramid.getMetadata());
    }

    public MrsImagePyramid getPyramid(String name,
        Properties providerProperties) throws IOException
    {
        return MrsImagePyramid.open(name, providerProperties);
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
        Properties providerProperties) throws Exception {
        try {
            String pyramidOutput = HadoopUtils.getDefaultImageBaseDirectory() + output;
            byte[] bytes = IOUtils.toByteArray(input);
            ByteArraySeekableStream seekableInput = new ByteArraySeekableStream(bytes);
            IngestImageSpark.quickIngest(seekableInput, pyramidOutput, false, null,
                false, protectionLevel, 0d);
          BuildPyramidSpark.build(pyramidOutput, new MeanAggregator(),
                HadoopUtils.createConfiguration(), providerProperties);
            return pyramidOutput;
        } catch (Exception e) {
            log.error("Ingest image to " + output + " failed.", e);
            return null;
        }
    }
}
