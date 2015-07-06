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

package org.mrgeo.utils;

import org.mrgeo.rasterops.ColorScale;
import org.mrgeo.rasterops.ColorScaleOpImage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.ImageTypeSpecifier;
import javax.imageio.ImageWriter;
import javax.imageio.stream.FileImageOutputStream;
import javax.imageio.stream.ImageOutputStream;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import javax.media.jai.BorderExtender;
import javax.media.jai.PlanarImage;
import javax.media.jai.operator.BandSelectDescriptor;
import java.awt.*;
import java.awt.geom.AffineTransform;
import java.awt.image.*;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/**
 * Java imaging utilities
 */
public class ImageUtils
{
  private static final Logger log = LoggerFactory.getLogger(ImageUtils.class);

  /**
   * 
   * @param image
   * @return
   */
  public static Image transformGrayToTransparency(BufferedImage image)
  {
    RGBImageFilter filter = new RGBImageFilter()
    {
      @Override
      public final int filterRGB(int x, int y, int rgb)
      {
        return (rgb << 8) & 0xFF000000;
      }
    };
    ImageProducer ip = new FilteredImageSource(image.getSource(), filter);
    return Toolkit.getDefaultToolkit().createImage(ip);
  }

  /** 
   * Convert Image to BufferedImage. 
   * 
   * @param image Image to be converted to BufferedImage. 
   * @return BufferedImage corresponding to provided Image. 
   */  
  public static BufferedImage imageToBufferedImage(final Image image)  
  {  
    //    final BufferedImage bufferedImage =  
    //        new BufferedImage(image.getWidth(null), image.getHeight(null), BufferedImage.TYPE_INT_RGB);  
    final BufferedImage bufferedImage =  
        new BufferedImage(image.getWidth(null), image.getHeight(null), BufferedImage.TYPE_3BYTE_BGR);  
    final Graphics2D g2 = bufferedImage.createGraphics();  
    g2.drawImage(image, 0, 0, null);  
    g2.dispose();  
    return bufferedImage;  
  }  

  /** 
   * Convert Image to BufferedImage. 
   * 
   * @param image Image to be converted to BufferedImage. 
   * @return BufferedImage corresponding to provided Image. 
   */  
  public static BufferedImage imageToRgbaBufferedImage(final Image image)  
  {  
    //    final BufferedImage bufferedImage =  
    //        new BufferedImage(image.getWidth(null), image.getHeight(null), BufferedImage.TYPE_INT_ARGB);  

    final BufferedImage bufferedImage =  
        new BufferedImage(image.getWidth(null), image.getHeight(null), BufferedImage.TYPE_4BYTE_ABGR);  
    final Graphics2D g2 = bufferedImage.createGraphics();  
    g2.drawImage(image, 0, 0, null);  
    g2.dispose();  
    return bufferedImage;  
  }  

  /**
   * Returns a transparent image
   * @param width image width
   * @param height image height
   * @return buffered image
   */
  public static BufferedImage getTransparentImage(int width, int height)
  {
    return imageToBufferedImage(
        transformGrayToTransparency(new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY)));
  }

  /**
   * Writes an image and returns the bytes
   * @param image image to write
   * @param writer image writer
   * @return image bytes
   * @throws IOException
   */
  public static byte[] getImageBytes(RenderedImage image, ImageWriter writer) throws IOException
  {
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream(); 
    ImageOutputStream imageStream = new MemoryCacheImageOutputStream(byteStream);
    writer.setOutput(imageStream);
    writer.write(image);
    imageStream.close();
    return byteStream.toByteArray();
  }

  /**
   * 
   * @param image
   * @return rgba image
   */
  public static RenderedImage trimPng(RenderedImage image)
  {
    log.debug("Trimming PNG...");
    BufferedImage indexedImage = 
        new BufferedImage(image.getWidth(), image.getHeight(), BufferedImage.TYPE_4BYTE_ABGR);
    Graphics2D graphics = indexedImage.createGraphics();

    //There seems to be a case(s) where the drawRenderedImage isn't actually doing a getData() on 
    //the underlying Raster, which means the opchain isn't being executed.  The code below, 
    //ultimately calling graphics.drawImage() always makes sure the getdata() is used properly.
    //graphics.drawRenderedImage(image, new AffineTransform());

    DataBuffer buffer = image.getData().getDataBuffer();

    ColorModel cm = image.getColorModel();
    SampleModel sm = cm.createCompatibleSampleModel(image.getWidth(), image.getHeight());
    WritableRaster raster = Raster.createWritableRaster(sm, buffer, null);
    BufferedImage img = new BufferedImage(cm, raster, false, null);

    graphics.drawImage(img, 0, 0, null);

    log.debug("PNG trimmed.");
    return indexedImage;
  }

  /**
   * Writes an image to a file
   * @param file file to write the image to
   * @param image image to write
   * @throws IOException
   */
  public static void writeImageToFile(File file, byte[] image) throws IOException
  {
    ImageOutputStream imageStream = new FileImageOutputStream(file);
    imageStream.write(image, 0, image.length);
    imageStream.close();
  }

  /**
   * Writes an image to a file
   * @param file file to write the image to
   * @param writer image writer
   * @param image image to write
   * @throws IOException
   */
  public static void writeImageToFile(File file, ImageWriter writer, RenderedImage image)
      throws IOException
      {
    ImageOutputStream imageStream = new FileImageOutputStream(file);
    writer.setOutput(imageStream);
    writer.write(image);
    imageStream.close();
      }

  /**
   * Converts a rendered image to have RGB bands only
   * @param source image to convert
   * @return RGB image
   * @throws IOException
   */
  public static RenderedImage convertToRgb(RenderedImage image) throws IOException
  {
    log.debug("Converting to RGB...");

    ColorModel cm = image.getColorModel();

    ColorScale cs;
    if (cm.getNumComponents() == 1)
    {
      cs = ColorScale.createDefaultGrayScale();
    }
    else 
    {
      cs = ColorScale.createDefault();
    }

    RenderedImage result =  ColorScaleOpImage.create(image, cs);
    if (result.getColorModel().getNumComponents() >= 4)
    {
      log.debug("Removing alpha band...");
      int[] rgb = { 0, 1, 2 };
      result = BandSelectDescriptor.create(result, rgb, null);
      log.debug("Alpha band removed.");
    }
    log.debug("Converted to RGB.");

    return result;
  }

  /**
   * Computes the output of the <RenderedImage>
   * (generally an <OpImage> instance)
   * into a <BufferedImage> to improve performance
   * on the encoding step.
   * https://107.23.31.196/redmine/issues/2266
   * @param input The <RenderedImage> to compute 
   * @return The buffered output
   */
  public static BufferedImage bufferRenderedImage(RenderedImage input)
  {
    ImageTypeSpecifier imageTypeSpecifier = ImageTypeSpecifier.createFromRenderedImage(input);
    int imageType = imageTypeSpecifier.getBufferedImageType();
    //I don't like this hack
    if (imageType == BufferedImage.TYPE_CUSTOM)
    {
      imageType = BufferedImage.TYPE_4BYTE_ABGR;
    }
    BufferedImage output = new BufferedImage(input.getWidth(),
      input.getHeight(), imageType);
    Graphics2D g = output.createGraphics();
    g.drawRenderedImage(input, new AffineTransform());
    return output;
  }

  /** 
   * Make provided image transparent wherever color matches the provided color. 
   * 
   * @param im BufferedImage whose color will be made transparent. 
   * @param color Color in provided image which will be made transparent. 
   * @return Image with transparency applied. 
   */  
  public static Image makeColorTransparent(final BufferedImage im, final Color color)  
  {  
    final ImageFilter filter = new RGBImageFilter()  
    {  
      // the color we are looking for... Alpha bits are set to opaque  
      public int markerRGB = color.getRGB() | 0xFF000000;  

      @Override
      public final int filterRGB(final int x, final int y, final int rgb)  
      {  
        if ((rgb | 0xFF000000) == markerRGB)  
        {  
          // Mark the alpha bits as zero - transparent  
          return 0x00FFFFFF & rgb;  
        }
        // nothing to do  
        return rgb;  
      }  
    };  

    final ImageProducer ip = new FilteredImageSource(im.getSource(), filter);  
    return Toolkit.getDefaultToolkit().createImage(ip);  
  }

  public static Raster cutTile(PlanarImage image, long tx, long ty, long minTx, long maxTy, int tilesize, BorderExtender extender)
  {
    int dtx = (int) (tx - minTx);
    int dty = (int) (maxTy - ty);

    int x = dtx * tilesize;
    int y = dty * tilesize;

    Rectangle cropRect = new Rectangle(x, y, tilesize, tilesize);

    // crop, and fill the extra data with nodatas
    Raster cropped;
    if (extender != null)
    {
      cropped = image.getExtendedData(cropRect, extender).createTranslatedChild(0, 0);
    }
    else
    {
      cropped = image.getData(cropRect).createTranslatedChild(0, 0);
    }

    // The crop has the potential to make sample models sizes that aren't identical, to this will force them to all be the
    // same
    final SampleModel model = cropped.getSampleModel().createCompatibleSampleModel(tilesize, tilesize);

    WritableRaster tile = Raster.createWritableRaster(model, null);
    tile.setDataElements(0, 0, cropped);

    return tile;
  }

  /**
   * Returns a Java image IO reader for the the given MIME type
   * 
   * @param mimeType
   *          MIME type
   * @return an image reader
   * @throws IOException
   */
  public static ImageReader createImageReader(final String mimeType) throws IOException
  {
    final Iterator<ImageReader> it = ImageIO.getImageReadersByMIMEType(mimeType);
    if (it.hasNext())
    {
      final ImageReader reader = it.next();
      return reader;
    }
    throw new IOException("Error creating ImageReader for MIME type: " + mimeType);
  }

  /**
   * Returns a Java image IO writer for the the given MIME type
   * 
   * @param mimeType
   *          MIME type
   * @return an image writer
   * @throws IOException
   */
  public static ImageWriter createImageWriter(final String mimeType) throws IOException
  {
    final Iterator<ImageWriter> it = ImageIO.getImageWritersByMIMEType(mimeType);
    if (it.hasNext())
    {
      final ImageWriter writer = it.next();
      return writer;
    }
    throw new IOException("Error creating ImageWriter for MIME type: " + mimeType);
  }


}
