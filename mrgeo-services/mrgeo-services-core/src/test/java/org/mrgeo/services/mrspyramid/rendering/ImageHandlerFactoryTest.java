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

package org.mrgeo.services.mrspyramid.rendering;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.colorscale.applier.ColorScaleApplier;
import org.mrgeo.colorscale.applier.JpegColorScaleApplier;
import org.mrgeo.colorscale.applier.PngColorScaleApplier;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.LocalRunnerTest;

import static org.junit.Assert.*;

@SuppressWarnings("all") // Test code, not included in production
public class ImageHandlerFactoryTest extends LocalRunnerTest
{
@BeforeClass
public static void setUp()
{
}

@Test
@Category(UnitTest.class)
public void testStandardFormats() throws Exception
{
  ImageRenderer renderer =
      (ImageRenderer) ImageHandlerFactory.getHandler(
          "image/png", ImageRenderer.class);
  assertTrue(renderer instanceof PngImageRenderer);
  assertTrue(renderer.getMimeTypes()[0].equals("image/png"));
  renderer =
      (ImageRenderer) ImageHandlerFactory.getHandler(
          "image/jpeg", ImageRenderer.class);
  assertTrue(renderer instanceof JpegImageRenderer);
  assertTrue(renderer.getMimeTypes()[0].equals("image/jpeg"));
  renderer =
      (ImageRenderer) ImageHandlerFactory.getHandler(
          "image/tiff", ImageRenderer.class);
  assertTrue(renderer instanceof TiffImageRenderer);
  assertTrue(renderer.getMimeTypes()[0].equals("image/tiff"));

  ColorScaleApplier applier =
      (ColorScaleApplier) ImageHandlerFactory.getHandler("image/png", ColorScaleApplier.class);
  assertTrue(applier instanceof PngColorScaleApplier);
  assertTrue(applier.getMimeTypes()[0].equals("image/png"));
  applier =
      (ColorScaleApplier) ImageHandlerFactory.getHandler("image/jpeg", ColorScaleApplier.class);
  assertTrue(applier instanceof JpegColorScaleApplier);
  assertTrue(applier.getMimeTypes()[0].equals("image/jpeg"));

  ImageResponseWriter writer =
      (ImageResponseWriter) ImageHandlerFactory.getHandler("image/png", ImageResponseWriter.class);
  assertTrue(writer instanceof PngImageResponseWriter);
  assertTrue(writer.getMimeTypes()[0].equals("image/png"));
  writer =
      (ImageResponseWriter) ImageHandlerFactory.getHandler("image/jpeg", ImageResponseWriter.class);
  assertTrue(writer instanceof JpegImageResponseWriter);
  assertTrue(writer.getMimeTypes()[0].equals("image/jpeg"));
  writer =
      (ImageResponseWriter) ImageHandlerFactory.getHandler("image/tiff", ImageResponseWriter.class);
  assertTrue(writer instanceof TiffImageResponseWriter);
  assertTrue(writer.getMimeTypes()[0].equals("image/tiff"));
}

@Test
@Category(UnitTest.class)
public void testFormatVariants() throws Exception
{
  //not testing all of the possible combination of image format here...

  ImageRenderer renderer =
      (ImageRenderer) ImageHandlerFactory.getHandler("png", ImageRenderer.class);
  assertTrue(renderer instanceof PngImageRenderer);
  assertTrue(renderer.getMimeTypes()[0].equals("image/png"));
  renderer =
      (ImageRenderer) ImageHandlerFactory.getHandler("PNG", ImageRenderer.class);
  assertTrue(renderer instanceof PngImageRenderer);
  assertTrue(renderer.getMimeTypes()[0].equals("image/png"));
  renderer =
      (ImageRenderer) ImageHandlerFactory.getHandler("jpg", ImageRenderer.class);
  assertTrue(renderer instanceof JpegImageRenderer);
  assertTrue(renderer.getMimeTypes()[0].equals("image/jpeg"));
  renderer =
      (ImageRenderer) ImageHandlerFactory.getHandler("JPG", ImageRenderer.class);
  assertTrue(renderer instanceof JpegImageRenderer);
  assertTrue(renderer.getMimeTypes()[0].equals("image/jpeg"));
  renderer =
      (ImageRenderer) ImageHandlerFactory.getHandler("jpeg", ImageRenderer.class);
  assertTrue(renderer instanceof JpegImageRenderer);
  assertTrue(renderer.getMimeTypes()[0].equals("image/jpeg"));
  renderer =
      (ImageRenderer) ImageHandlerFactory.getHandler("JPEG", ImageRenderer.class);
  assertTrue(renderer instanceof JpegImageRenderer);
  assertTrue(renderer.getMimeTypes()[0].equals("image/jpeg"));
  renderer =
      (ImageRenderer) ImageHandlerFactory.getHandler("tif", ImageRenderer.class);
  assertTrue(renderer instanceof TiffImageRenderer);
  assertTrue(renderer.getMimeTypes()[0].equals("image/tiff"));
  renderer =
      (ImageRenderer) ImageHandlerFactory.getHandler("TIF", ImageRenderer.class);
  assertTrue(renderer instanceof TiffImageRenderer);
  assertTrue(renderer.getMimeTypes()[0].equals("image/tiff"));
  renderer =
      (ImageRenderer) ImageHandlerFactory.getHandler("tiff", ImageRenderer.class);
  assertTrue(renderer instanceof TiffImageRenderer);
  assertTrue(renderer.getMimeTypes()[0].equals("image/tiff"));
  renderer =
      (ImageRenderer) ImageHandlerFactory.getHandler("TIFF", ImageRenderer.class);
  assertTrue(renderer instanceof TiffImageRenderer);
  assertTrue(renderer.getMimeTypes()[0].equals("image/tiff"));

  ColorScaleApplier applier =
      (ColorScaleApplier) ImageHandlerFactory.getHandler("png", ColorScaleApplier.class);
  assertTrue(applier instanceof PngColorScaleApplier);
  assertTrue(applier.getMimeTypes()[0].equals("image/png"));
  applier =
      (ColorScaleApplier) ImageHandlerFactory.getHandler("PNG", ColorScaleApplier.class);
  assertTrue(applier instanceof PngColorScaleApplier);
  assertTrue(applier.getMimeTypes()[0].equals("image/png"));
  applier =
      (ColorScaleApplier) ImageHandlerFactory.getHandler("jpg", ColorScaleApplier.class);
  assertTrue(applier instanceof JpegColorScaleApplier);
  assertTrue(applier.getMimeTypes()[0].equals("image/jpeg"));
  applier =
      (ColorScaleApplier) ImageHandlerFactory.getHandler("JPG", ColorScaleApplier.class);
  assertTrue(applier instanceof JpegColorScaleApplier);
  assertTrue(applier.getMimeTypes()[0].equals("image/jpeg"));
  applier =
      (ColorScaleApplier) ImageHandlerFactory.getHandler("jpeg", ColorScaleApplier.class);
  assertTrue(applier instanceof JpegColorScaleApplier);
  assertTrue(applier.getMimeTypes()[0].equals("image/jpeg"));
  applier =
      (ColorScaleApplier) ImageHandlerFactory.getHandler("JPEG", ColorScaleApplier.class);
  assertTrue(applier instanceof JpegColorScaleApplier);
  assertTrue(applier.getMimeTypes()[0].equals("image/jpeg"));

  ImageResponseWriter writer =
      (ImageResponseWriter) ImageHandlerFactory.getHandler("png", ImageResponseWriter.class);
  assertTrue(writer instanceof PngImageResponseWriter);
  assertTrue(writer.getMimeTypes()[0].equals("image/png"));
  writer =
      (ImageResponseWriter) ImageHandlerFactory.getHandler("PNG", ImageResponseWriter.class);
  assertTrue(writer instanceof PngImageResponseWriter);
  assertTrue(writer.getMimeTypes()[0].equals("image/png"));
  writer =
      (ImageResponseWriter) ImageHandlerFactory.getHandler("jpg", ImageResponseWriter.class);
  assertTrue(writer instanceof JpegImageResponseWriter);
  assertTrue(writer.getMimeTypes()[0].equals("image/jpeg"));
  writer =
      (ImageResponseWriter) ImageHandlerFactory.getHandler("JPG", ImageResponseWriter.class);
  assertTrue(writer instanceof JpegImageResponseWriter);
  assertTrue(writer.getMimeTypes()[0].equals("image/jpeg"));
  writer =
      (ImageResponseWriter) ImageHandlerFactory.getHandler("jpeg", ImageResponseWriter.class);
  assertTrue(writer instanceof JpegImageResponseWriter);
  assertTrue(writer.getMimeTypes()[0].equals("image/jpeg"));
  writer =
      (ImageResponseWriter) ImageHandlerFactory.getHandler("JPEG", ImageResponseWriter.class);
  assertTrue(writer instanceof JpegImageResponseWriter);
  assertTrue(writer.getMimeTypes()[0].equals("image/jpeg"));
  writer =
      (ImageResponseWriter) ImageHandlerFactory.getHandler("tif", ImageResponseWriter.class);
  assertTrue(writer instanceof TiffImageResponseWriter);
  assertTrue(writer.getMimeTypes()[0].equals("image/tiff"));
  writer =
      (ImageResponseWriter) ImageHandlerFactory.getHandler("TIF", ImageResponseWriter.class);
  assertTrue(writer instanceof TiffImageResponseWriter);
  assertTrue(writer.getMimeTypes()[0].equals("image/tiff"));
  writer =
      (ImageResponseWriter) ImageHandlerFactory.getHandler("tiff", ImageResponseWriter.class);
  assertTrue(writer instanceof TiffImageResponseWriter);
  assertTrue(writer.getMimeTypes()[0].equals("image/tiff"));
  writer =
      (ImageResponseWriter) ImageHandlerFactory.getHandler("TIFF", ImageResponseWriter.class);
  assertTrue(writer instanceof TiffImageResponseWriter);
  assertTrue(writer.getMimeTypes()[0].equals("image/tiff"));
}

@Test
@Category(UnitTest.class)
public void testNullFormat() throws Exception
{
  try
  {
    ImageHandlerFactory.getHandler(null, ImageRenderer.class);
    assertFalse(false);
  }
  catch (Exception e)
  {
    assertTrue(e.getMessage().contains("NULL image format"));
  }

  try
  {
    ImageHandlerFactory.getHandler("image/png", null);
    assertFalse(false);
  }
  catch (Exception e)
  {
    assertTrue(e.getMessage().contains("NULL handler type"));
  }
}

@Test
@Category(UnitTest.class)
public void testBadFormat() throws Exception
{
  try
  {
    ImageHandlerFactory.getHandler("abc", ImageRenderer.class);
    assertFalse(false);
  }
  catch (Exception e)
  {
    assertEquals(e.getMessage(), "Unsupported image format - abc");
  }

  //color scales aren't applied to tifs
  try
  {
    ImageHandlerFactory.getHandler("tif", ColorScaleApplier.class);
    assertFalse(false);
  }
  catch (Exception e)
  {
    assertEquals(e.getMessage(), "Unsupported image format - tif");
  }
  try
  {
    ImageHandlerFactory.getHandler("tiff", ColorScaleApplier.class);
    assertFalse(false);
  }
  catch (Exception e)
  {
    assertEquals(e.getMessage(), "Unsupported image format - tiff");
  }
}
}
