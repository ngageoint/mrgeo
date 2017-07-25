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

package org.mrgeo.services.mrspyramid.rendering;

import org.mrgeo.colorscale.applier.ColorScaleApplier;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Selects an image handler for images rendered by WMS requests (e.g. image renderer, color scale
 * applier, image response writer, etc.). Uses text matching between the requested image format and
 * class handler names to select the appropriate handler. There is no ImageHandler interface persay,
 * but if "handlers" follow a convention similar to what's described in the ImageRenderer interface,
 * they can be instantiated by this factory.
 */
public class ImageHandlerFactory
{
private static final Logger log = LoggerFactory.getLogger(ImageHandlerFactory.class);

// Became frustrated by the mime types not being picked up on deployed systems despite following
// the instructions in the documentation at:
// http://docs.oracle.com/javaee/5/api/javax/activation/MimetypesFileTypeMap.html
// Its currently far simpler to do some string comparisons to find the correct handler to use.
// If necessary, the former code used to convert the image format to a mime type can be revived
// from source history.
static Map<Class<?>, Map<String, Class<?>>> imageFormatHandlers = null;
static Map<Class<?>, Map<String, Class<?>>> mimeTypeHandlers = null;

static {
  loadHandlers();
}
/**
 * Returns a MrGeo WMS "image handler" for the requested image format
 */
//  public static Object getHandler(final String format, final Class<?> handlerType) throws Exception
//  {
//    return getHandler(format, handlerType);
//  }
@SuppressWarnings("unused")
public static Object getHandler(String imageFormat, Class<?> handlerType)
    throws IllegalAccessException, InstantiationException
{

  if (org.apache.commons.lang3.StringUtils.isEmpty(imageFormat))
  {
    throw new IllegalArgumentException("NULL image format requested.");
  }
  log.debug("Requested image format: {}", imageFormat);
  imageFormat = imageFormat.toLowerCase();

  if (handlerType == null)
  {
    throw new IllegalArgumentException("NULL handler type requested.");
  }
  log.debug("Requested handler type: {}", handlerType.getName());

  // first look in the mime types
  if (mimeTypeHandlers.containsKey(handlerType))
  {
    Map<String, Class<?>> handlers = mimeTypeHandlers.get(handlerType);

    if (handlers.containsKey(imageFormat))
    {
      Object cl;
      cl = handlers.get(imageFormat.toLowerCase()).newInstance();

      return cl;
    }
  }

  // now look in the formats
  if (imageFormatHandlers.containsKey(handlerType))
  {
    Map<String, Class<?>> handlers = imageFormatHandlers.get(handlerType);

    if (handlers.containsKey(imageFormat))
    {
      Object cl;
      cl = handlers.get(imageFormat.toLowerCase()).newInstance();

      return cl;
    }
  }

  throw new IllegalArgumentException("Unsupported image format - " + imageFormat);
}

public static String[] getImageFormats(Class<?> handlerType)
{

  if (imageFormatHandlers.containsKey(handlerType))
  {
    Map<String, Class<?>> handlers = imageFormatHandlers.get(handlerType);

    return handlers.keySet().toArray(new String[0]);
  }

  throw new IllegalArgumentException("Invalid handler type: " + handlerType.getCanonicalName() +
      ". Not supported.");
}

public static String[] getMimeFormats(Class<?> handlerType)
{

  if (mimeTypeHandlers.containsKey(handlerType))
  {
    Map<String, Class<?>> handlers = mimeTypeHandlers.get(handlerType);

    return handlers.keySet().toArray(new String[0]);
  }

  throw new IllegalArgumentException("Invalid handler type: " + handlerType.getCanonicalName() +
      ". Not supported.");
}

private static void addFormatHandlers(Map<String, Class<?>> handlers, Class<?> clazz)
{
  try
  {
    Object cl;
    cl = clazz.newInstance();

    Method method;
    method = clazz.getMethod("getWmsFormats");
    Object o = method.invoke(cl);

    String[] formats;
    if (o != null)
    {
      formats = (String[]) o;

      for (String format : formats)
      {
        handlers.put(format, clazz);
        log.info("      {}", format);
      }
    }
  }
  catch (InstantiationException ignored)
  {
  }
  catch (InvocationTargetException |
      IllegalArgumentException | NoSuchMethodException | SecurityException | IllegalAccessException e)
  {
    log.error("Exception thrown while processing class " + clazz.getName(), e);
  }
}

private static void addMimeHandlers(Map<String, Class<?>> handlers, Class<?> clazz)
{
  try
  {

    Object cl;
    cl = clazz.newInstance();

    Method method;
    method = clazz.getMethod("getMimeTypes");
    Object o = method.invoke(cl);

    String[] formats;
    if (o != null)
    {
      formats = (String[]) o;

      for (String format : formats)
      {
        handlers.put(format, clazz);
        log.info("      {}", format);
      }
    }
  }
  catch (InstantiationException | InvocationTargetException |
      IllegalArgumentException | NoSuchMethodException | SecurityException | IllegalAccessException e)
  {
    log.debug("Exception thrown", e);
  }
}

/**
 * Returns a MrGeo WMS "image handler" for the requested image format
 */

private static synchronized void loadHandlers()
{
  if (imageFormatHandlers == null)
  {
    imageFormatHandlers = new HashMap<>();
    mimeTypeHandlers = new HashMap<>();

    Reflections reflections = new Reflections("org.mrgeo"); // ClassUtils.getPackageName(ImageRenderer.class));

    // image format renderers
    mimeTypeHandlers.put(ImageRenderer.class, new HashMap<>());
    imageFormatHandlers.put(ImageRenderer.class, new HashMap<>());

    Map<String, Class<?>> mimeHandlers = mimeTypeHandlers.get(ImageRenderer.class);
    Map<String, Class<?>> formatHandlers = imageFormatHandlers.get(ImageRenderer.class);

    Set<Class<? extends ImageRenderer>> imageRenderers = reflections
        .getSubTypesOf(ImageRenderer.class);

    for (Class<? extends ImageRenderer> clazz : imageRenderers)
    {
      log.info("Registering Image Renderer: {}", clazz.getCanonicalName());

      log.info("  Mime Types");
      addMimeHandlers(mimeHandlers, clazz);

      log.info("  Format Strings");
      addFormatHandlers(formatHandlers, clazz);
    }

    //reflections = new Reflections(ClassUtils.getPackageName(ColorScaleApplier.class));

    // Color scale appliers
    mimeTypeHandlers.put(ColorScaleApplier.class, new HashMap<>());
    imageFormatHandlers.put(ColorScaleApplier.class, new HashMap<>());

    mimeHandlers = mimeTypeHandlers.get(ColorScaleApplier.class);
    formatHandlers = imageFormatHandlers.get(ColorScaleApplier.class);

    Set<Class<? extends ColorScaleApplier>> colorscaleappliers = reflections
        .getSubTypesOf(ColorScaleApplier.class);

    for (Class<? extends ColorScaleApplier> clazz : colorscaleappliers)
    {
      log.info("Registering Color Scale Applier: {}", clazz.getCanonicalName());
      log.info("  Mime Types");
      addMimeHandlers(mimeHandlers, clazz);

      log.info("  Format Strings");
      addFormatHandlers(formatHandlers, clazz);
    }

    //reflections = new Reflections(ClassUtils.getPackageName(ImageResponseWriter.class));

    // image response writers
    mimeTypeHandlers.put(ImageResponseWriter.class, new HashMap<>());
    imageFormatHandlers.put(ImageResponseWriter.class, new HashMap<>());

    mimeHandlers = mimeTypeHandlers.get(ImageResponseWriter.class);
    formatHandlers = imageFormatHandlers.get(ImageResponseWriter.class);
    Set<Class<? extends ImageResponseWriter>> imageresponsewriters = reflections
        .getSubTypesOf(ImageResponseWriter.class);

    for (Class<? extends ImageResponseWriter> clazz : imageresponsewriters)
    {
      log.info("Registering Image Response Writer: {}", clazz.getCanonicalName());
      log.info("  Mime Types");
      addMimeHandlers(mimeHandlers, clazz);

      log.info("  Format Strings");
      addFormatHandlers(formatHandlers, clazz);
    }
  }
}

}
