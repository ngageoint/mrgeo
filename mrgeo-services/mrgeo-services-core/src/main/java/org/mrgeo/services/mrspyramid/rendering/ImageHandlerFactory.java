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

package org.mrgeo.services.mrspyramid.rendering;

import org.apache.commons.lang.ClassUtils;
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

  /**
   * Returns a MrGeo WMS "image handler" for the requested image format
   * 
   * @param imageFormat
   *          mage format the requested image handler supports
   * @param handlerType
   *          a supported image handler type (see comments below to see how that is determined)
   * @return an object of specified type
   * @throws Exception
   */
  public static Object getHandler(final String format, final Class<?> handlerType) throws Exception
  {
    return getHandler(format, handlerType, null, null);
  }

  @SuppressWarnings("unused")
  public static Object getHandler(String imageFormat, final Class<?> handlerType,
    final Object[] constructorParams, final Class<?>[] constructorParamTypes) throws Exception
  {

    if (imageFormatHandlers == null || mimeTypeHandlers == null)
    {
      loadHandlers();
    }

    if (org.apache.commons.lang.StringUtils.isEmpty(imageFormat))
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
      final Map<String, Class<?>> handlers = mimeTypeHandlers.get(handlerType);

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
      final Map<String, Class<?>> handlers = imageFormatHandlers.get(handlerType);

      if (handlers.containsKey(imageFormat))
      {
        Object cl;
        cl = handlers.get(imageFormat.toLowerCase()).newInstance();

        return cl;
      }
    }

    throw new IllegalArgumentException("Unsupported image format - " + imageFormat);
  }

  public static String[] getImageFormats(final Class<?> handlerType)
  {
    if (imageFormatHandlers == null || mimeTypeHandlers == null)
    {
      loadHandlers();
    }

    if (imageFormatHandlers.containsKey(handlerType))
    {
      final Map<String, Class<?>> handlers = imageFormatHandlers.get(handlerType);

      return handlers.keySet().toArray(new String[0]);
    }

    throw new IllegalArgumentException("Invalid handler type: " + handlerType.getCanonicalName() +
      ". Not supported.");
  }

  public static String[] getMimeFormats(final Class<?> handlerType)
  {
    if (imageFormatHandlers == null || mimeTypeHandlers == null)
    {
      loadHandlers();
    }

    if (mimeTypeHandlers.containsKey(handlerType))
    {
      final Map<String, Class<?>> handlers = mimeTypeHandlers.get(handlerType);

      return handlers.keySet().toArray(new String[0]);
    }

    throw new IllegalArgumentException("Invalid handler type: " + handlerType.getCanonicalName() +
      ". Not supported.");
  }

  private static void addFormatHandlers(final Map<String, Class<?>> handlers, final Class<?> clazz)
  {
    try
    {

      Object cl;
      cl = clazz.newInstance();

      Method method;
      method = clazz.getMethod("getWmsFormats");
      final Object o = method.invoke(cl);

      String[] formats;
      if (o != null)
      {
        formats = (String[]) o;

        for (final String format : formats)
        {
          handlers.put(format, clazz);
          log.info("      {}", format);
        }
      }
    }
    catch (final InstantiationException e)
    {
    }
    catch (final IllegalAccessException e)
    {
    }
    catch (final SecurityException e)
    {
    }
    catch (final NoSuchMethodException e)
    {
    }
    catch (final IllegalArgumentException e)
    {
    }
    catch (final InvocationTargetException e)
    {
    }
  }

  private static void addMimeHandlers(final Map<String, Class<?>> handlers, final Class<?> clazz)
  {
    try
    {

      Object cl;
      cl = clazz.newInstance();

      Method method;
      method = clazz.getMethod("getMimeTypes");
      final Object o = method.invoke(cl);

      String[] formats;
      if (o != null)
      {
        formats = (String[]) o;

        for (final String format : formats)
        {
          handlers.put(format, clazz);
          log.info("      {}", format);
        }
      }
    }
    catch (final InstantiationException e)
    {
    }
    catch (final IllegalAccessException e)
    {
    }
    catch (final SecurityException e)
    {
    }
    catch (final NoSuchMethodException e)
    {
    }
    catch (final IllegalArgumentException e)
    {
    }
    catch (final InvocationTargetException e)
    {
    }
  }

  /**
   * Returns a MrGeo WMS "image handler" for the requested image format
   * 
   * @param imageFormat
   *          image format the requested image handler supports
   * @param handlerType
   *          a supported image handler type (see comments below to see how that is determined)
   * @param constructorParams
   *          parameters to pass to the image handlers constructor; optional (pass null for none)
   * @param constructorParamTypes
   *          parameters types to pass to the image handlers constructor; optional (pass null for
   *          none)
   * @return an object of specified type
   * @throws Exception
   */

  private static synchronized void loadHandlers()
  {
    if (imageFormatHandlers == null)
    {
      imageFormatHandlers = new HashMap<Class<?>, Map<String, Class<?>>>();
      mimeTypeHandlers = new HashMap<Class<?>, Map<String, Class<?>>>();

      Reflections reflections = new Reflections(ClassUtils.getPackageName(ImageRenderer.class));

      // image format renderers
      mimeTypeHandlers.put(ImageRenderer.class, new HashMap<String, Class<?>>());
      imageFormatHandlers.put(ImageRenderer.class, new HashMap<String, Class<?>>());

      Map<String, Class<?>> mimeHandlers = mimeTypeHandlers.get(ImageRenderer.class);
      Map<String, Class<?>> formatHandlers = imageFormatHandlers.get(ImageRenderer.class);

      final Set<Class<? extends ImageRenderer>> imageRenderers = reflections
        .getSubTypesOf(ImageRenderer.class);

      for (final Class<? extends ImageRenderer> clazz : imageRenderers)
      {
        log.info("Registering Image Renderer: {}", clazz.getCanonicalName());

        log.info("  Mime Types");
        addMimeHandlers(mimeHandlers, clazz);

        log.info("  Format Strings");
        addFormatHandlers(formatHandlers, clazz);
      }

      reflections = new Reflections(ClassUtils.getPackageName(ColorScaleApplier.class));

      // Color scale appliers
      mimeTypeHandlers.put(ColorScaleApplier.class, new HashMap<String, Class<?>>());
      imageFormatHandlers.put(ColorScaleApplier.class, new HashMap<String, Class<?>>());

      mimeHandlers = mimeTypeHandlers.get(ColorScaleApplier.class);
      formatHandlers = imageFormatHandlers.get(ColorScaleApplier.class);

      final Set<Class<? extends ColorScaleApplier>> colorscaleappliers = reflections
        .getSubTypesOf(ColorScaleApplier.class);

      for (final Class<? extends ColorScaleApplier> clazz : colorscaleappliers)
      {
        log.info("Registering Color Scale Applier: {}", clazz.getCanonicalName());
        log.info("  Mime Types");
        addMimeHandlers(mimeHandlers, clazz);

        log.info("  Format Strings");
        addFormatHandlers(formatHandlers, clazz);
      }

      reflections = new Reflections(ClassUtils.getPackageName(ImageResponseWriter.class));

      // image response writers
      mimeTypeHandlers.put(ImageResponseWriter.class, new HashMap<String, Class<?>>());
      imageFormatHandlers.put(ImageResponseWriter.class, new HashMap<String, Class<?>>());

      mimeHandlers = mimeTypeHandlers.get(ImageResponseWriter.class);
      formatHandlers = imageFormatHandlers.get(ImageResponseWriter.class);
      final Set<Class<? extends ImageResponseWriter>> imageresponsewriters = reflections
        .getSubTypesOf(ImageResponseWriter.class);

      for (final Class<? extends ImageResponseWriter> clazz : imageresponsewriters)
      {
        log.info("Registering Image Image Response Writer: {}", clazz.getCanonicalName());
        log.info("  Mime Types");
        addMimeHandlers(mimeHandlers, clazz);

        log.info("  Format Strings");
        addFormatHandlers(formatHandlers, clazz);
      }
    }
  }

}
