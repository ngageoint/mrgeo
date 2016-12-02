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

package org.mrgeo.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.IIOException;
import javax.imageio.stream.ImageOutputStream;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Utilities to be used with Servlet requests/responses
 */
public class ServletUtils
{
private static final Logger log = LoggerFactory.getLogger(ServletUtils.class);

/**
 * Prints request attributes to the debug logger
 * @param request servlet request
 */
//  public static void printRequestAttributes(HttpServletRequest request)
//  {
//    @SuppressWarnings("rawtypes")
//    Enumeration enAttr = request.getAttributeNames();
//    while (enAttr.hasMoreElements())
//    {
//      String attributeName = (String)enAttr.nextElement();
//      log.debug("Attribute Name: {}, Value: {}", attributeName, (request.getAttribute(attributeName)).toString());
//    }
//  }

/**
 * Prints request attributes to the debug logger
 * @param request servlet request
 */
//  public static void printRequestURL(HttpServletRequest request)
//  {
//    final StringBuffer requestURL = request.getRequestURL();
//    final String queryString = request.getQueryString();
//
//    if (queryString == null)
//    {
//      log.debug("HTTP Request {}", requestURL.toString());
//    }
//    else
//    {
//      log.debug("HTTP Request {}?{}", requestURL.toString(), queryString);
//    }
//  }

/**
 * Prints request parameters to the debug logger
 * @param request servlet request
 */
//  public static void printRequestParams(HttpServletRequest request)
//  {
//    @SuppressWarnings("rawtypes")
//    Enumeration enParams = request.getParameterNames();
//    while (enParams.hasMoreElements())
//    {
//      String paramName = (String)enParams.nextElement();
//      log.debug("Attribute Name: {}, Value: {}", paramName, request.getParameter(paramName));
//    }
//  }

/**
 * Writes an image to a servlet response
 *
 * @param response servlet response
 * @param image    image bytes
 * @throws IOException
 */
@SuppressWarnings("squid:S1166") // Exception caught and handled
public static void writeImageToResponse(HttpServletResponse response, byte[] image)
    throws IOException
{
  try
  {
    ImageOutputStream imageStream = new MemoryCacheImageOutputStream(response.getOutputStream());
    imageStream.write(image, 0, image.length);
    response.setContentLength((int) imageStream.length());
    imageStream.close();
  }
  // TODO: remove these catches - This is to prevent seeing the broken pipes exception
  // over and over again in the trace...leave catch in until issue is fixed (#1122)
  catch (IIOException | IndexOutOfBoundsException ignored)
  {
  }
}


/**
 * Validates the existence and type of an HTTP parameter and returns it
 * @param request servlet request
 * @param name parameter name
 * @param type parameter data type
 * @return parameter value
 * @throws Exception if the parameter does not exist or cannot be cast to the requested data type
 */
//  public static String validateAndGetParamValue(HttpServletRequest request, String name,
//    String type) throws Exception
//  {
//    validateParam(request, name, type);
//    return getParamValue(request, name);
//  }

/**
 * Validates the existence and type of an HTTP parameter.
 * @param request servlet request
 * @param name parameter name
 * @param type parameter data type
 * @throws Exception if the parameter does not exist or cannot be cast to the requested data type
 */
//  public static void validateParam(HttpServletRequest request, String name, String type)
//    throws Exception
//  {
//    CaseInsensitiveMap params = new CaseInsensitiveMap(request.getParameterMap());
//    if (params.containsKey(name))
//    {
//      String value = ((String[]) params.get(name))[0];
//      if (StringUtils.isEmpty(value))
//      {
//        throw new IllegalArgumentException("Missing request parameter: " + name);
//      }
//      if (type.equals("integer"))
//      {
//        try
//        {
//          Integer.parseInt(value);
//        }
//        catch (NumberFormatException e)
//        {
//          throw new IllegalArgumentException("Invalid request parameter: " + name);
//        }
//      }
//      if (type.equals("double"))
//      {
//        try
//        {
//          Double.parseDouble(value);
//        }
//        catch (NumberFormatException e)
//        {
//          throw new IllegalArgumentException("Invalid request parameter: " + name);
//        }
//      }
//    }
//    else
//    {
//      throw new IllegalArgumentException("Missing request parameter: " + name);
//    }
//  }

/**
 * Retrieves a parameter value from a servlet request
 * @param request servlet request
 * @param name parameter name
 * @return parameter value
 */
//  public static String getParamValue(HttpServletRequest request, String name)
//  {
//    CaseInsensitiveMap params = new CaseInsensitiveMap(request.getParameterMap());
//    if (params.containsKey(name))
//    {
//      return ((String[])params.get(name))[0];
//    }
//    return null;
//  }
}
