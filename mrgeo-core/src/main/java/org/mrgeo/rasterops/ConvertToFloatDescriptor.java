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

package org.mrgeo.rasterops;

import javax.media.jai.JAI;
import javax.media.jai.OperationDescriptorImpl;
import javax.media.jai.registry.RenderedRegistryMode;
import java.awt.*;
import java.awt.image.RenderedImage;
import java.awt.image.renderable.ParameterBlock;
import java.awt.image.renderable.RenderedImageFactory;
import java.util.Vector;

/**
 * @author jason.surratt
 * 
 */
public class ConvertToFloatDescriptor extends OperationDescriptorImpl implements
    RenderedImageFactory
{
  private static final long serialVersionUID = 1L;
  public final static int COLOR_SCALE = 0;
  public static final String OPERATION_NAME = ConvertToFloatDescriptor.class.getName();

  public static RenderedImage create(RenderedImage src)
  {
    ParameterBlock paramBlock = (new ParameterBlock()).addSource(src);
    return JAI.create(OPERATION_NAME, paramBlock, null);
  }

  public ConvertToFloatDescriptor()
  {
    // I realize this formatting is horrendous, but Java won't let me assign
    // variables before
    // calling super.
    super(new String[][] { { "GlobalName", OPERATION_NAME }, { "LocalName", OPERATION_NAME },
        { "Vendor", "com.spadac" }, { "Description", "" }, { "DocURL", "http://www.spadac.com/" },
        { "Version", "1.0" }, { "arg0Desc", "" } },
        new String[] { RenderedRegistryMode.MODE_NAME }, 0, new String[] {}, new Class[] {},
        // I'm handing an enumerated value for the kernel b/c I had class not
        // found errors under Linux
        new Object[] {}, null);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * java.awt.image.renderable.RenderedImageFactory#create(java.awt.image.renderable
   * .ParameterBlock, java.awt.RenderingHints)
   */
  @Override
  @SuppressWarnings("unchecked")
  public RenderedImage create(ParameterBlock paramBlock, RenderingHints hints)
  {
    @SuppressWarnings("rawtypes")
    Vector sources = new Vector();
    sources.add(paramBlock.getRenderedSource(0));

    ConvertToFloatOpImage result;
      result = ConvertToFloatOpImage.create(paramBlock.getRenderedSource(0));

    return result;
  }

  @Override
  public String toString()
  {
    return "to float";
  }
}
