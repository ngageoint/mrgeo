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

package org.mrgeo.opimage;

import javax.media.jai.JAI;
import javax.media.jai.OperationDescriptorImpl;
import javax.media.jai.RenderedOp;
import javax.media.jai.registry.RenderedRegistryMode;
import java.awt.*;
import java.awt.image.RenderedImage;
import java.awt.image.renderable.ParameterBlock;
import java.awt.image.renderable.RenderedImageFactory;

/**
 * @author jason.surratt
 * 
 */
public class ReplaceNullDescriptor extends OperationDescriptorImpl implements RenderedImageFactory
{
  private static final long serialVersionUID = 1L;
  public final static int COLOR_SCALE = 0;
  public static final String OPERATION_NAME = ReplaceNullDescriptor.class.getName();

  public static RenderedOp create(RenderedImage src1, double newValue, RenderingHints hints)
  {
    return create(src1, newValue, false, hints);
  }

  /**
   * 
   * @param src1
   * @param newValue
   * @param newNull
   *          If this is true, then the newValue becomes the new null.
   * @param hints
   * @return
   */
  public static RenderedOp create(RenderedImage src1, double newValue, boolean newNull,
      RenderingHints hints)
  {
    ParameterBlock paramBlock = (new ParameterBlock()).addSource(src1).add(newValue).add(
        newNull ? 1 : 0);
    return JAI.create(OPERATION_NAME, paramBlock, hints);
  }

  public ReplaceNullDescriptor()
  {
    // I realize this formatting is horrendous, but Java won't let me assign
    // variables before
    // calling super.
    super(new String[][] { { "GlobalName", OPERATION_NAME }, { "LocalName", OPERATION_NAME },
        { "Vendor", "com.spadac" }, { "Description", "" }, { "DocURL", "http://www.spadac.com/" },
        { "Version", "1.0" } }, new String[] { RenderedRegistryMode.MODE_NAME }, 1,
        new String[] { "newValue", "newNull" }, new Class[] { Double.class, Integer.class },
        new Object[] { Double.NaN, new Integer(0) }, null);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * java.awt.image.renderable.RenderedImageFactory#create(java.awt.image.renderable
   * .ParameterBlock, java.awt.RenderingHints)
   */
  @Override
  public RenderedImage create(ParameterBlock paramBlock, RenderingHints hints)
  {
    return ReplaceNullOpImage.create(paramBlock.getRenderedSource(0), paramBlock
        .getDoubleParameter(0), paramBlock.getIntParameter(1) == 1 ? true : false, hints);
  }

}
