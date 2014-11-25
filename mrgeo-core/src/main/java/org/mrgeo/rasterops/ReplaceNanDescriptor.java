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

package org.mrgeo.rasterops;

import javax.media.jai.JAI;
import javax.media.jai.OperationDescriptorImpl;
import javax.media.jai.RenderedOp;
import javax.media.jai.registry.RenderedRegistryMode;
import javax.media.jai.util.ImagingException;
import java.awt.*;
import java.awt.image.RenderedImage;
import java.awt.image.renderable.ParameterBlock;
import java.awt.image.renderable.RenderedImageFactory;
import java.io.IOException;

/**
 * @author jason.surratt
 *
 */
public class ReplaceNanDescriptor extends OperationDescriptorImpl implements RenderedImageFactory
{
  private static final long serialVersionUID = 1L;
  public final static int COLOR_SCALE = 0;
  public static final String OPERATION_NAME = ReplaceNanDescriptor.class.getName();

  public static RenderedOp create(RenderedImage src1, double newValue)
  {
    ParameterBlock paramBlock = (new ParameterBlock()).addSource(src1).add(newValue);
    return JAI.create(OPERATION_NAME, paramBlock, null);
  }

  public ReplaceNanDescriptor()
  {
    // I realize this formatting is horrendous, but Java won't let me assign
    // variables before
    // calling super.
    super(new String[][] { { "GlobalName", OPERATION_NAME }, { "LocalName", OPERATION_NAME },
        { "Vendor", "com.spadac" }, { "Description", "" }, { "DocURL", "http://www.spadac.com/" },
        { "Version", "1.0" } }, new String[] { RenderedRegistryMode.MODE_NAME }, 1, new String[] {
        "newValue" }, new Class[] { Double.class }, new Object[] { Double.NaN }, null);
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
    ReplaceNanOpImage result;
    try
    {
      result = ReplaceNanOpImage.create(paramBlock.getRenderedSource(0), paramBlock
          .getDoubleParameter(0));
    }
    catch (IOException e)
    {
      throw new ImagingException(e);
    }

    return result;
  }

}
