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

package org.mrgeo.opimage;

import javax.media.jai.JAI;
import javax.media.jai.OperationDescriptorImpl;
import javax.media.jai.registry.RenderedRegistryMode;
import java.awt.*;
import java.awt.image.RenderedImage;
import java.awt.image.renderable.ParameterBlock;
import java.awt.image.renderable.RenderedImageFactory;

/**
 * @author jason.surratt
 * 
 */
public class ConstantDescriptor extends OperationDescriptorImpl implements
    RenderedImageFactory
{
  public static final String OPERATION_NAME = ConstantDescriptor.class.getName();
  private static final long serialVersionUID = 1L;

  public static RenderedImage create(double value, int tilesize, RenderingHints hints)
  {
    ParameterBlock paramBlock = (new ParameterBlock())
        .add(value)
        .add(tilesize);
    return JAI.create(OPERATION_NAME, paramBlock, hints);
  }

  public ConstantDescriptor()
  {
    // I realize this formatting is horrendous, but Java won't let me assign
    // variables before
    // calling super.
    super(new String[][] { { "GlobalName", OPERATION_NAME }, { "LocalName", OPERATION_NAME },
        { "Vendor", "org.mrgeo" }, { "Description", "Creates a constant value" },
        { "DocURL", "http://mrgeo.org/" }, { "Version", "1.0" } },
        new String[] { RenderedRegistryMode.MODE_NAME }, 
        0,
        new String[] { "value", "tilesize" },
        new Class[] { Double.class, Integer.class }, 
        new Object[] { NO_PARAMETER_DEFAULT, NO_PARAMETER_DEFAULT }, null);
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
    ConstantOpImage result;

    result = ConstantOpImage.create(paramBlock.getDoubleParameter(0),
        paramBlock.getIntParameter(1));

    return result;
  }

}
