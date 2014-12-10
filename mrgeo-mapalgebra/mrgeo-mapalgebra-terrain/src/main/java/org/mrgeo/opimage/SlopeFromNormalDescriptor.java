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
public class SlopeFromNormalDescriptor extends OperationDescriptorImpl implements
RenderedImageFactory
{
  public static final String OPERATION_NAME = SlopeFromNormalDescriptor.class.getName();
  private static final long serialVersionUID = 1L;
  private static final int UNITS = 0;

  public static RenderedImage create(RenderedImage slope, int units, RenderingHints hints)
  {
    ParameterBlock paramBlock = (new ParameterBlock()).addSource(slope).add(units);
    return new SlopeFromNormalDescriptor().create(paramBlock, hints);
  }

  public SlopeFromNormalDescriptor()
  {
    // I realize this formatting is horrendous, but Java won't let me assign
    // variables before
    // calling super.
    super(new String[][] { { "GlobalName", OPERATION_NAME }, { "LocalName", OPERATION_NAME },
        { "Vendor", "com.spadac" }, { "Description", "Max slope values for an elevation surface" },
        { "DocURL", "http://www.spadac.com/" }, { "Version", "1.0" } },
        new String[] { RenderedRegistryMode.MODE_NAME }, 
        1, 
        new String[] { "units" },
        new Class[] { Integer.class }, 
        new Object[] { NO_PARAMETER_DEFAULT }, null);
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
    int units = paramBlock.getIntParameter(UNITS);

    return  SlopeFromNormalOpImage.create(paramBlock.getRenderedSource(0), units, hints);
  }

}
