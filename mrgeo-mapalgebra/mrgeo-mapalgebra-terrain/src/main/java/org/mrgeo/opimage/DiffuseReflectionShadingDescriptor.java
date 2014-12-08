/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */

package org.mrgeo.opimage;

import javax.media.jai.JAI;
import javax.media.jai.OperationDescriptorImpl;
import javax.media.jai.RenderedOp;
import javax.media.jai.registry.RenderedRegistryMode;
import javax.media.jai.util.ImagingException;
import javax.vecmath.Vector3d;
import java.awt.*;
import java.awt.image.RenderedImage;
import java.awt.image.renderable.ParameterBlock;
import java.awt.image.renderable.RenderedImageFactory;
import java.io.IOException;
import java.util.Vector;

/**
 * @author jason.surratt
 * 
 */
public class DiffuseReflectionShadingDescriptor extends OperationDescriptorImpl implements
    RenderedImageFactory
{
  public final static int LIGHT_SOURCE = 0;
  public static final String OPERATION_NAME = DiffuseReflectionShadingDescriptor.class.getName();
  private static final long serialVersionUID = 1L;

  public static RenderedOp create(RenderedImage elevation, Vector3d lightSource,
      RenderingHints hints)
  {
    ParameterBlock paramBlock = (new ParameterBlock()).addSource(elevation).add(lightSource);
    return JAI.create(OPERATION_NAME, paramBlock, hints);
  }

  public DiffuseReflectionShadingDescriptor()
  {
    // I realize this formatting is horrendous, but Java won't let me assign
    // variables before
    // calling super.
    super(new String[][] { { "GlobalName", OPERATION_NAME }, { "LocalName", OPERATION_NAME },
        { "Vendor", "com.spadac" }, { "Description", "Relief shading in geographic coordinates" },
        { "DocURL", "http://www.spadac.com/" }, { "Version", "1.0" }, { "arg0Desc", "" } },
        new String[] { RenderedRegistryMode.MODE_NAME }, 1, new String[] { "lightSource" },
        new Class[] { Vector3d.class }, new Object[] { new Vector3d(1, 1, -1) }, null);
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

    Vector3d lightSource = (Vector3d) paramBlock.getObjectParameter(LIGHT_SOURCE);

    DiffuseReflectionShadingOpImage result;
    try
    {
      result = DiffuseReflectionShadingOpImage.create(paramBlock.getRenderedSource(0), lightSource,
          hints);
    }
    catch (IOException e)
    {
      throw new ImagingException(e);
    }

    return result;
  }
}
