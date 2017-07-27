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

import org.gdal.osr.SpatialReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles PNG image format WMS image rendering
 */
public class PngImageRenderer extends ImageRendererAbstract
{
@SuppressWarnings("unused")
private static final Logger log = LoggerFactory.getLogger(PngImageRenderer.class);

public PngImageRenderer()
{
  super();
}

public PngImageRenderer(SpatialReference srs)
{
  super(srs);
}

/*
 * (non-Javadoc)
 *
 * @see org.mrgeo.services.wms.ImageRenderer#getMimeType()
 */
@Override
public String[] getMimeTypes()
{
  return new String[]{"image/png"};
}

@Override
public String[] getWmsFormats()
{
  return new String[]{"png"};
}

}
