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

package org.mrgeo.services.wcs;

import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.services.SecurityUtils;
import org.mrgeo.services.Version;
import org.mrgeo.services.mrspyramid.rendering.ImageHandlerFactory;
import org.mrgeo.services.mrspyramid.rendering.ImageRenderer;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.TMSUtils;
import org.mrgeo.utils.XmlUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.awt.image.DataBuffer;
import java.io.IOException;
import java.util.Arrays;

public class DescribeCoverageDocumentGenerator
{
  public DescribeCoverageDocumentGenerator()
  {
  }

  public Document generateDoc(Version version, String s, String[] layers) throws IOException
  {
    Document doc = XmlUtils.createDocument();

    if (version.isLess("1.1.0"))
    {
      generate100(doc, version, layers);
    }
    else
    {
      generate110(doc, version, layers);
    }

    return doc;
  }

  private void generate110(Document doc, Version version, String[] layers) throws IOException
  {
    Element descriptions = XmlUtils.createElement(doc, "wcs:CoverageDescriptions");
    setupNamespaces(descriptions, version);

    for (MrsImageDataProvider provider : getLayers(layers))
    {
      MrsPyramidMetadata metadata = provider.getMetadataReader().read();

      int maxzoom = metadata.getMaxZoomLevel();

      Element description = XmlUtils.createElement(descriptions, "wcs:CoverageDescription");
      // name and label
      XmlUtils.createTextElement2(description, "ows:Title", provider.getResourceName());
      XmlUtils.createTextElement2(description, "ows:Abstract", "Layer generated using MrGeo");
      XmlUtils.createTextElement2(description, "wcs:Identifier", provider.getResourceName());

      // spatial data
      Element spatial = XmlUtils.createElement(XmlUtils.createElement(description, "wcs:Domain"), "wcs:SpatialDomain");

      // bounds
      Bounds bounds = metadata.getBounds();

      Element envelope = XmlUtils.createElement(spatial, "ows:BoundingBox");
      envelope.setAttribute("crs", "urn:ogc:def:crs:OGC:1.3:CRS84");
      envelope.setAttribute("dimensions", "2");
      XmlUtils.createTextElement2(envelope, "ows:LowerCorner",
          String.format("%.8f %.8f", bounds.getMinX(), bounds.getMinY()));
      XmlUtils.createTextElement2(envelope, "ows:UpperCorner",
          String.format("%.8f %.8f", bounds.getMaxX(), bounds.getMaxY()));

       envelope = XmlUtils.createElement(spatial, "ows:BoundingBox");
      envelope.setAttribute("crs", "urn:ogc:def:crs:EPSG::4326");
      envelope.setAttribute("dimensions", "2");
      XmlUtils.createTextElement2(envelope, "ows:LowerCorner",
          String.format("%.8f %.8f", bounds.getMinY(), bounds.getMinX()));
      XmlUtils.createTextElement2(envelope, "ows:UpperCorner",
          String.format("%.8f %.8f", bounds.getMaxY(), bounds.getMaxX()));



      double res = TMSUtils.resolution(maxzoom, metadata.getTilesize());

      Element grid = XmlUtils.createElement(spatial, "wcs:GridCRS");
      XmlUtils.createTextElement2(grid, "wcs:GridBaseCRS", "urn:ogc:def:crs:EPSG::4326");
      XmlUtils.createTextElement2(grid, "wcs:GridType", "urn:ogc:def:method:WCS:1.1:2dGridIn2dCrs");
      // origin
      XmlUtils.createTextElement2(grid, "wcs:GridOrigin", String.format("%.8f %.8f", bounds.getMinX(), bounds.getMinY()));
      XmlUtils.createTextElement2(grid, "wcs:GridCS", "urn:ogc:def:cs:OGC:0.0:Grid2dSquareCS");

      // pixel size
      XmlUtils.createTextElement2(grid, "wcs:GridOffsets", String.format("%.8f 0.0 0.0 %.8f", res, -res));

      // band info
      Element bands = XmlUtils.createElement(XmlUtils.createElement(description, "wcs:Range"), "wcs:Field");
      XmlUtils.createTextElement2(bands, "wcs:Identifier", "contents");
      XmlUtils.createElement(XmlUtils.createElement(bands, "wcs:Definition"), "wcs:AnyValue");

      // Interpolations
      Element interp = XmlUtils.createElement(bands, "wcs:InterpolationMethods");
      if (metadata.getClassification() == MrsPyramidMetadata.Classification.Categorical)
      {
        XmlUtils.createTextElement2(interp, "wcs:Default", "nearest neighbour");
      }
      else
      {
        XmlUtils.createTextElement2(interp, "wcs:Default", "linear");
      }

      XmlUtils.createTextElement2(description, "wcs:SupportedCRS", "urn:ogc:def:crs:EPSG::4326");
      XmlUtils.createTextElement2(description, "wcs:SupportedCRS", "EPSG:4326");

      String[] formatStr = ImageHandlerFactory.getMimeFormats(ImageRenderer.class);

      Arrays.sort(formatStr);
      for (String format: formatStr)
      {
        XmlUtils.createTextElement2(description, "wcs:SupportedFormat", format);
      }

    }
  }

  private void generate100(Document doc, Version version, String[] layers) throws IOException
  {

    Element desc = XmlUtils.createElement(doc, "CoverageDescription");
    setupNamespaces(desc, version);

    //noinspection LoopStatementThatDoesntLoop
    for (MrsImageDataProvider provider : getLayers(layers))
    {
      MrsPyramidMetadata metadata = provider.getMetadataReader().read();

      int maxzoom = metadata.getMaxZoomLevel();

      Element offering = XmlUtils.createElement(desc, "CoverageOffering");
      // name and label
      XmlUtils.createTextElement2(offering, "name", provider.getResourceName());
      XmlUtils.createTextElement2(offering, "label", provider.getResourceName());
      XmlUtils.createTextElement2(offering, "description", "Layer generated using MrGeo");

      // bounds
      Bounds bounds = metadata.getBounds();

      Element envelope = XmlUtils.createElement(offering, "lonLatEnvelope");
      envelope.setAttribute("srsName", "WGS84(DD)");
      XmlUtils.createTextElement2(envelope, "gml:pos",
          String.format("%.8f %.8f", bounds.getMinX(), bounds.getMinY()));
      XmlUtils.createTextElement2(envelope, "gml:pos",
          String.format("%.8f %.8f", bounds.getMaxX(), bounds.getMaxY()));

      // spatial data
      Element spatial = XmlUtils.createElement(XmlUtils.createElement(offering, "domainSet"), "spatialDomain");

      // another envelope
      Element senvelope = XmlUtils.createElement(spatial, "gml:Envelope");
      senvelope.setAttribute("srsName", "EPSG:4326");
      XmlUtils.createTextElement2(senvelope, "gml:pos",
          String.format("%.8f %.8f", bounds.getMinX(), bounds.getMinY()));
      XmlUtils.createTextElement2(senvelope, "gml:pos",
          String.format("%.8f %.8f", bounds.getMaxX(), bounds.getMaxY()));

      Element grid = XmlUtils.createElement(spatial, "gml:RectifiedGrid");
      Element limits = XmlUtils.createElement(grid, "gml:limits");

      // min/max pixels for the image (pixel bounds at the lowest level)
      final LongRectangle pixelBounds = metadata.getPixelBounds(maxzoom);
      Element genvelope = XmlUtils.createElement(limits, "gml:GridEnvelope");
      XmlUtils.createTextElement2(genvelope, "gml:low",
          String.format("%d %d", pixelBounds.getMinX(), pixelBounds.getMinY()));
      XmlUtils.createTextElement2(genvelope, "gml:high",
          String.format("%d %d", pixelBounds.getMaxX(), pixelBounds.getMaxY()));

      // band names
      for (int b = 0; b < metadata.getBands(); b++)
      {
        XmlUtils.createTextElement2(grid, "gml:axisName", "Band " + b);
      }

      // origin
      Element origin = XmlUtils.createElement(grid, "gml:origin");
      XmlUtils.createTextElement2(origin, "gml:pos", String.format("%.8f %.8f", bounds.getMinX(), bounds.getMinY()));

      // pixel size
      double res = TMSUtils.resolution(maxzoom, metadata.getTilesize());
      XmlUtils.createTextElement2(grid, "gml:offsetVector",
          String.format("%.12f 0.0", res));
      XmlUtils.createTextElement2(grid, "gml:offsetVector",
          String.format("0.0 %.12f", -res));

      // Band info
      Element range = XmlUtils.createElement(XmlUtils.createElement(offering, "rangeSet"), "RangeSet");

      XmlUtils.createTextElement2(range, "name", provider.getResourceName());
      XmlUtils.createTextElement2(range, "label", provider.getResourceName());

      Element axis = XmlUtils.createElement(XmlUtils.createElement(range, "axisDescription"), "AxisDescription");

      XmlUtils.createTextElement2(axis, "name", "Band");
      XmlUtils.createTextElement2(axis, "label", "Band");

      Element interval = XmlUtils.createElement(XmlUtils.createElement(axis, "value"), "interval");
      XmlUtils.createTextElement2(interval, "min", "1");
      XmlUtils.createTextElement2(interval, "max", "" + metadata.getBands());

      Element nodata = XmlUtils.createElement(range, "nullValues");
      for (int b = 0; b < metadata.getBands(); b++) {
        switch (metadata.getTileType()) {
        case DataBuffer.TYPE_BYTE:
          XmlUtils.createTextElement2(nodata, "singleValue", "" + metadata.getDefaultValueByte(b));
          break;
        case DataBuffer.TYPE_INT:
        case DataBuffer.TYPE_SHORT:
        case DataBuffer.TYPE_USHORT:
          XmlUtils.createTextElement2(nodata, "singleValue", "" + metadata.getDefaultValueInt(b));
          break;
        case DataBuffer.TYPE_FLOAT:
          XmlUtils.createTextElement2(nodata, "singleValue", "" + metadata.getDefaultValueFloat(b));
          break;
        case DataBuffer.TYPE_DOUBLE:
          XmlUtils.createTextElement2(nodata, "singleValue", "" + metadata.getDefaultValueDouble(b));
          break;
        }
      }

      // CRSs
      XmlUtils.createTextElement2(XmlUtils.createElement(offering, "supportedCRSs"),
          "requestResponseCRSs", "EPSG:4326");

      // Formats
      Element formats = XmlUtils.createElement(offering, "supportedFormats");
      formats.setAttribute("nativeFormat", "GeoTIFF");

      String[] formatStr = ImageHandlerFactory.getImageFormats(ImageRenderer.class);

      Arrays.sort(formatStr);
      for (String format: formatStr)
      {
        XmlUtils.createTextElement2(formats, "formats", format);
      }

      // Interpolations
      Element interp = XmlUtils.createElement(offering, "supportedInterpolations");
      if (metadata.getClassification() == MrsPyramidMetadata.Classification.Categorical)
      {
        interp.setAttribute("default", "nearest neighbour");
        XmlUtils.createTextElement2(interp, "interpolationMethod", "nearest neighbour");
      }
      else
      {
        interp.setAttribute("default", "bilinear");
        XmlUtils.createTextElement2(interp, "interpolationMethod", "bilinear");
      }

      break;  // only 1 layer for CoverageDescription
    }
  }


  private void setupNamespaces(Element dtr, Version version)
  {
    dtr.setAttribute("version", version.toString());
    dtr.setAttribute("xmlns", "http://www.opengis.net/wcs");
    dtr.setAttribute("xmlns:ogc", "http://www.opengis.net/ogc");
    dtr.setAttribute("xmlns:gml", "http://www.opengis.net/gml");
    dtr.setAttribute("xmlns:xlink", "http://www.w3.org/1999/xlink");
    dtr.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
    if (!version.isLess("1.1.0"))
    {
      dtr.setAttribute("xmlns:ows", "http://www.opengis.net/ows/1.1" + version.toString());
      dtr.setAttribute("xmlns:wcs", "http://www.opengis.net/wcs/" + version.toString());
    }
    dtr.setAttribute("xsi:schemaLocation",
        "http://www.opengis.net/wcs http://schemas.opengis.net/wcs/" + version.toString() + "/describeCoverage.xsd");
  }

  private MrsImageDataProvider[] getLayers(String[] layers) throws IOException
  {
    ProviderProperties providerProperties = SecurityUtils.getProviderProperties();

    MrsImageDataProvider[] providers = new MrsImageDataProvider[layers.length];

    for (int i = 0; i < layers.length; i++)
    {
      providers[i] = DataProviderFactory.getMrsImageDataProvider(layers[i],
          DataProviderFactory.AccessMode.READ, providerProperties);
    }

    return providers;

  }
}
