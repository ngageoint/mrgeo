package org.mrgeo.services.wcs;

import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.image.MrsImagePyramidMetadata;
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
import java.util.Properties;

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
      generageCoverage(doc, version, layers);
    }
    else
    {
      generateIdentifiers(doc, version, layers);
    }

    return doc;
  }

  private void generateIdentifiers(Document doc, Version version, String[] layers) throws IOException
  {
    Element descriptions = XmlUtils.createElement(doc, "wcs:CoverageDescriptions");
    setupNamespaces(descriptions, version);

    for (MrsImageDataProvider provider : getLayers(layers))
    {
      MrsImagePyramidMetadata metadata = provider.getMetadataReader().read();

      int maxzoom = metadata.getMaxZoomLevel();

      Element description = XmlUtils.createElement(descriptions, "wcs:CoverageDescription");
      // name and label
      XmlUtils.createTextElement2(description, "ows:Title", provider.getResourceName());
      XmlUtils.createTextElement2(description, "ows:Abstract", "Layer generated using MrGeo");
      XmlUtils.createTextElement2(description, "wcs:Identifier", provider.getResourceName());
    }
  }

  private void generageCoverage(Document doc, Version version, String[] layers) throws IOException
  {

    Element desc = XmlUtils.createElement(doc, "CoverageDescription");
    setupNamespaces(desc, version);

    //noinspection LoopStatementThatDoesntLoop
    for (MrsImageDataProvider provider : getLayers(layers))
    {
      MrsImagePyramidMetadata metadata = provider.getMetadataReader().read();

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
        XmlUtils.createTextElement2(formats, "format", format);
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
    Properties providerProperties = SecurityUtils.getProviderProperties();

    MrsImageDataProvider[] providers = new MrsImageDataProvider[layers.length];

    for (int i = 0; i < layers.length; i++)
    {
      providers[i] = DataProviderFactory.getMrsImageDataProvider(layers[i],
          DataProviderFactory.AccessMode.READ, providerProperties);
    }

    return providers;

  }
}
