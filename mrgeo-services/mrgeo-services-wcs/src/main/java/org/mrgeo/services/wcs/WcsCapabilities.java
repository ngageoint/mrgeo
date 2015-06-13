package org.mrgeo.services.wcs;

import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.image.MrsImage;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.rasterops.ColorScale;
import org.mrgeo.services.Version;
import org.mrgeo.services.mrspyramid.ColorScaleManager;
import org.mrgeo.services.mrspyramid.rendering.ImageHandlerFactory;
import org.mrgeo.services.mrspyramid.rendering.ImageRenderer;
import org.mrgeo.utils.LatLng;
import org.mrgeo.utils.XmlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

public class WcsCapabilities
{
  private static final Logger log = LoggerFactory.getLogger(WcsCapabilities.class);

          /**
           * Generates an XML document for a DescribeTiles request
           * @param version WMS version
           * @param requestUrl HTTP request url
           * @param pyramidFiles list of pyramid directories being served by MrGeo
           * @return XML document list of pyramid directories being served by MrGeo
           * @throws IOException
           * @throws InterruptedException
           * @throws ParserConfigurationException
           */

  public Document generateDoc(Version version, String requestUrl,
                              MrsImageDataProvider[] pyramidFiles) throws IOException, InterruptedException,
          ParserConfigurationException
  {
    Document doc;
    DocumentBuilderFactory dBF = DocumentBuilderFactory.newInstance();
    dBF.setValidating(true);

    DocumentBuilder builder = dBF.newDocumentBuilder();
    doc = builder.newDocument();

    Element wmc = doc.createElement("WCS_Capabilities");
    wmc.setAttribute("version", version.toString());
    // Assume version 1.0.0 for now...
    wmc.setAttribute("xmlns", "http://www.opengis.net/wcsc");
    wmc.setAttribute("xmlns:xlink", "http://www.w3.org/1999/xlink");
    wmc.setAttribute("xmlns:gml", "http://www.opengis.net/gml");
    wmc.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
    wmc.setAttribute("xsi:schemaLocation",
                     "http://www.opengis.net/wcs http://schemas.opengeospatial.net/wcs/" + version.toString() + "/wcsCapabilities.xsd");
    doc.appendChild(wmc);
    // //
    // Service
    // //
    Element service = XmlUtils.createElement(wmc, "Service");
    wmc.appendChild(service);
    // WMT Defined
    XmlUtils.createTextElement2(service, "Name", "OGC:WC");
    XmlUtils.createTextElement2(service, "description", "MrGeo Web Coverage Service");
    XmlUtils.createTextElement2(service, "label", "MrGeo Web Coverage Service");
    XmlUtils.createTextElement2(service, "Fees", "none");
    XmlUtils.createTextElement2(service, "AccessConstraints", "none");

    // //
    // Capability
    // //
    Element capability = XmlUtils.createElement(wmc, "Capability");

    // Request
    Element requestTag = XmlUtils.createElement(capability, "Request");
    // GetCapabilities
    {
      Element getCapabilities = XmlUtils.createElement(requestTag, "GetCapabilities");
      Element gcDcpType = XmlUtils.createElement(getCapabilities, "DCPType");
      addHttpElement(gcDcpType, requestUrl, version);
    }
    {
      Element describeCoverage = XmlUtils.createElement(requestTag, "DescribeCoverage");
      Element dcDcpType = XmlUtils.createElement(describeCoverage, "DCPType");
      addHttpElement(dcDcpType, requestUrl, version);
    }
    {
      Element getCapabilities = XmlUtils.createElement(requestTag, "GetCoverage");
      Element gcDcpType = XmlUtils.createElement(getCapabilities, "DCPType");
      addHttpElement(gcDcpType, requestUrl, version);
    }

    // Exception
    Element exception = XmlUtils.createElement(capability, "Exception");
    XmlUtils.createTextElement2(exception, "Format", "application/vnd.ogc.se_xml");

    // ContentMetadata
    {
      Element contentMetadata = XmlUtils.createElement(wmc, "ContentMetadata");
      addLayers(contentMetadata, version, pyramidFiles);
    }
    return doc;
  }

  private void addCapability(Element parent, String capability, Version version, String requestUrl)
  {
    // GetMap
    Element element = XmlUtils.createElement(parent, capability);
    String[] formats = ImageHandlerFactory.getMimeFormats(ImageRenderer.class);

    Arrays.sort(formats);
    for (String format: formats)
    {
      XmlUtils.createTextElement2(element, "Format", format);
    }

    Element gmDcpType = XmlUtils.createElement(element, "DCPType");
    addHttpElement(gmDcpType, requestUrl, version);
  }

  /*
   * Adds data layers to the GetCapabilities response
   */
  private void addLayers(Element parent, Version version,
                         MrsImageDataProvider[] providers) throws InterruptedException, IOException
  {
    double minx = Double.MAX_VALUE;
    double maxx = -Double.MAX_VALUE;
    double miny = Double.MAX_VALUE;
    double maxy = -Double.MAX_VALUE;

    Arrays.sort(providers, new Comparator<MrsImageDataProvider>()
    {
      @Override
      public int compare(MrsImageDataProvider o1, MrsImageDataProvider o2)
      {
        return o1.getResourceName().compareTo(o2.getResourceName());
      }
    });

    for (MrsImageDataProvider provider : providers)
    {
      log.debug("pyramids: " + provider.getResourceName());

      Document doc = parent.getOwnerDocument();
      Element layer = doc.createElement("CoverageOfferingBrief");
      // we'll add the layer to the parent later after all the child elements are
      // constructed in case there is a problem.

      XmlUtils.createTextElement2(layer, "description", provider.getResourceName());
      XmlUtils.createTextElement2(layer, "name", provider.getResourceName());
      XmlUtils.createTextElement2(layer, "label", provider.getResourceName());

      try
      {
        MrsImagePyramid pyramid = MrsImagePyramid.open(provider);
        try
        {
          minx = Math.min(minx, pyramid.getBounds().getMinX());
          miny = Math.min(miny, pyramid.getBounds().getMinY());
          maxx = Math.max(maxx, pyramid.getBounds().getMaxX());
          maxy = Math.max(maxy, pyramid.getBounds().getMaxY());

          Element envelope = XmlUtils.createElement(layer, "lonLatEnvelope");
          envelope.setAttribute("srsName", "WGS84(DD)");
          XmlUtils.createTextElement2(envelope, "gml:pos",
                                      "" + pyramid.getBounds().getMinX() + " " +
                                              pyramid.getBounds().getMinY());
          XmlUtils.createTextElement2(envelope, "gml:pos",
                                      "" + pyramid.getBounds().getMaxX() + " " +
                                              pyramid.getBounds().getMaxY());
          parent.appendChild(layer);
        }
        catch (NullPointerException e)
        {
          e.printStackTrace();
        }
      }
      catch (IOException e)
      {
        log.info("Skipping " + provider.getResourceName() + " in WCS GetCapabilities", e);
        // suck up the exception, there may be a bad file in the images directory...
      }
    }
  }

  /*
   * Adds OGC metadata elements to the the parent element
   */
  private static void addHttpElement(Element parent, String requestUrl, Version version)
  {
    Element http = XmlUtils.createElement(parent, "HTTP");
    Element get = XmlUtils.createElement(http, "Get");
    if (version.isEqual("1.1.1"))
    {
      Element onlineResource = XmlUtils.createElement(get, "OnlineResource");
      onlineResource.setAttribute("xmlns:xlink", "http://www.w3.org/1999/xlink");
      onlineResource.setAttribute("xlink:type", "simple");
      onlineResource.setAttribute("xlink:href", requestUrl);
    }
    else
    {
      XmlUtils.createTextElement2(get, "OnlineResource", requestUrl);
    }

    Element post = XmlUtils.createElement(http, "Post");
    if (version.isEqual("1.1.1"))
    {
      Element onlineResource = XmlUtils.createElement(post, "OnlineResource");
      onlineResource.setAttribute("xmlns:xlink", "http://www.w3.org/1999/xlink");
      onlineResource.setAttribute("xlink:type", "simple");
      onlineResource.setAttribute("xlink:href", requestUrl);
    }
    else
    {
      XmlUtils.createTextElement2(post, "OnlineResource", requestUrl);
    }
  }
}
