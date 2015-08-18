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

package org.mrgeo.resources;

import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.services.Configuration;
import org.mrgeo.utils.Bounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Comparator;
import java.util.zip.ZipOutputStream;

@Path("/kml")
public class KmlGenerator
{
  @SuppressWarnings("unused")
  private class Version
  {
    private int major, minor, micro;

    /**
     *
     *
     * @param str
     */
    public Version(String str)
    {
      String[] l = str.split("\\.");
      major = Integer.valueOf(l[0]);
      minor = Integer.valueOf(l[1]);
      micro = Integer.valueOf(l[2]);
    }

    /**
     *
     *
     * @param str
     * @return
     */
    public int compareTo(String str)
    {
      Version other = new Version(str);
      if (other.major < major)
        return 1;
      if (other.major > major)
        return -1;
      if (other.minor < minor)
        return 1;
      if (other.minor > minor)
        return -1;
      if (other.micro < micro)
        return 1;
      if (other.micro > micro)
        return -1;
      return 0;
    }

    public int getMajor()
    {
      return major;
    }

    public int getMicro()
    {
      return micro;
    }

    public int getMinor()
    {
      return minor;
    }

    /**
     *
     *
     * @param str
     * @return
     */
    public boolean isEqual(String str)
    {
      return compareTo(str) == 0;
    }

    /**
     *
     *
     * @param str
     * @return
     */
    public boolean isLess(String str)
    {
      return compareTo(str) == -1;
    }
  }

  private static final long serialVersionUID = 1L;
  private String baseUrl;

  private static final String KML_MIME_TYPE = "application/vnd.google-earth.kml+xml";
  private Version version;

  private final String WMS_VERSION = "1.3.0";

  private static final Logger log = LoggerFactory.getLogger(KmlGenerator.class);

  public KmlGenerator()
  {
  }

  private void addHttpElement(Element parent, HttpServletRequest request)
  {
    Element http = createElement(parent, "HTTP");
    Element get = createElement(http, "Get");
    if (version.isEqual("1.1.1"))
    {
      Element onlineResource = createElement(get, "OnlineResource");
      onlineResource.setAttribute("xmlns:xlink", "http://www.w3.org/1999/xlink");
      onlineResource.setAttribute("xlink:type", "simple");
      onlineResource.setAttribute("xlink:href", request.getRequestURL().toString());
    }
    else
    {
      createTextElement(get, "OnlineResource", request.getRequestURL().toString());
    }

    Element post = createElement(http, "Post");
    if (version.isEqual("1.1.1"))
    {
      Element onlineResource = createElement(post, "OnlineResource");
      onlineResource.setAttribute("xmlns:xlink", "http://www.w3.org/1999/xlink");
      onlineResource.setAttribute("xlink:type", "simple");
      onlineResource.setAttribute("xlink:href", request.getRequestURL().toString());
    }
    else
    {
      createTextElement(get, "OnlineResource", request.getRequestURL().toString());
    }
  }

  /*
 * Returns a list of all MrsImagePyramid version 2 data in the home data directory
 */
  private static MrsImageDataProvider[] getPyramidFilesList(
      final ProviderProperties providerProperties) throws IOException
  {
    String[] images = DataProviderFactory.listImages(providerProperties);

    Arrays.sort(images);

    MrsImageDataProvider[] providers = new MrsImageDataProvider[images.length];

    for (int i = 0; i < images.length; i++)
    {
      // original code looked for a MrsImagePyramid.toc file, not sure why, it wasn't used anywhere else...
//      Path toc = new Path(f.getPath(), "MrsImagePyramid.toc");
//      if (fs.exists(toc))
//      {
//        files.add(f);
//      }

      providers[i] = DataProviderFactory.getMrsImageDataProvider(images[i],
          DataProviderFactory.AccessMode.READ, providerProperties);
    }

    return providers;
  }

  /*
   *
   */
  private void addLayersToCapability(Element capability, Version kmlVersion,
      final ProviderProperties providerProperties) throws IOException
  {

    MrsImageDataProvider[] providers = getPyramidFilesList(providerProperties);

    Arrays.sort(providers, new Comparator<MrsImageDataProvider>()
    {
      @Override
      public int compare(MrsImageDataProvider o1, MrsImageDataProvider o2)
      {
        return o1.getResourceName().compareTo(o2.getResourceName());
      }
    });


    Element rootLayer = createElement(capability, "Layer");
    rootLayer.setAttribute("queryable", "0");
    rootLayer.setAttribute("opaque", "0");
    rootLayer.setAttribute("noSubsets", "0");
    createTextElement(rootLayer, "Title", "AllLayers");
    createTextElement(rootLayer, "SRS", "EPSG:4326");

    double minx = Double.MAX_VALUE;
    double maxx = -Double.MAX_VALUE;
    double miny = Double.MAX_VALUE;
    double maxy = -Double.MAX_VALUE;

    for (MrsImageDataProvider f : providers)
    {
      Element layer = createElement(rootLayer, "Layer");
      layer.setAttribute("queryable", "1");
      layer.setAttribute("cascaded", "0");
      layer.setAttribute("opaque", "1");
      layer.setAttribute("noSubsets", "0");
      layer.setAttribute("fixedWidth", "0");
      layer.setAttribute("fixedHeight", "0");
      createTextElement(layer, "Title", f.getResourceName());
      createTextElement(layer, "Name", f.getResourceName());

      try
      {
        MrsImagePyramid mp = MrsImagePyramid.open(f);

        minx = Math.min(minx, mp.getBounds().getMinX());
        miny = Math.min(miny, mp.getBounds().getMinY());
        maxx = Math.max(maxx, mp.getBounds().getMaxX());
        maxy = Math.max(maxy, mp.getBounds().getMaxY());

        if (kmlVersion.isLess("1.3.0"))
        {
          createTextElement(layer, "SRS", "EPSG:4326");
          Element llbb = createElement(layer, "LatLonBoundingBox");
          llbb.setAttribute("minx", String.valueOf(mp.getBounds().getMinX()));
          llbb.setAttribute("miny", String.valueOf(mp.getBounds().getMinY()));
          llbb.setAttribute("maxx", String.valueOf(mp.getBounds().getMaxX()));
          llbb.setAttribute("maxy", String.valueOf(mp.getBounds().getMaxY()));
        }
        else
        {
          createTextElement(layer, "CRS", "CRS:84");
          Element bb = createElement(layer, "EX_GeographicBoundingBox");
          createTextElement(bb, "westBoundLongitude", String.valueOf(mp.getBounds().getMinX()));
          createTextElement(bb, "eastBoundLongitude", String.valueOf(mp.getBounds().getMaxX()));
          createTextElement(bb, "southBoundLatitude", String.valueOf(mp.getBounds().getMinY()));
          createTextElement(bb, "northBoundLatitude", String.valueOf(mp.getBounds().getMaxY()));
        }
      }
      catch (IOException e)
      {
        // no op
      }

    }
  }

  /*
   * Checks the provided document against the 1.1.1 DTD
   */
  @SuppressWarnings("unused")
  private void checkForErrors(Document doc) throws SAXException, IOException, TransformerException,
      ParserConfigurationException
  {
    ByteArrayOutputStream strm = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(strm);
    writeDocument(doc, pw);

    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setValidating(true);
    DocumentBuilder builder = factory.newDocumentBuilder();
    builder.setErrorHandler(new ErrorHandler()
    {
      // Validation errors
      @Override
      public void error(SAXParseException ex) throws SAXParseException
      {
        log.error("Error at " + ex.getLineNumber() + " line.");
        log.error(ex.getMessage());
      }

      // Ignore the fatal errors
      @Override
      public void fatalError(SAXParseException ex) throws SAXException
      {
        log.error("Fatal Error at " + ex.getLineNumber() + " line.");
        log.error(ex.getMessage());
      }

      // Show warnings
      @Override
      public void warning(SAXParseException ex) throws SAXParseException
      {
        log.warn("Warning at " + ex.getLineNumber() + " line.");
        log.warn(ex.getMessage());
      }
    });

    ByteArrayInputStream input = new ByteArrayInputStream(strm.toByteArray());

    Document xmlDocument = builder.parse(input);
    DOMSource source = new DOMSource(xmlDocument);
    StreamResult result = new StreamResult(new ByteArrayOutputStream());
    TransformerFactory tf = TransformerFactory.newInstance();
    Transformer transformer = tf.newTransformer();
    transformer.setOutputProperty(OutputKeys.DOCTYPE_SYSTEM,
        "http://schemas.opengis.net/wms/1.1.1/WMS_MS_Capabilities.dtd");
    transformer.transform(source, result);
  }

  /**
   *
   *
   * @param parent
   * @param tagName
   * @return
   */
  public Element createElement(Element parent, String tagName)
  {
    Document doc = parent.getOwnerDocument();
    Element e = doc.createElement(tagName);
    parent.appendChild(e);
    return e;
  }

  /**
   *
   *
   * @param parent
   * @param tagName
   * @param text
   * @return
   */
  public Element createTextElement(Element parent, String tagName, String text)
  {
    Document doc = parent.getOwnerDocument();
    Element e = doc.createElement(tagName);
    e.appendChild(doc.createTextNode(text));
    parent.appendChild(e);
    return e;
  }

  @GET
  public void doGet(@Context HttpServletRequest request, @Context HttpServletResponse response)
      throws ServletException, IOException
  {
    init();

    response.setHeader("Cache-Control", "no-store");
    response.setHeader("Pragma", "no-cache");
    response.setDateHeader("Expires", 0);

    try
    {
      if (request.getQueryString() != null)
      {
        String serviceParam = request.getParameter("SERVICE");
        String requestParam = request.getParameter("REQUEST");

        if (serviceParam == null || serviceParam.isEmpty())
        {
          serviceParam = "kml";
        }

        if (!serviceParam.toLowerCase().equals("kml"))
        {
          throw new Exception("Unsupported service type was requested. (only KML is supported '"
              + serviceParam + "')");
        }

        ProviderProperties providerProperties = null;
        if (requestParam.toLowerCase().equals("getcapabilities"))
        {
          getCapabilities(request, response, providerProperties);
        }
        else if (requestParam.toLowerCase().equals("getkmlrootnode"))
        {
          getNetworkKmlRootNode(request, response, providerProperties);
        }
        else if (requestParam.toLowerCase().equals("getkmlnode"))
        {
          getNetworkKmlNode(request, response);
        }
        else
        {
          throw new Exception("No viable request made.");
        }
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
      try
      {
        Document doc;
        DocumentBuilderFactory dBF = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = dBF.newDocumentBuilder();
        doc = builder.newDocument();

        Element ser = doc.createElement("ServiceExceptionReport");
        doc.appendChild(ser);
        ser.setAttribute("version", WMS_VERSION);
        Element se = createElement(ser, "ServiceException");
        String code = e.getLocalizedMessage();
        if (code == null || code.isEmpty())
        {
          code = e.getClass().getName();
        }
        se.setAttribute("code", code);
        ByteArrayOutputStream strm = new ByteArrayOutputStream();
        e.printStackTrace(new PrintStream(strm));
        se.setAttribute("locator", strm.toString());
        PrintWriter out = response.getWriter();

        writeDocument(doc, out);
      }
      catch (Exception exception)
      {
        throw new IOException("Exception while creating XML exception (ah, the irony)."
            + exception.getLocalizedMessage());
      }
    }
  }

  @POST
  public void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException
  {
    doGet(request, response);
  }

  /*
   *
   */
  private void getCapabilities(HttpServletRequest request, HttpServletResponse response,
      final ProviderProperties providerProperties) throws Exception
  {
    Document doc;
    DocumentBuilderFactory dBF = DocumentBuilderFactory.newInstance();
    dBF.setValidating(true);

    DocumentBuilder builder = dBF.newDocumentBuilder();
    doc = builder.newDocument();

    String versionStr = request.getParameter("VERSION");
    if (versionStr == null || versionStr.isEmpty())
    {
      versionStr = "1.1.1";
    }
    version = new Version(versionStr);

    Element wmc = doc.createElement("WMT_MS_Capabilities");
    doc.appendChild(wmc);
    wmc.setAttribute("version", versionStr);
    wmc.setAttribute("updateSequence", "0");
    if (version.isEqual("1.3.0"))
    {
      wmc.setAttribute("xmlns", "http://www.opengis.net/wms");
      wmc.setAttribute("xmlns:xlink", "http://www.w3.org/1999/xlink");
      wmc.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
      wmc.setAttribute("xsi:schemaLocation",
          "http://www.opengis.net/wms http://schemas.opengis.net/wms/1.3.0/capabilities_1_3_0.xsd");
    }
    // //
    // Service
    // //
    Element service = createElement(wmc, "Service");
    wmc.appendChild(service);
    // WMT Defined
    createTextElement(service, "Name", "OGC:WMS");
    createTextElement(service, "Title", "MrGeo Web Map Service");
    createTextElement(service, "Abstract", "MrGeo Web Map Service");
    if (version.isLess("1.3.0"))
    {
      // / TODO Take out ASDF
      Element onlineResource = createElement(service, "OnlineResource");
      onlineResource.setAttribute("xmlns:xlink", "http://www.w3.org/1999/xlink");
      onlineResource.setAttribute("xlink:type", "simple");
      onlineResource.setAttribute("xlink:href", request.getRequestURL().toString());
    }
    else
    {
      createTextElement(service, "OnlineResource", request.getRequestURL().toString());
    }
    createTextElement(service, "Fees", "none");
    createTextElement(service, "AccessConstraints", "none");

    // //
    // Capability
    // //
    Element capability = createElement(wmc, "Capability");

    // Request
    Element requestTag = createElement(capability, "Request");
    // GetCapabilities
    Element getCapabilities = createElement(requestTag, "GetCapabilities");
    createTextElement(getCapabilities, "Format", "application/vnd.ogc.wms_xml");
    Element gcDcpType = createElement(getCapabilities, "DCPType");
    addHttpElement(gcDcpType, request);

    // GetMap
    Element getMap = createElement(requestTag, "GetKml");
    createTextElement(getMap, "Format", KML_MIME_TYPE);
    Element gmDcpType = createElement(getMap, "DCPType");
    addHttpElement(gmDcpType, request);

    // not supported at this time.
    // // GetFeatureInfo

    // Exception
    Element exception = createElement(capability, "Exception");
    createTextElement(exception, "Format", "application/vnd.ogc.se_xml");
    createTextElement(exception, "Format", "application/vnd.ogc.se_inimage");
    createTextElement(exception, "Format", "application/vnd.ogc.se_blank");

    // Layer
    addLayersToCapability(capability, version, providerProperties);

    PrintWriter out = response.getWriter();

    // checkForErrors(doc);
    writeDocument(doc, out);
  }

  /*
   *
   */
  @SuppressWarnings("unused")
  private void getKmz(HttpServletRequest request, HttpServletResponse response) throws IOException
  {
    OpImageRegistrar.registerMrGeoOps();

    String url = request.getRequestURL().toString();
    String serviceParam = request.getParameter("SERVICE");

    String hostParam = getWmsHost(request);

    String layerType = request.getParameter("LAYERS");
    if (layerType == null)
    {
      throw new IllegalArgumentException("Layer type must be specified.");
    }

    String kmlBody;

    String format = KML_MIME_TYPE;

    String hrefString = "<href>" + url + "?LAYERS=" + layerType + "&amp;SERVICE=" + serviceParam
        + "&amp;REQUEST=getviewkml" + "&amp;WMSHOST=" + hostParam + "</href>";

    kmlBody = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<kml xmlns=\"http://www.opengis.net/kml/2.2\">\n" + " <NetworkLink>\n" + "  <name>"
        + layerType + "Link</name>\n" + "  <open>1</open>\n" + "  <Url>\n" + "   " + hrefString
        + "   <viewRefreshMode>onStop</viewRefreshMode>\n"
        + "   <viewRefreshTime>1</viewRefreshTime>\n" + "  </Url>\n"
        + "  <visibility>1</visibility>\n" + " </NetworkLink>\n" + "</kml>\n";

    ZipOutputStream zos = new ZipOutputStream(response.getOutputStream());
    zos.setLevel(9);

    PrintStream kmlStream = new PrintStream(zos);

    kmlStream.println(kmlBody);
    response.setContentType(format);

    response.setContentLength(kmlBody.length());

    kmlStream.close();

  }

  /*
   *
   */
  private void getNetworkKmlNode(HttpServletRequest request, HttpServletResponse response)
      throws IOException
  {
    OpImageRegistrar.registerMrGeoOps();

    String url = request.getRequestURL().toString();
    String serviceParam = request.getParameter("SERVICE");
    String bboxParam = request.getParameter("BBOX");
    String nodeIdParam = request.getParameter("NODEID");
    String levelParam = request.getParameter("LEVELS");
    String resParam = request.getParameter("RESOLUTION");

    int maxLevels = Integer.valueOf(levelParam);
    int resolution = Integer.valueOf(resParam);

    String[] bBoxValues = bboxParam.split(",");
    if (bBoxValues.length != 4)
    {
      throw new IllegalArgumentException("Bounding box must have four comma delimited arguments.");
    }

    double minX = Double.valueOf(bBoxValues[0]);
    double minY = Double.valueOf(bBoxValues[1]);
    double maxX = Double.valueOf(bBoxValues[2]);
    double maxY = Double.valueOf(bBoxValues[3]);

    String[] nodeIdValues = nodeIdParam.split(",");
    if (nodeIdValues.length != 2)
    {
      throw new IllegalArgumentException("Level must have two comma delimited arguments.");
    }

    int levelId = Integer.valueOf(nodeIdValues[0]);
    int nodeId = Integer.valueOf(nodeIdValues[1]);

    String hostParam = getWmsHost(request);

    String layerType = request.getParameter("LAYERS");
    if (layerType == null)
    {
      throw new IllegalArgumentException("Layer type must be specified.");
    }

    String kmlBody;

    String format = KML_MIME_TYPE;

    double aspect = (maxX - minX) / (maxY - minY);
    int width, height;

    if (aspect > 1.0)
    {
      width = resolution;
      height = (int) (resolution / aspect);
    }
    else
    {
      width = (int) (resolution * aspect);
      height = resolution;
    }

    String hrefImgString = "<href>" + baseUrl + "WmsGenerator" + "?LAYERS=" + layerType
        + "&amp;BBOX=" + String.valueOf(minX) + "," + String.valueOf(minY) + ","
        + String.valueOf(maxX) + "," + String.valueOf(maxY) + "&amp;FORMAT=image/png&amp;WIDTH="
        + String.valueOf(width) + "&amp;" + "HEIGHT=" + String.valueOf(height)
        + "&amp;REQUEST=getMap&amp;SERVICE=wms</href>";

    int minLodPix, maxLodPix;

    if (levelId == 0)
    {
      minLodPix = -1;
      maxLodPix = MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT;
    }
    else if (levelId == maxLevels)
    {
      minLodPix = 256;
      maxLodPix = -1;
    }
    else
    {
      minLodPix = 256;
      maxLodPix = MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT;
    }

    kmlBody = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<kml xmlns=\"http://www.opengis.net/kml/2.2\">\n" + "<Document>\n" + " <Region>\n"
        + "   <LatLonAltBox>\n" + "    <north>"
        + String.valueOf(maxY)
        + "</north>\n"
        + "    <south>"
        + String.valueOf(minY)
        + "</south>\n"
        + "    <east>"
        + String.valueOf(maxX)
        + "</east>\n"
        + "    <west>"
        + String.valueOf(minX)
        + "</west>\n"
        + "    <rotation>0</rotation>\n"
        + "   </LatLonAltBox>\n"
        + "   <Lod>\n"
        + "    <minLodPixels>"
        + String.valueOf(minLodPix)
        + "</minLodPixels>\n"
        + "	   <maxLodPixels>"
        + String.valueOf(maxLodPix)
        + "</maxLodPixels>\n"
        + "	   <minFadeExtent>0</minFadeExtent>\n"
        + "	   <maxFadeExtent>0</maxFadeExtent>\n"
        + "   </Lod>\n" + " </Region>\n";

    int childCounter = 0;
    int childId;

    for (double y = minY; y < maxY && levelId <= maxLevels; y += ((maxY - minY) / 2.0))
    {
      for (double x = minX; x < maxX; x += ((maxX - minX) / 2.0))
      {
        childId = nodeId * 2 + childCounter;

        double childMinX, childMinY, childMaxX, childMaxY;
        childMinX = x;
        childMaxX = x + ((maxX - minX) / 2.0);
        childMinY = y;
        childMaxY = y + ((maxY - minY) / 2.0);

        String childHrefKmlString = url + "?LAYERS=" + layerType + "&amp;SERVICE=" + serviceParam
            + "&amp;REQUEST=getkmlnode" + "&amp;WMSHOST=" + hostParam + "&amp;BBOX="
            + String.valueOf(childMinX) + "," + String.valueOf(childMinY) + ","
            + String.valueOf(childMaxX) + "," + String.valueOf(childMaxY) + "&amp;LEVELS="
            + levelParam + "&amp;RESOLUTION=" + resParam + "&amp;NODEID="
            + String.valueOf(levelId + 1) + "," + String.valueOf(childId);

        kmlBody += writeKmlNetworkLink(childMinX, childMinY, childMaxX, childMaxY, levelId,
            childId, childHrefKmlString);

        childCounter++;
      }
    }

    kmlBody += " <GroundOverlay>\n" + "   <drawOrder>0</drawOrder>\n" + "   <Icon>\n"
        + hrefImgString + "\n" + "  </Icon>\n" + " <LatLonBox>\n" + "   <north>"
        + String.valueOf(maxY) + "</north>\n" + "   <south>" + String.valueOf(minY) + "</south>\n"
        + "   <east>" + String.valueOf(maxX) + "</east>\n" + "   <west>" + String.valueOf(minX)
        + "</west>\n" + "   <minAltitude>0</minAltitude>\n" + "   <maxAltitude>0</maxAltitude>\n"
        + " </LatLonBox>\n" + " </GroundOverlay>\n" + "</Document>\n" + "</kml>\n";

    PrintStream kmlStream = new PrintStream(response.getOutputStream());

    kmlStream.println(kmlBody);
    response.setContentType(format);

    response.setContentLength(kmlBody.length());

    kmlStream.close();

  }

  /*
   *
   */
  private void getNetworkKmlRootNode(HttpServletRequest request,
      HttpServletResponse response, final ProviderProperties providerProperties)
      throws IOException
  {
    OpImageRegistrar.registerMrGeoOps();

    String url = request.getRequestURL().toString();
    String serviceParam = request.getParameter("SERVICE");
    String bboxParam = request.getParameter("BBOX");
    String levelParam = request.getParameter("LEVELS");
    String resParam = request.getParameter("RESOLUTION");
    String layer = request.getParameter("LAYERS");

    String headerInfo = "attachment; filename=" + layer + ".kml";
    response.setHeader("Content-Disposition", headerInfo);
    String hostParam = getWmsHost(request);
    String format = KML_MIME_TYPE;
    String[] bBoxValues = bboxParam.split(",");
    if (bBoxValues.length != 4)
    {
      throw new IllegalArgumentException("Bounding box must have four comma delimited arguments.");
    }

    double minX = Double.valueOf(bBoxValues[0]);
    double minY = Double.valueOf(bBoxValues[1]);
    double maxX = Double.valueOf(bBoxValues[2]);
    double maxY = Double.valueOf(bBoxValues[3]);
    Bounds inputBounds = new Bounds(minX, minY, maxX, maxY);

    String kmlBody = getKmlBodyAsString(serviceParam, url, inputBounds, layer, hostParam, levelParam, resParam, providerProperties);
    PrintStream kmlStream = new PrintStream(response.getOutputStream());

    kmlStream.println(kmlBody);
    response.setContentType(format);

    response.setContentLength(kmlBody.length());

    kmlStream.close();

  }

//  public static String getKmlBodyAsString(String service, String url, Bounds inputBounds, String layer, String wmsHost,
//    String levelStr, String res, final ProviderProperties providerProperties) throws IOException {
//    String bbox="";
//    if (layer == null)
//    {
//      throw new IllegalArgumentException("Layer must be specified.");
//    }
//    double minX = inputBounds.getMinX();
//    double minY = inputBounds.getMinY();
//    double maxX = inputBounds.getMaxX();
//    double maxY = inputBounds.getMaxY();
//
//    String baseDir = HadoopUtils.getDefaultImageBaseDirectory();
//    Path filePath = new Path(baseDir, layer);
//    int level = 0;
//    FileSystem fs = HadoopFileUtils.getFileSystem(filePath);
//    if (fs.exists(filePath))
//    {
//      MrsImagePyramid  pyramid = MrsImagePyramid.open(filePath.toString(), providerProperties);
//      if (pyramid != null)
//      {
//        level = pyramid.getNumLevels();
//        if (levelStr == null)
//        {
//          levelStr = String.valueOf(level);
//        }
//        Bounds bounds = pyramid.getBounds();
//        if (bounds.toEnvelope().contains(inputBounds.toEnvelope()))
//        {
//          double layerMinX = bounds.getMinX();
//          double layerMinY = bounds.getMinY();
//          double layerMaxX = bounds.getMaxX();
//          double layerMaxY = bounds.getMaxY();
//          bbox = layerMinX + "," + layerMinY + "," + layerMaxX + "," + layerMaxY;
//        }
//      }
//    }
//
//    String kmlBody;
//
//    //String format = KML_MIME_TYPE;
//
//    String hrefKmlString = "<href>" + url + "?LAYERS=" + layer + "&amp;SERVICE=" + service
//        + "&amp;REQUEST=getkmlnode" + "&amp;WMSHOST=" + wmsHost + "&amp;BBOX=" + bbox
//        + "&amp;LEVELS=" + level + "&amp;RESOLUTION=" + res + "&amp;NODEID=0,0"
//        + "</href>";
//
//    kmlBody = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
//        + "<kml xmlns=\"http://www.opengis.net/kml/2.2\">\n" + "<Document>\n" + "<Region>\n"
//        + "  <LatLonAltBox>\n" + "    <north>"
//        + String.valueOf(maxY)
//        + "</north>\n"
//        + "    <south>"
//        + String.valueOf(minY)
//        + "</south>\n"
//        + "    <east>"
//        + String.valueOf(maxX)
//        + "</east>\n"
//        + "    <west>"
//        + String.valueOf(minX)
//        + "</west>\n"
//        + "  </LatLonAltBox>\n"
//        + "  <Lod>\n"
//        + "    <minLodPixels>256</minLodPixels>\n"
//        + "    <maxLodPixels>-1</maxLodPixels>\n"
//        + "  </Lod>\n"
//        + "</Region>\n"
//        + "<NetworkLink>\n"
//        + "  <name>0</name>\n"
//        + "  <Region>\n"
//        + "    <LatLonAltBox>\n"
//        + "      <north>"
//        + String.valueOf(maxY)
//        + "</north>\n"
//        + "      <south>"
//        + String.valueOf(minY)
//        + "</south>\n"
//        + "      <east>"
//        + String.valueOf(maxX)
//        + "</east>\n"
//        + "      <west>"
//        + String.valueOf(minX)
//        + "</west>\n"
//        + "   </LatLonAltBox>\n"
//        + "   <Lod>\n"
//        + "     <minLodPixels>256</minLodPixels>\n"
//        + "     <maxLodPixels>-1</maxLodPixels>\n"
//        + "   </Lod>\n"
//        + " </Region>\n"
//        + " <Link>\n"
//        + hrefKmlString
//        + "\n"
//        + "   <viewRefreshMode>onRegion</viewRefreshMode>\n"
//        + " </Link>\n"
//        + "</NetworkLink>\n" + "</Document>\n" + "</kml>\n";
//    return kmlBody;
//  }

  public static String getKmlBodyAsString(String service, String url, Bounds inputBounds, String layer, String wmsHost,
      String levelStr, String res, final ProviderProperties providerProperties) throws IOException {
    String bbox="";
    if (layer == null)
    {
      throw new IllegalArgumentException("Layer must be specified.");
    }
    double minX = inputBounds.getMinX();
    double minY = inputBounds.getMinY();
    double maxX = inputBounds.getMaxX();
    double maxY = inputBounds.getMaxY();

    MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(layer, DataProviderFactory.AccessMode.READ, providerProperties);
    MrsImagePyramid  pyramid = MrsImagePyramid.open(dp);

    int level = pyramid.getMaximumLevel();

    Bounds bounds = pyramid.getBounds();
    if (bounds.toEnvelope().contains(inputBounds.toEnvelope()))
    {
      double layerMinX = bounds.getMinX();
      double layerMinY = bounds.getMinY();
      double layerMaxX = bounds.getMaxX();
      double layerMaxY = bounds.getMaxY();
      bbox = layerMinX + "," + layerMinY + "," + layerMaxX + "," + layerMaxY;
    }

    String kmlBody;

    //String format = KML_MIME_TYPE;

    String hrefKmlString = "<href>" + url + "?LAYERS=" + layer + "&amp;SERVICE=" + service
        + "&amp;REQUEST=getkmlnode" + "&amp;WMSHOST=" + wmsHost + "&amp;BBOX=" + bbox
        + "&amp;LEVELS=" + level + "&amp;RESOLUTION=" + res + "&amp;NODEID=0,0"
        + "</href>";

    kmlBody = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<kml xmlns=\"http://www.opengis.net/kml/2.2\">\n" + "<Document>\n" + "<Region>\n"
        + "  <LatLonAltBox>\n" + "    <north>"
        + String.valueOf(maxY)
        + "</north>\n"
        + "    <south>"
        + String.valueOf(minY)
        + "</south>\n"
        + "    <east>"
        + String.valueOf(maxX)
        + "</east>\n"
        + "    <west>"
        + String.valueOf(minX)
        + "</west>\n"
        + "  </LatLonAltBox>\n"
        + "  <Lod>\n"
        + "    <minLodPixels>256</minLodPixels>\n"
        + "    <maxLodPixels>-1</maxLodPixels>\n"
        + "  </Lod>\n"
        + "</Region>\n"
        + "<NetworkLink>\n"
        + "  <name>0</name>\n"
        + "  <Region>\n"
        + "    <LatLonAltBox>\n"
        + "      <north>"
        + String.valueOf(maxY)
        + "</north>\n"
        + "      <south>"
        + String.valueOf(minY)
        + "</south>\n"
        + "      <east>"
        + String.valueOf(maxX)
        + "</east>\n"
        + "      <west>"
        + String.valueOf(minX)
        + "</west>\n"
        + "   </LatLonAltBox>\n"
        + "   <Lod>\n"
        + "     <minLodPixels>256</minLodPixels>\n"
        + "     <maxLodPixels>-1</maxLodPixels>\n"
        + "   </Lod>\n"
        + " </Region>\n"
        + " <Link>\n"
        + hrefKmlString
        + "\n"
        + "   <viewRefreshMode>onRegion</viewRefreshMode>\n"
        + " </Link>\n"
        + "</NetworkLink>\n" + "</Document>\n" + "</kml>\n";
    return kmlBody;
  }
  /*
   *
   */
  private String getWmsHost(HttpServletRequest request) throws MalformedURLException
  {
    String result = request.getParameter("WMSHOST");
    if (result == null)
    {
      URL requestUrl = new URL(request.getRequestURL().toString());
      result = requestUrl.getHost();
      if (requestUrl.getPort() != -1)
      {
        result = String.format("%s:%d", result, requestUrl.getPort());
      }
    }
    return result;
  }

  private void init() throws ServletException
  {
    OpImageRegistrar.registerMrGeoOps();
    try
    {
      baseUrl = Configuration.getInstance().getProperties().getProperty("base.url");
    }
    catch (IllegalStateException e)
    {
      throw new ServletException("Error reading configuration information.", e);
    }
  }


  private void writeDocument(Document doc, PrintWriter out) throws TransformerException
  {
    TransformerFactory transformerFactory = TransformerFactory.newInstance();
    Transformer transformer = transformerFactory.newTransformer();
    transformer.setOutputProperty(OutputKeys.INDENT, "yes");
    transformer.setOutputProperty(OutputKeys.DOCTYPE_SYSTEM,
        "http://schemas.opengis.net/wms/1.1.1/WMS_MS_Capabilities.dtd");
    DOMSource source = new DOMSource(doc);
    StreamResult result = new StreamResult(out);
    transformer.transform(source, result);
  }

  private String writeKmlNetworkLink(double minX, double minY, double maxX, double maxY, int level,
      int id, String hrefContent)
  {
    String nodeString = "";

    nodeString += " <NetworkLink>\n" + "   <name>" + String.valueOf(level) + "_"
        + String.valueOf(id) + "</name>\n" + "   <Region>\n" + "     <LatLonAltBox>\n"
        + "       <north>" + String.valueOf(maxY) + "</north>\n" + "       <south>"
        + String.valueOf(minY) + "</south>\n" + "       <east>" + String.valueOf(maxX)
        + "</east>\n" + "       <west>" + String.valueOf(minX) + "</west>\n"
        + "       <minAltitude>0</minAltitude>\n" + "       <maxAltitude>0</maxAltitude>\n"
        + "     </LatLonAltBox>\n" + "   <Lod>\n" + "	  <minLodPixels>256</minLodPixels>\n"
        + "	  <maxLodPixels>-1</maxLodPixels>\n" + "	  <minFadeExtent>0</minFadeExtent>\n"
        + "	  <maxFadeExtent>0</maxFadeExtent>\n" + "   </Lod>\n" + "   </Region>\n"
        + "   <Link>\n" + "     <href>" + hrefContent + "</href>\n"
        + "     <viewRefreshMode>onRegion</viewRefreshMode>\n" + "   </Link>\n"
        + " </NetworkLink>\n";

    return nodeString;
  }
}
