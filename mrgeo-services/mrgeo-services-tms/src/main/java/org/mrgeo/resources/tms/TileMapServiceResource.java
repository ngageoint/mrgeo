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

/**
 *
 */
package org.mrgeo.resources.tms;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;

import org.apache.commons.lang.StringUtils;
import org.geotools.coverage.CoverageFactoryFinder;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.factory.GeoTools;
import org.geotools.geometry.Envelope2D;
import org.geotools.process.ProcessException;
import org.geotools.process.raster.ContourProcess;
import org.geotools.util.NullProgressListener;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.image.MrsImage;
import org.mrgeo.image.MrsImageException;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.image.MrsImagePyramidMetadata.ImageMetadata;
import org.mrgeo.rasterops.ColorScale;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.tile.TileNotFoundException;
import org.mrgeo.services.Configuration;
import org.mrgeo.services.SecurityUtils;
import org.mrgeo.services.mrspyramid.ColorScaleManager;
import org.mrgeo.services.mrspyramid.rendering.*;
import org.mrgeo.services.tms.TmsService;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.TMSUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Attr;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.activation.MimetypesFileTypeMap;
import javax.imageio.ImageWriter;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.dom.DOMSource;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 *
 */
@Path("/tms")
public class TileMapServiceResource
{

  private static final Logger log = LoggerFactory.getLogger(TileMapServiceResource.class);
  private static final MimetypesFileTypeMap mimeTypeMap = new MimetypesFileTypeMap();
  private static final String VERSION = "1.0.0";
  private static final String SRS = "EPSG:4326";
  private static final String GENERAL_ERROR = "An error occurred in Tile Map Service";
  private static String imageBaseDir = HadoopUtils.getDefaultImageBaseDirectory();
  public static String KML_VERSION = "http://www.opengis.net/kml/2.2";
  public static String KML_EXTENSIONS = "http://www.google.com/kml/ext/2.2";
  public static String KML_MIME_TYPE = "application/vnd.google-earth.kml+xml";
  private static Double MIN_CONTOUR_LENGTH = 0.01;

  @Context
  TmsService service;
  static Properties props;

  static
  {
    init();
  }

  public static void init()
  {
    try
    {
      if (props == null)
      {
        props = Configuration.getInstance().getProperties();
      }
    }
    catch (final IllegalStateException e)
    {
      log.error(MrGeoConstants.MRGEO_HDFS_IMAGE + " must be specified in the MrGeo configuration file (" + e.getMessage() +
          ")");
    }
  }


  protected static Response createEmptyTile(final ImageResponseWriter writer, final int width,
    final int height)
  {
    // return an empty image
    final int dataType;
    if (writer.getResponseMimeType() == "image/jpeg")
    {
      dataType = BufferedImage.TYPE_3BYTE_BGR;
    }
    else
    {
      // dataType = BufferedImage.TYPE_INT_ARGB;
      dataType = BufferedImage.TYPE_4BYTE_ABGR;
    }

    final BufferedImage bufImg = new BufferedImage(width, height, dataType);
    final Graphics2D g = bufImg.createGraphics();
    g.setColor(new Color(0, 0, 0, 0));
    g.fillRect(0, 0, width, height);
    g.dispose();

    return writer.write(bufImg.getData()).build();
  }

  protected static Document mrsPyramidMetadataToTileMapXml(final String raster, final String url,
    final MrsImagePyramidMetadata mpm) throws ParserConfigurationException
    {
    /*
     * String tileMap = "<?xml version='1.0' encoding='UTF-8' ?>" +
     * "<TileMap version='1.0.0' tilemapservice='http://localhost/mrgeo-services/api/tms/1.0.0'>" +
     * "  <Title>AfPk Elevation V2</Title>" + "  <Abstract>A test of V2 MrsImagePyramid.</Abstract>"
     * + "  <SRS>EPSG:4326</SRS>" + "  <BoundingBox minx='68' miny='33' maxx='72' maxy='35' />" +
     * "  <Origin x='68' y='33' />" +
     * "  <TileFormat width='512' height='512' mime-type='image/tiff' extension='tif' />" +
     * "  <TileSets profile='global-geodetic'>" +
     * "    <TileSet href='http://localhost/mrgeo-services/api/tms/1.0.0/AfPkElevationV2/1' units-per-pixel='0.3515625' order='1' />"
     * +
     * "    <TileSet href='http://localhost/mrgeo-services/api/tms/1.0.0/AfPkElevationV2/2' units-per-pixel='0.17578125' order='2' />"
     * +
     * "    <TileSet href='http://localhost/mrgeo-services/api/tms/1.0.0/AfPkElevationV2/3' units-per-pixel='0.08789063' order='3' />"
     * +
     * "    <TileSet href='http://localhost/mrgeo-services/api/tms/1.0.0/AfPkElevationV2/4' units-per-pixel='0.08789063' order='4' />"
     * +
     * "    <TileSet href='http://localhost/mrgeo-services/api/tms/1.0.0/AfPkElevationV2/5' units-per-pixel='0.08789063' order='5' />"
     * +
     * "    <TileSet href='http://localhost/mrgeo-services/api/tms/1.0.0/AfPkElevationV2/6' units-per-pixel='0.08789063' order='6' />"
     * +
     * "    <TileSet href='http://localhost/mrgeo-services/api/tms/1.0.0/AfPkElevationV2/7' units-per-pixel='0.08789063' order='7' />"
     * +
     * "    <TileSet href='http://localhost/mrgeo-services/api/tms/1.0.0/AfPkElevationV2/8' units-per-pixel='0.08789063' order='8' />"
     * +
     * "    <TileSet href='http://localhost/mrgeo-services/api/tms/1.0.0/AfPkElevationV2/9' units-per-pixel='0.08789063' order='9' />"
     * +
     * "    <TileSet href='http://localhost/mrgeo-services/api/tms/1.0.0/AfPkElevationV2/10' units-per-pixel='0.08789063' order='10' />"
     * + "  </TileSets>" + "</TileMap>";
     */

    final DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
    final DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

    // root elements
    final Document doc = docBuilder.newDocument();
    final Element rootElement = doc.createElement("TileMap");
    doc.appendChild(rootElement);
    final Attr v = doc.createAttribute("version");
    v.setValue(VERSION);
    rootElement.setAttributeNode(v);
    final Attr tilemapservice = doc.createAttribute("tilemapservice");
    tilemapservice.setValue(normalizeUrl(normalizeUrl(url).replace(raster, "")));
    rootElement.setAttributeNode(tilemapservice);

    // child elements
    final Element title = doc.createElement("Title");
    title.setTextContent(raster);
    rootElement.appendChild(title);

    final Element abst = doc.createElement("Abstract");
    abst.setTextContent("");
    rootElement.appendChild(abst);

    final Element srs = doc.createElement("SRS");
    srs.setTextContent(SRS);
    rootElement.appendChild(srs);

    final Element bbox = doc.createElement("BoundingBox");
    rootElement.appendChild(bbox);
    final Attr minx = doc.createAttribute("minx");
    minx.setValue(String.valueOf(mpm.getBounds().getMinX()));
    bbox.setAttributeNode(minx);
    final Attr miny = doc.createAttribute("miny");
    miny.setValue(String.valueOf(mpm.getBounds().getMinY()));
    bbox.setAttributeNode(miny);
    final Attr maxx = doc.createAttribute("maxx");
    maxx.setValue(String.valueOf(mpm.getBounds().getMaxX()));
    bbox.setAttributeNode(maxx);
    final Attr maxy = doc.createAttribute("maxy");
    maxy.setValue(String.valueOf(mpm.getBounds().getMaxY()));
    bbox.setAttributeNode(maxy);

    final Element origin = doc.createElement("Origin");
    rootElement.appendChild(origin);
    final Attr x = doc.createAttribute("x");
    x.setValue(String.valueOf(mpm.getBounds().getMinX()));
    origin.setAttributeNode(x);
    final Attr y = doc.createAttribute("y");
    y.setValue(String.valueOf(mpm.getBounds().getMinY()));
    origin.setAttributeNode(y);

    final Element tileformat = doc.createElement("TileFormat");
    rootElement.appendChild(tileformat);
    final Attr w = doc.createAttribute("width");
    w.setValue(String.valueOf(mpm.getTilesize()));
    tileformat.setAttributeNode(w);
    final Attr h = doc.createAttribute("height");
    h.setValue(String.valueOf(mpm.getTilesize()));
    tileformat.setAttributeNode(h);
    final Attr mt = doc.createAttribute("mime-type");
    mt.setValue("image/tiff");
    tileformat.setAttributeNode(mt);
    final Attr ext = doc.createAttribute("extension");
    ext.setValue("tif");
    tileformat.setAttributeNode(ext);

    final Element tilesets = doc.createElement("TileSets");
    rootElement.appendChild(tilesets);
    final Attr profile = doc.createAttribute("profile");
    profile.setValue("global-geodetic");
    tilesets.setAttributeNode(profile);

    for (int i = 0; i <= mpm.getMaxZoomLevel(); i++)
    {
      final Element tileset = doc.createElement("TileSet");
      tilesets.appendChild(tileset);
      final Attr href = doc.createAttribute("href");
      href.setValue(normalizeUrl(normalizeUrl(url)) + "/" + i);
      tileset.setAttributeNode(href);
      final Attr upp = doc.createAttribute("units-per-pixel");
      upp.setValue(String.valueOf(180d / 256d / Math.pow(2, i)));
      tileset.setAttributeNode(upp);
      final Attr order = doc.createAttribute("order");
      order.setValue(String.valueOf(i));
      tileset.setAttributeNode(order);
    }

    return doc;
    }

  protected static Document mrsPyramidTileToContourKml(final Raster tile,
    final Envelope2D envelope, final Double interval) throws ProcessException,
    ParserConfigurationException
    {

    final DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
    final DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

    // root elements
    final Document doc = docBuilder.newDocument();
    final Element rootElement = doc.createElement("kml");
    doc.appendChild(rootElement);
    final Attr ns = doc.createAttribute("xmlns");
    ns.setValue(KML_VERSION);
    rootElement.setAttributeNode(ns);
    /*
     * Attr gxns = doc.createAttribute("xmlns:gx"); gxns.setValue(KML_EXTENSIONS);
     * rootElement.setAttributeNode(gxns);
     */
    // child elements
    final Element d = doc.createElement("Document");
    rootElement.appendChild(d);

    final Element nm = doc.createElement("name");
    nm.setTextContent("contours");
    d.appendChild(nm);
    final Element dsc = doc.createElement("description");
    dsc.setTextContent("interval = " + interval);
    d.appendChild(dsc);

    final Element sty = doc.createElement("Style");
    d.appendChild(sty);
    final Element lsty = doc.createElement("ListStyle");
    sty.appendChild(lsty);
    final Element lit = doc.createElement("listItemType");
    lit.setTextContent("checkHideChildren");
    lsty.appendChild(lit);

    final Element normalStyle = doc.createElement("Style");
    normalStyle.setAttribute("id", "normalState");
    final Element iconStyle = doc.createElement("IconStyle");
    final Element icon = doc.createElement("Icon");
    final Element scale = doc.createElement("scale");
    scale.setTextContent("0");
    iconStyle.appendChild(scale.cloneNode(true));
    iconStyle.appendChild(icon.cloneNode(true));
    final Element highlightScale = doc.createElement("scale");
    highlightScale.setTextContent("1.0");
    final Element highlightColor = doc.createElement("color");
    highlightColor.setTextContent("ffffffff");
    final Element labelStyle = doc.createElement("LabelStyle");
    labelStyle.appendChild(scale.cloneNode(true));
    final Element highlightStyle = doc.createElement("Style");
    highlightStyle.setAttribute("id", "highlightState");
    final Element contourStyle = doc.createElement("StyleMap");
    contourStyle.setAttribute("id", "styleMapContour");
    final Element normalPair = doc.createElement("Pair");
    final Element highlightPair = doc.createElement("Pair");
    final Element normalKey = doc.createElement("key");
    normalKey.setTextContent("normal");
    final Element highlightKey = doc.createElement("key");
    highlightKey.setTextContent("highlight");
    final Element normalStyleUrl = doc.createElement("styleUrl");
    normalStyleUrl.setTextContent("#normalState");
    final Element highlightStyleUrl = doc.createElement("styleUrl");
    highlightStyleUrl.setTextContent("#highlightState");

    d.appendChild(normalStyle);
    normalStyle.appendChild(iconStyle.cloneNode(true));
    normalStyle.appendChild(labelStyle.cloneNode(true));

    d.appendChild(highlightStyle);
    highlightStyle.appendChild(iconStyle);
    final Element highlightLabelStyle = doc.createElement("LabelStyle");
    highlightStyle.appendChild(highlightLabelStyle);
    highlightLabelStyle.appendChild(highlightScale);
    highlightLabelStyle.appendChild(highlightColor);
    final Element highlightLineStyle = doc.createElement("LineStyle");
    highlightStyle.appendChild(highlightLineStyle);
    final Element highlightLineWidth = doc.createElement("width");
    highlightLineWidth.setTextContent("2");
    highlightLineStyle.appendChild(highlightLineWidth);
    // Element highlightLabelVis = doc.createElement("gx:labelVisibility");
    // highlightLabelVis.setTextContent("1");
    // highlightLineStyle.appendChild(highlightLabelVis);

    d.appendChild(contourStyle);
    contourStyle.appendChild(normalPair);
    normalPair.appendChild(normalKey);
    normalPair.appendChild(normalStyleUrl);

    contourStyle.appendChild(highlightPair);
    highlightPair.appendChild(highlightKey);
    highlightPair.appendChild(highlightStyleUrl);

    final WritableRaster wRaster = tile.createCompatibleWritableRaster();
    wRaster.setRect(-tile.getMinX(), -tile.getMinY(), tile);
    final GridCoverageFactory factory = CoverageFactoryFinder.getGridCoverageFactory(GeoTools
      .getDefaultHints());
    final GridCoverage2D coverage = factory.create("GridCoverage", wRaster, envelope);

    final ContourProcess cp = new ContourProcess();
    final SimpleFeatureCollection features = cp.execute(coverage, 0, null,
      Double.valueOf(interval), true, true, null, new NullProgressListener());

    final SimpleFeatureIterator iterator = features.features();
    try
    {
      while (iterator.hasNext())
      {
        final SimpleFeature feature = iterator.next();
        final LineString geom = (LineString) feature.getDefaultGeometry();
        if (geom.getLength() > MIN_CONTOUR_LENGTH)
        {
          final Double attrValue = (Double) feature.getAttribute("value");

          final Element plm = doc.createElement("Placemark");
          d.appendChild(plm);
          final Element plmStyle = doc.createElement("styleUrl");
          plmStyle.setTextContent("#styleMapContour");
          plm.appendChild(plmStyle);
          final Element plmname = doc.createElement("name");
          plmname.setTextContent(String.valueOf(attrValue));
          plm.appendChild(plmname);

          final Element mgeom = doc.createElement("MultiGeometry");
          plm.appendChild(mgeom);

          final Element line = doc.createElement("LineString");
          mgeom.appendChild(line);
          final Element coords = doc.createElement("coordinates");

          final Coordinate[] linecoords = geom.getCoordinates();
          final List<String> coordList = new ArrayList<String>();
          for (int i = 0; i < linecoords.length; i++)
          {
            final Coordinate coo = linecoords[i];
            coordList.add(coo.x + "," + coo.y + "," + "0");

            // Add a point label every 5000th vertice
            if (i == 0 || i % 5000 == 0)
            {
              final Element pt = doc.createElement("Point");
              mgeom.appendChild(pt);
              final Element ptcoords = doc.createElement("coordinates");
              ptcoords.setTextContent(coo.x + "," + coo.y + "," + "0");
              pt.appendChild(ptcoords);
            }

          }
          coords.setTextContent(StringUtils.join(coordList, "\n"));

          line.appendChild(coords);
        }
      }
    }
    finally
    {
      iterator.close();
    }

    return doc;
    }

  protected static Document mrsPyramidToTileMapServiceXml(final String url,
    final List<String> pyramidNames) throws ParserConfigurationException,
    DOMException, UnsupportedEncodingException
  {
    /*
     * String tileMapService = "<?xml version='1.0' encoding='UTF-8' ?>" +
     * "<TileMapService version='1.0.0' services='http://localhost/mrgeo-services/api/tms/'>" +
     * "  <Title>Example Tile Map Service</Title>" +
     * "  <Abstract>This is a longer description of the example tiling map service.</Abstract>" +
     * "  <TileMaps>" + "    <TileMap " + "      title='AfPk Elevation V2' " +
     * "      srs='EPSG:4326' " + "      profile='global-geodetic' " +
     * "      href='http:///localhost/mrgeo-services/api/tms/1.0.0/AfPkElevationV2' />" +
     * "  </TileMaps>" + "</TileMapService>";
     */

    final DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
    final DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

    // root elements
    final Document doc = docBuilder.newDocument();
    final Element rootElement = doc.createElement("TileMapService");
    doc.appendChild(rootElement);
    final Attr v = doc.createAttribute("version");
    v.setValue(VERSION);
    rootElement.setAttributeNode(v);
    final Attr service = doc.createAttribute("services");
    service.setValue(normalizeUrl(normalizeUrl(url).replace(VERSION, "")));
    rootElement.setAttributeNode(service);

    // child elements
    final Element title = doc.createElement("Title");
    title.setTextContent("Tile Map Service");
    rootElement.appendChild(title);

    final Element abst = doc.createElement("Abstract");
    abst.setTextContent("MrGeo MrsImagePyramid rasters available as TMS");
    rootElement.appendChild(abst);

    final Element tilesets = doc.createElement("TileMaps");
    rootElement.appendChild(tilesets);

    Collections.sort(pyramidNames);
    for (final String p : pyramidNames)
    {
      final Element tileset = doc.createElement("TileMap");
      tilesets.appendChild(tileset);
      final Attr href = doc.createAttribute("href");
      href.setValue(normalizeUrl(url) + "/" + URLEncoder.encode(p, "UTF-8"));
      tileset.setAttributeNode(href);
      final Attr maptitle = doc.createAttribute("title");
      maptitle.setValue(p);
      tileset.setAttributeNode(maptitle);
      final Attr srs = doc.createAttribute("srs");
      srs.setValue(SRS);
      tileset.setAttributeNode(srs);
      final Attr profile = doc.createAttribute("profile");
      profile.setValue("global-geodetic");
      tileset.setAttributeNode(profile);
    }

    return doc;
  }

  protected static String normalizeUrl(final String url)
  {
    String newUrl;
    newUrl = (url.lastIndexOf("/") == url.length() - 1) ? url.substring(0, url.length() - 1) : url;
    return newUrl;
  }

  protected static Document rootResourceXml(final String url) throws ParserConfigurationException
  {
    /*
     * <?xml version="1.0" encoding="UTF-8" ?> <Services> <TileMapService
     * title="MrGeo Tile Map Service" version="1.0.0"
     * href="http://localhost:8080/mrgeo-services/api/tms/1.0.0" /> </Services>
     */

    final DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
    final DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
    final Document doc = docBuilder.newDocument();
    final Element rootElement = doc.createElement("Services");
    doc.appendChild(rootElement);
    final Element tms = doc.createElement("TileMapService");
    rootElement.appendChild(tms);
    final Attr title = doc.createAttribute("title");
    title.setValue("MrGeo Tile Map Service");
    tms.setAttributeNode(title);
    final Attr v = doc.createAttribute("version");
    v.setValue(VERSION);
    tms.setAttributeNode(v);
    final Attr href = doc.createAttribute("href");
    href.setValue(normalizeUrl(url) + "/" + VERSION);
    tms.setAttributeNode(href);

    return doc;
  }

  @GET
  @Produces("text/xml")
  public Response getRootResource(@Context final HttpServletRequest hsr)
  {
    try
    {
      final String url = hsr.getRequestURL().toString();
      final Document doc = rootResourceXml(url);
      final DOMSource source = new DOMSource(doc);
      return Response.ok(source, "text/xml").header("Content-type", "text/xml").build();

    }
    catch (final ParserConfigurationException ex)
    {
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(GENERAL_ERROR).build();
    }

  }

  // @GET
  // @Produces("image/*")
  // @Path("old/{version}/{raster}/{z}/{x}/{y}.{format}")
  // public Response oldGetTile(@PathParam("version") final String version,
  // @PathParam("raster") String raster, @PathParam("z") final Integer z,
  // @PathParam("x") final Integer x, @PathParam("y") final Integer y,
  // @PathParam("format") final String format,
  // @QueryParam("color-scale-name") final String colorScaleName,
  // @QueryParam("color-scale") final String colorScale, @QueryParam("min") final Double min,
  // @QueryParam("max") final Double max,
  // @DefaultValue("1") @QueryParam("maskMax") final Double maskMax,
  // @QueryParam("mask") final String mask)
  // {
  //
  // try
  // {
  // // check the request format
  // final int formatHash = format.hashCode();
  // ImageWriter writer;
  // switch (formatHash)
  // {
  // case 114833:
  // writer = ImageUtils.createImageWriter("image/tiff");
  // break;
  // case 111145:
  // writer = ImageUtils.createImageWriter("image/png");
  // break;
  // case 105441:
  // writer = ImageUtils.createImageWriter("image/jpeg");
  // break;
  // default:
  // return Response.status(Status.BAD_REQUEST).entity("Unsupported image format - " + format)
  // .build();
  // }
  //
  // if (Base64.isBase64(raster))
  // {
  // raster = new String(Base64.decode(raster));
  // }
  //
  // // Check cache for metadata, if not found read from pyramid
  // // and store in cache
  // MrsImagePyramidMetadata mpm;
  // try
  // {
  // mpm = service.getMetadata(raster);
  // final ImageMetadata zlm = mpm.getImageMetadata()[z];
  // if (zlm.tileBounds == null)
  // {
  // return Response.status(Status.NOT_FOUND).entity("Tile not found").build();
  // }
  // if (!zlm.tileBounds.contains(x, y))
  // {
  // return returnEmptyTile(writer, mpm.getTilesize(), mpm.getTilesize(), format);
  // }
  // }
  // catch (final ExecutionException e)
  // {
  // return Response.status(Status.NOT_FOUND).entity("Tile map not found - " + raster).build();
  // }
  //
  // OpImageRegistrar.registerMrGeoOps();
  //
  // // FIXME: Will we ever have to support multiband images here?
  // double[] extrema = new double[2];
  // final ImageStats stats = mpm.getStats(0);
  //
  // if (min != null && max != null)
  // {
  // extrema[0] = min;
  // extrema[1] = max;
  // }
  // else if (stats != null)
  // {
  // extrema[0] = stats.min;
  // extrema[1] = stats.max;
  //
  // // Check for min/max override values from the request
  // if (min != null)
  // {
  // extrema[0] = min;
  // }
  // if (max != null)
  // {
  // extrema[1] = max;
  // }
  // }
  // else
  // {
  // extrema = null;
  // }
  //
  // final double transparentValue = mpm.getDefaultValue(0);
  //
  // ColorScale cs = null;
  // try
  // {
  // cs = service.getColorScale(colorScale, colorScaleName, raster, new ColorScaleInfo.Builder()
  // .transparent(transparentValue).extrema(extrema));
  // }
  // catch (final Exception te)
  // {
  // return Response.status(Status.NOT_FOUND).type("text/plain").entity(te.getMessage()).build();
  // }
  //
  // // load the pyramid, and get the TMS tile if it exists
  // // or else return blank image
  // final MrsImagePyramid mp = service.getPyramid(raster);
  // final MrsImage zImage = mp.getImage(z);
  // // If the zImage is null that means the pyramid level does not exist
  // if (zImage == null)
  // {
  // return Response.status(Status.NOT_FOUND).build();
  // }
  // final int width = zImage.getTileWidth();
  // final int height = zImage.getTileHeight();
  //
  // BufferedImage bufImg;
  // if (zImage.isTileEmpty(x, y))
  // {
  // return returnEmptyTile(writer, width, height, format);
  // }
  //
  // try
  // {
  // final Raster xyTile = zImage.getTile(x, y);
  //
  // final WritableRaster wr = Raster.createWritableRaster(xyTile.getSampleModel(), xyTile
  // .getDataBuffer(), null);
  // final ColorModel cm = RasterUtils.createColorModel(xyTile);
  // bufImg = new BufferedImage(cm, wr, false, null);
  //
  // final ByteArrayOutputStream baos = new ByteArrayOutputStream();
  //
  // // Apply mask if requested
  // if (mask != null && !mask.isEmpty())
  // {
  // final int b = 0;
  // final MrsImagePyramidMetadata maskMpm = service.getMetadata(mask);
  //
  // final MrsImagePyramid maskMp = service.getPyramid(mask);
  // final MrsImage maskZImage = maskMp.getImage(z);
  // if (maskZImage != null)
  // {
  // try
  // {
  // final Raster maskXyTile = maskZImage.getTile(x, y);
  // for (int w = 0; w < maskXyTile.getWidth(); w++)
  // {
  // for (int h = 0; h < maskXyTile.getHeight(); h++)
  // {
  // final double maskPixel = maskXyTile.getSampleDouble(w, h, b);
  // if (maskPixel > maskMax ||
  // Double.compare(maskPixel, maskMpm.getDefaultValue(b)) == 0)
  // {
  // wr.setSample(w, h, b, mpm.getDefaultValue(b));
  // }
  // }
  // }
  // }
  // catch (final TileNotFoundException ex)
  // {
  // // If not mask tile exists, a blank tile should be returned
  // return returnEmptyTile(writer, width, height, format);
  // }
  // finally
  // {
  // maskZImage.close();
  // }
  // }
  // }
  //
  // // return a non-colormapped geotiff for tiff format
  // switch (formatHash)
  // {
  // case 114833:
  // final Bounds bnds = new Bounds(TMSUtils.tileBoundsArray(x, y, z, width));
  // GeoTiffExporter.export(bufImg, bnds, baos, false, null, mpm.getDefaultValue(0));
  // break;
  // case 111145:
  // final MemoryCacheImageOutputStream imageStreamPng = new MemoryCacheImageOutputStream(baos);
  // writer.setOutput(imageStreamPng);
  // // Assume 3 band images are RGB and should NOT have a color scale applied
  // if (mpm.getBands() == 3)
  // {
  // // Read nodata from metadata and assign to color
  // final int[] defaultValues = mpm.getDefaultValuesInt();
  // final Color nodataColor = new Color(defaultValues[0], defaultValues[1],
  // defaultValues[2]);
  //
  // writer.write(ImageUtils.imageToRgbaBufferedImage(ImageUtils.makeColorTransparent(
  // bufImg, nodataColor)));
  // }
  // else
  // {
  // // writer.write(ImageUtils.bufferRenderedImage(
  // // ColorScaleApplier.applyToRgba(bufImg, cs)
  // // ));
  // new PngImageResponseWriter().writeToStream(org.mrgeo.rasterops.ColorScaleApplier
  // .applyToRgba(bufImg, cs).getData(), baos);
  // }
  // imageStreamPng.close();
  // break;
  // case 105441:
  // final MemoryCacheImageOutputStream imageStreamJpg = new MemoryCacheImageOutputStream(baos);
  // writer.setOutput(imageStreamJpg);
  // // Assume 3 band images are RGB and should NOT have a color scale applied
  // if (mpm.getBands() == 3)
  // {
  // writer.write(bufImg);
  // }
  // else
  // {
  // writer.write(org.mrgeo.rasterops.ColorScaleApplier.applyToRgb(bufImg, cs));
  // }
  // imageStreamJpg.close();
  // break;
  // default:
  // return Response.status(Status.BAD_REQUEST).entity("Unsupported image format - " + format)
  // .build();
  // }
  // final byte[] imageData = baos.toByteArray();
  // baos.close();
  //
  // final String type = mimeTypeMap.getContentType("output." + format);
  // return Response.ok(imageData).header("Content-Type", type).build();
  // }
  // catch (final TileNotFoundException ex)
  // {
  // // requested tile is outside of image bounds,
  // // return a blank tile
  // return returnEmptyTile(writer, width, height, format);
  // }
  // finally
  // {
  // zImage.close();
  // }
  // }
  // catch (final Exception e)
  // {
  // log.error("Exception occurred getting tile " + raster + "/" + z + "/" + x + "/" + y + "." +
  // format, e);
  // return Response.status(Status.INTERNAL_SERVER_ERROR).entity(GENERAL_ERROR).build();
  // }
  // }

  @SuppressWarnings("static-method")
  @GET
  @Produces("image/*")
  @Path("{version}/{raster}/{z}/{x}/{y}.{format}")
  public Response getTile(@PathParam("version") final String version,
    @PathParam("raster") String pyramid, @PathParam("z") final Integer z,
    @PathParam("x") final Integer x, @PathParam("y") final Integer y,
    @PathParam("format") final String format,
    @QueryParam("color-scale-name") final String colorScaleName,
    @QueryParam("color-scale") final String colorScale, @QueryParam("min") final Double min,
    @QueryParam("max") final Double max,
    @DefaultValue("1") @QueryParam("maskMax") final Double maskMax,
    @QueryParam("mask") final String mask)
  {

    final ImageRenderer renderer;
    Raster raster;

    try
    {
      renderer = (ImageRenderer) ImageHandlerFactory.getHandler(format, ImageRenderer.class);
      
      // TODO: Need to construct provider properties from the WebRequest using
      // a new security layer and pass those properties.
      // Apply mask if requested
      Properties providerProperties = SecurityUtils.getProviderProperties();
      if (mask != null && !mask.isEmpty())
      {
        raster = renderer.renderImage(pyramid, x, y, z, mask, maskMax, providerProperties);
      }
      else
      {
        raster = renderer.renderImage(pyramid, x, y, z, providerProperties);
      }
      if (!(renderer instanceof TiffImageRenderer) && raster.getNumBands() != 3 &&
          raster.getNumBands() != 4)
      {
        ColorScale cs = null;
        if (colorScaleName != null)
        {
          cs = ColorScaleManager.fromName(colorScaleName, props);
        }
        else if (colorScale != null)
        {
          cs = ColorScaleManager.fromJSON(colorScale);
        }
//        else
//        {
//          cs = ColorScaleManager.fromPyramid(pyramid, driver);
//        }

        final double[] extrema = renderer.getExtrema();

        // Check for min/max override values from the request
        if (min != null)
        {
          extrema[0] = min;
        }
        if (max != null)
        {
          extrema[1] = max;
        }

        raster = ((ColorScaleApplier) ImageHandlerFactory.getHandler(format,
          ColorScaleApplier.class)).applyColorScale(raster, cs, extrema, renderer
            .getDefaultValues());
      }

      // Apply mask if requested
//      if (mask != null && !mask.isEmpty())
//      {
//        try
//        {
//          final MrsImagePyramidMetadata maskMetadata = service.getMetadata(mask);
//
//          final Raster maskRaster = renderer.renderImage(mask, x, y, z, props, driver);
//          final WritableRaster wr = RasterUtils.makeRasterWritable(raster);
//
//          final int band = 0;
//          final double nodata = maskMetadata.getDefaultValue(band);
//
//          for (int w = 0; w < maskRaster.getWidth(); w++)
//          {
//            for (int h = 0; h < maskRaster.getHeight(); h++)
//            {
//              final double maskPixel = maskRaster.getSampleDouble(w, h, band);
//              if (maskPixel > maskMax || Double.compare(maskPixel, nodata) == 0)
//              {
//                wr.setSample(w, h, band, nodata);
//              }
//            }
//          }
//        }
//        catch (final TileNotFoundException ex)
//        {
//          raster = RasterUtils.createEmptyRaster(raster.getWidth(), raster.getHeight(), raster
//            .getNumBands(), raster.getTransferType(), 0);
//        }
//      }

      return ((ImageResponseWriter) ImageHandlerFactory.getHandler(format,
        ImageResponseWriter.class)).write(raster, renderer.getDefaultValues()).build();

    }
    catch (final IllegalArgumentException e)
    {
      return Response.status(Status.BAD_REQUEST).entity("Unsupported image format - " + format)
          .build();
    }
    catch (final IOException e)
    {
      return Response.status(Status.NOT_FOUND).entity("Tile map not found - " + pyramid).build();
    }
    catch (final MrsImageException e)
    {
      return Response.status(Status.NOT_FOUND).entity("Tile map not found - " + pyramid + ": " + z)
          .build();
    }
    catch (final TileNotFoundException e)
    {
      // return Response.status(Status.NOT_FOUND).entity("Tile not found").build();
      try
      {
        final MrsImagePyramidMetadata metadata = service.getMetadata(pyramid);

        return createEmptyTile(((ImageResponseWriter) ImageHandlerFactory.getHandler(format,
          ImageResponseWriter.class)), metadata.getTilesize(), metadata.getTilesize());
      }
      catch (final Exception e1)
      {
        log.error("Exception occurred creating blank tile " + pyramid + "/" + z + "/" + x + "/" +
            y + "." + format, e1);
      }
    }
    catch (final ColorScale.BadJSONException e)
    {
      return Response.status(Status.NOT_FOUND).entity("Unable to parse color scale JSON").build();

    }
    catch (final ColorScale.BadSourceException e)
    {
      return Response.status(Status.NOT_FOUND).entity("Unable to open color scale file").build();
    }
    catch (final ColorScale.BadXMLException e)
    {
      return Response.status(Status.NOT_FOUND).entity("Unable to parse color scale XML").build();
    }
    catch (final ColorScale.ColorScaleException e)
    {
      return Response.status(Status.NOT_FOUND).entity("Unable to open color scale").build();
    }
    catch (final Exception e)
    {
      log.error("Exception occurred getting tile " + pyramid + "/" + z + "/" + x + "/" + y + "." +
          format, e);
    }

    return Response.status(Status.INTERNAL_SERVER_ERROR).entity(GENERAL_ERROR).build();
  }

  @GET
  @Produces("text/xml")
  @Path("/{version}/{raster}/{z}/{x}/{y}/{interval}.kml")
  public Response getTileContours(@PathParam("version") final String version,
    @PathParam("raster") String raster, @PathParam("z") final Integer z,
    @PathParam("x") final Integer x, @PathParam("y") final Integer y,
    @PathParam("interval") final Double interval)
  {

    try
    {
      // Check cache for metadata, if not found read from pyramid
      // and store in cache
      MrsImagePyramidMetadata mpm;
      try
      {
        mpm = service.getMetadata(raster);
        // If not close enough to max zoom level, bail out
        if (mpm.getMaxZoomLevel() != z)
        {
          return Response.ok("<kml/>").header("Content-type", "text/xml").build();
        }
        final ImageMetadata zlm = mpm.getImageMetadata()[z];
        if (zlm.tileBounds == null)
        {
          return Response.status(Status.NOT_FOUND).entity("Tile not found").build();
        }
        if (!zlm.tileBounds.contains(x, y))
        {
          return Response.status(Status.NOT_FOUND).entity("Tile is empty").build();
        }
      }
      catch (final ExecutionException e)
      {
        return Response.status(Status.NOT_FOUND).entity("Tile map not found - " + raster).build();
      }

      OpImageRegistrar.registerMrGeoOps();

      // load the pyramid, and get the TMS tile if it exists
      // or else return blank image
      final MrsImagePyramid mp = service.getPyramid(raster);
      final MrsImage zImage = mp.getImage(z);
      // If the zImage is null that means the pyramid level does not exist
      if (zImage == null)
      {
        return Response.status(Status.NOT_FOUND).build();
      }
      if (zImage.isTileEmpty(x, y))
      {
        return Response.status(Status.NOT_FOUND).entity("Tile is empty").build();
      }

      try
      {
        final Raster xyTile = zImage.getTile(x, y);
        final Bounds tileBounds = TMSUtils.tileBounds(x, y, z, mpm.getTilesize())
            .convertNewToOldBounds();
        final Envelope2D envelope = new Envelope2D();
        envelope.setRect(tileBounds.toRectangle2D());

        final Document kml = mrsPyramidTileToContourKml(xyTile, envelope, interval);

        // return Response.ok(kml, new
        // MimetypesFileTypeMap().getContentType("kml")).header("Content-type",
        // KmlService.KML_MIME_TYPE).build();
        return Response.ok(kml).header("Content-type", "text/xml").build();

      }
      catch (final IllegalArgumentException ex)
      {
        // requested tile is outside of image bounds,
        // return a blank tile
        return Response.status(Status.NOT_FOUND).entity("Tile is empty").build();
      }
      finally
      {
        zImage.close();
      }
    }
    catch (final Exception e)
    {
      log.error("Exception occurred getting tile " + raster + "/" + z + "/" + x + "/" + y + ".kml",
        e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(GENERAL_ERROR).build();
    }
  }

  @GET
  @Produces("text/xml")
  @Path("/{version}/{raster}")
  public Response getTileMap(@PathParam("version") final String version,
    @PathParam("raster") String raster, @Context final HttpServletRequest hsr)
  {
    try
    {
      final String url = hsr.getRequestURL().toString();
      // Check cache for metadata, if not found read from pyramid
      // and store in cache
      final MrsImagePyramidMetadata mpm = service.getMetadata(raster);
      final Document doc = mrsPyramidMetadataToTileMapXml(raster, url, mpm);
      final DOMSource source = new DOMSource(doc);

      return Response.ok(source, "text/xml").header("Content-type", "text/xml").build();

    }
    catch (final ExecutionException e)
    {
      log.error("MrsImagePyramid " + raster + " not found", e);
      return Response.status(Status.NOT_FOUND).entity("Tile map not found - " + raster).build();
    }
    catch (final ParserConfigurationException ex)
    {
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(GENERAL_ERROR).build();
    }
  }

  @GET
  @Produces("text/xml")
  @Path("/{version}")
  public Response getTileMapService(@PathParam("version") final String version,
    @Context final HttpServletRequest hsr)
  {
    try
    {
      final String url = hsr.getRequestURL().toString();
      final Document doc = mrsPyramidToTileMapServiceXml(url, service.listImages());
      final DOMSource source = new DOMSource(doc);

      return Response.ok(source, "text/xml").header("Content-type", "text/xml").build();

    }
    catch (final IOException e)
    {
      log.error("File system exception for " + imageBaseDir, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(GENERAL_ERROR).build();
    }
    catch (final ParserConfigurationException ex)
    {
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(GENERAL_ERROR).build();
    }
  }

  protected Response returnEmptyTile(final ImageWriter writer, final int width, final int height,
    final String format) throws IOException
    {
    // return an empty image
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    int dataType;
    if (format.equalsIgnoreCase("jpg"))
    {
      dataType = BufferedImage.TYPE_3BYTE_BGR;
    }
    else
    {
      // dataType = BufferedImage.TYPE_INT_ARGB;
      dataType = BufferedImage.TYPE_4BYTE_ABGR;
    }
    final BufferedImage bufImg = new BufferedImage(width, height, dataType);
    final Graphics2D g = bufImg.createGraphics();
    g.setColor(new Color(0, 0, 0, 0));
    g.fillRect(0, 0, width, height);
    g.dispose();

    final MemoryCacheImageOutputStream imageStream = new MemoryCacheImageOutputStream(baos);
    writer.setOutput(imageStream);
    writer.write(bufImg);
    imageStream.close();
    final byte[] imageData = baos.toByteArray();
    baos.close();
    final String type = mimeTypeMap.getContentType("output." + format);
    return Response.ok(imageData).header("Content-Type", type).build();

    // A 404 - Not Found response may be the most appropriate, but results in pink tiles,
    // maybe change that behavior on the OpenLayers client?
    // return Response.status( Response.Status.NOT_FOUND).build();

    }

}
