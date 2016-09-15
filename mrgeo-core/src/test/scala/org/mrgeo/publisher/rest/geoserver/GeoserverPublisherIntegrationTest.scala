package org.mrgeo.publisher.rest.geoserver

import com.fasterxml.jackson.databind.ObjectMapper
import org.mrgeo.core.MrGeoProperties
import org.mrgeo.image.MrsPyramidMetadata
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.mapalgebra.utils.RasterMapOpTestSupport
import org.mrgeo.publisher.MrGeoPublisherFactory
import org.mrgeo.publisher.PublisherProfileConfigProperties._
import org.mrgeo.publisher.rest.RestClient
import org.mrgeo.publisher.rest.geoserver.GeoserverPublisherConfigurator._
import org.mrgeo.utils.tms.{Bounds, TMSUtils}
import org.mrgeo.utils.{GDALUtils, LongRectangle}
import org.scalatest
import org.scalatest.{BeforeAndAfter, FlatSpec, Ignore}

import scala.collection.JavaConverters._

/**
  * Created by ericwood on 8/17/16.
  */

/**
  * Integration test to verify publishing of images to geoserver.
  *
  * Note that this test expects geoserver to by at an endpoint geoserver:8080.  You host file may need to be altered
  * to point the alias the geoserver being tested to that endpoint.
  */
@Ignore
class GeoserverPublisherIntegrationTest extends FlatSpec with BeforeAndAfter with RasterMapOpTestSupport {

  private val profileName = "mrgeo-geoserver-publisher-integration-test"
  private val geoserverUrl = "http://localhost:8080/geoserver/rest" // http://geoserver:8080/geoserver/rest"
  private val workspace = "mrgeo-geoserver-publisher-integration-test-workspace"
  private val namespace = "mrgeo-geoserver-publisher-integration-test-namespace"
  private val namespaceUri = "http://mrgeo-geoserver-publisher-integration-test-namespace-uri"
  private val coverageStoreName = "mrgeo-geoserver-publisher-integration-test-coverageStoreName"
  private val mrgeoMasterPassword = "master"
  private val geoserverUsername = "admin"
  private val encryptedGeoserverPassword = "7sCorK/tevCoO9YZC2R73p5eAhitf1EO"
  private val descriptionPrefix = "Created from MrGeo image "

  private val pyramidName = "org.mrgeo.rest.geoserver.GeoserverPublisherIntegratonTest.pyramidName"
  private val epsg = "EPSG:4326"
  // This needs to be initialized after the master password is set, otherwise it will prime the properties instance and
  // diasable property decryption
  private var nativeCRSString: String = _
  private val projectionPolicy = "REPROJECT_TO_DECLARED"
  private val nativeFormat = "GEOTIFF"
  private val supportedFormats = List("GIF", "PNG", "JPEG", "TIFF", "ImageMosaic", "GEOTIFF", "ArcGrid", "Gtopo30")

  private val tileIds: Array[Long] = Array(11, 12, 19, 20)
  private val zoomLevel = 3
  private val tileSize = 512
  private val bounds: Bounds = new Bounds(-44, -44, 44, 44)

  var subject: GeoserverPublisher = _
  var inputImage: RasterMapOp = _

  before {
    // Set up the conf with the geoserver properties
    setupConfig

    // get all publishers from the factory.  The subject should be the only one in the list.
    subject = MrGeoPublisherFactory.getAllPublishers.get(0).asInstanceOf[GeoserverPublisher]

    // Create a mapop to publish

    inputImage = createRasterMapOpWithBounds(tileIds, zoomLevel, tileSize, bounds, pyramidName)
  }

  after {

  }

  behavior of "GeoserverPublisher"

  it should "publish an image to a Geoserver using the Geoserver REST API" in {
    subject.publishImage(pyramidName, inputImage.metadata.get)
    verifyGeoserverImageData
  }

  def setupConfig = {
    // Set a master password in the system properties so property decryption is enabled
    System.setProperty("mrgeo.encryption.masterPassword", mrgeoMasterPassword);
    nativeCRSString = GDALUtils.EPSG4326
    // Get the MrGeoProperties instance.  Because a static reference to the properties returned are held by the
    // MrGeoProperties class, we can inject any properties needed for testing
    val mrGeoProperties = MrGeoProperties.getInstance
    mrGeoProperties.put(s"${MRGEO_PUBLISHER_PROPERTY_PREFIX}.${profileName}.class", classOf[GeoserverPublisher].getName)
    mrGeoProperties.put(s"${MRGEO_PUBLISHER_PROPERTY_PREFIX}.${profileName}.configuratorClass",
      classOf[GeoserverPublisherConfigurator].getName)
    mrGeoProperties.put(s"${MRGEO_PUBLISHER_PROPERTY_PREFIX}.${profileName}.${GEOSERVER_URL_PROPERTY}", geoserverUrl)
    mrGeoProperties.put(s"${MRGEO_PUBLISHER_PROPERTY_PREFIX}.${profileName}.${GEOSERVER_USERNAME_PROPERTY}",
      geoserverUsername)
    mrGeoProperties.put(s"${MRGEO_PUBLISHER_PROPERTY_PREFIX}.${profileName}.${GEOSERVER_PASSWORD_PROPERTY}",
      s"ENC(${encryptedGeoserverPassword})")
    mrGeoProperties.put(s"${MRGEO_PUBLISHER_PROPERTY_PREFIX}.${profileName}.${WORKSPACE_NAME_PROPERTY}", workspace)
    mrGeoProperties.put(s"${MRGEO_PUBLISHER_PROPERTY_PREFIX}.${profileName}.${COVERAGE_STORE_NAME_PROPERTY}", coverageStoreName)
  }

  def verifyGeoserverImageData = {
    val url = s"${GeoserverPublisher.WORKSPACES_URL}/${workspace}/${GeoserverPublisher.COVERAGE_STORES_URL}/" +
      s"${coverageStoreName}/${GeoserverPublisher.COVERAGES_URL}/${pyramidName}.json"
    val jsonCoverage = new RestClient(geoserverUrl, "admin", "geoserver").getJson(url)
    assert(jsonCoverage != null)
    val jsonMapper = new ObjectMapper
    val rootNode = jsonMapper.readTree(jsonCoverage)
    val coverageNode = rootNode.get("coverage")

    /**
      * Verify the values of multiple nodes match a single expected result
      *
      * @param expectedNodeValue
      * @param nodeValues
      */
    def verifyNodes(expectedNodeValue: Any, nodeValues: List[Any]): Unit = {
      nodeValues.foreach(nv => {
        assertResult(expectedNodeValue) {
          nv
        }
      })
    }

    def verifyBoundingBox: Unit = {
      val nativeBoundingBoxNode = coverageNode.get("nativeBoundingBox")
      val latLonBoundingBoxNode = coverageNode.get("latLonBoundingBox")
      verifyNodes(bounds.w, List(
        nativeBoundingBoxNode.get("minx").asDouble(),
        latLonBoundingBoxNode.get("minx").asDouble()
      ))
      verifyNodes(bounds.s, List(
        nativeBoundingBoxNode.get("miny").asDouble(),
        latLonBoundingBoxNode.get("miny").asDouble()
      ))
      verifyNodes(bounds.e, List(
        nativeBoundingBoxNode.get("maxx").asDouble(),
        latLonBoundingBoxNode.get("maxx").asDouble()
      ))
      verifyNodes(bounds.n, List(
        nativeBoundingBoxNode.get("maxy").asDouble(),
        latLonBoundingBoxNode.get("maxy").asDouble()
      ))
      verifyNodes(epsg, List(
        nativeBoundingBoxNode.get("crs").asText(),
        latLonBoundingBoxNode.get("crs").asText()
      ))
    }

    def verifyGrid: Unit = {
      val imageMetadata: MrsPyramidMetadata = inputImage.metadata.get
      val rect: LongRectangle = imageMetadata.getPixelBounds(zoomLevel)

      val gridNode = coverageNode.get("grid")
      assertResult(2) {
        gridNode.get("@dimension").asInt()
      }
      assertResult(epsg) {
        gridNode.get("crs").asText()
      }
      val rangeNode = gridNode.get("range")
      assertResult(s"${rect.getMinX} ${rect.getMinY}") {
        rangeNode.get("low").asText()
      }
      assertResult(s"${rect.getMaxX} ${rect.getMaxY}") {
        rangeNode.get("high").asText()
      }
      val transformNode = gridNode.get("transform")
      assertResult(bounds.w) {
        transformNode.get("translateX").asDouble()
      }
      assertResult(bounds.n) {
        transformNode.get("translateY").asDouble()
      }
      verifyNodes(TMSUtils.resolution(zoomLevel, tileSize), List(
        transformNode.get("scaleX").asDouble(),
        transformNode.get("scaleY").asDouble()
      ))
    }

    def verifyDimensions = {
      val dimensionsNode = coverageNode.get("dimensions")
      dimensionsNode.get("coverageDimension").asScala.zipWithIndex.foreach{case (dimensionNode, bandIndex) => {
        assertResult(s"band ${bandIndex}") {
          dimensionNode.get("name").asText()
        }
        assertResult(s"Band ${bandIndex + 1} (double)") {
          dimensionNode.get("description").asText()
        }
        val rangeNode = dimensionNode.get("range")
        // Test raster is filled with all ones
        assertResult((1.0, 1.0)) {
          (rangeNode.get("min").asDouble(), rangeNode.get("max").asDouble())
        }
      }}
    }

    verifyNodes(pyramidName, List(
      coverageNode.get("name").asText(),
      coverageNode.get("nativeName").asText(),
      coverageNode.get("title").asText(),
      coverageNode.get("description").asText().split(descriptionPrefix)(1)
    ))
    // Can't compare the entire string becasue Geoserver changes formatting and adds decimals.  Just compare the first
    // 108 characters after removing all whitespace and that should be good enough to confirm the right string was used.
    assertResult(nativeCRSString.filter(_ > ' ').take(54)) {
      coverageNode.get("nativeCRS").asText().filter(_ > ' ').take(54)
    }
    assertResult(epsg) {
      coverageNode.get("srs").asText()
    }
    assertResult(projectionPolicy) {
      coverageNode.get("projectionPolicy").asText()
    }
    assert(coverageNode.get("enabled").asBoolean())
    assertResult(nativeFormat) {
      coverageNode.get("nativeFormat").asText()
    }

    verifyBoundingBox
    verifyGrid

    assertResult(supportedFormats) {
      coverageNode.get("supportedFormats").get("string").asScala.map(_.asText)
    }

    verifyDimensions

    val requestSRSNode = coverageNode.get("requestSRS").get("string")
    val responseSRSNode = coverageNode.get("responseSRS").get("string")
    verifyNodes(epsg, List(requestSRSNode.asScala.head.asText, responseSRSNode.asScala.head.asText))

  }

}
