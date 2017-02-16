package org.mrgeo.publisher.rest.geoserver;

import org.mrgeo.publisher.MrGeoPublisher;
import org.mrgeo.publisher.MrGeoPublisherConfigurator;

import java.util.Properties;


@SuppressWarnings("squid:S2068") // Not a hardcoded password
public class GeoserverPublisherConfigurator implements MrGeoPublisherConfigurator
{

static final String GEOSERVER_URL_PROPERTY = "geoserverUrl";
static final String GEOSERVER_USERNAME_PROPERTY = "geoserverUsername";
static final String GEOSERVER_PASSWORD_PROPERTY = "geoserverPassword";
static final String WORKSPACE_NAME_PROPERTY = "workspace.name";
static final String COVERAGE_STORE_NAME_PROPERTY = "coverageStore.name";
static final String COVERAGE_STORE_DESCRIPTION_PROPERTY = "coverageStore.description";
static final String COVERAGE_STORE_TYPE_PROPERTY = "coverageStore.type";
static final String COVERAGE_STORE_CONFIG_URL_PROPERTY = "coverageStore.configUrl";


private static final String DEFAULT_WORKSPACE = "mrgeo";
private static final String DEFAULT_COVERAGE_STORE_NAME = "mrgeo";
private static final String DEFAULT_COVERAGE_STORE_DESCRIPTION =
    "MrGeo data source, automatically (periodically) updated with new layers";
private static final String DEFAULT_COVERAGE_STORE_TYPE = "MrGeo";
private static final String DEFAULT_COVERAGE_STORE_CONFIG_URL = "mrgeo.config";


@Override
public void configure(MrGeoPublisher publisher, Properties profileProperties)
{
  if (!GeoserverPublisher.class.isInstance(publisher))
  {
    throw new IllegalArgumentException("Instances of GeoserverPublisherConfigurator can only be used to " +
        "configure instances of GeosserverPublisher (or one of its subclasses)");
  }
  GeoserverPublisher geoserverPublisher = (GeoserverPublisher) publisher;
  String geoserverUrl = profileProperties.getProperty(GEOSERVER_URL_PROPERTY);
  if (geoserverUrl == null)
  {
    throw new IllegalArgumentException("Geoserver Url must be specified to configure Geoserver Publisher");
  }
  String geoserverUsername = profileProperties.getProperty(GEOSERVER_USERNAME_PROPERTY);
  String geoserverPassword = profileProperties.getProperty(GEOSERVER_PASSWORD_PROPERTY);
  if (geoserverUsername == null || geoserverPassword == null)
  {
    throw new IllegalArgumentException(
        "Geoserver username and password must be specified to configure Geoserver Publisher");
  }
  geoserverPublisher.setGeoserverEndpoint(geoserverUrl, geoserverUsername, geoserverPassword);
  geoserverPublisher.setWorkspace(profileProperties.getProperty(WORKSPACE_NAME_PROPERTY, DEFAULT_WORKSPACE));
  geoserverPublisher
      .setCoverageStoreName(profileProperties.getProperty(COVERAGE_STORE_NAME_PROPERTY, DEFAULT_COVERAGE_STORE_NAME));
  geoserverPublisher.setCoverageStoreDescription(profileProperties.getProperty(COVERAGE_STORE_DESCRIPTION_PROPERTY,
      DEFAULT_COVERAGE_STORE_DESCRIPTION));
  geoserverPublisher.setCoverageStoreType(profileProperties.getProperty(COVERAGE_STORE_TYPE_PROPERTY,
      DEFAULT_COVERAGE_STORE_TYPE));
  geoserverPublisher.setCoverageStoreConfigUrl(profileProperties.getProperty(COVERAGE_STORE_CONFIG_URL_PROPERTY,
      DEFAULT_COVERAGE_STORE_CONFIG_URL));
}
}
