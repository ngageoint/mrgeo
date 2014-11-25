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

package org.mrgeo;

// taken from: https://github.com/mlex/jerseytest/blob/master/mjl-jersey-server/src/test/java/de/codecentric/mjl/jerseytest/helpers/FilteringInMemoryTestContainerFactory.java

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.spi.container.WebApplication;
import com.sun.jersey.spi.container.WebApplicationFactory;
import com.sun.jersey.test.framework.AppDescriptor;
import com.sun.jersey.test.framework.LowLevelAppDescriptor;
import com.sun.jersey.test.framework.impl.container.inmemory.TestResourceClientHandler;
import com.sun.jersey.test.framework.spi.container.TestContainer;
import com.sun.jersey.test.framework.spi.container.TestContainerFactory;
import com.sun.jersey.test.framework.spi.container.inmemory.InMemoryTestContainerFactory;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.Set;
import java.util.logging.Logger;

/**
 * A low-level test-container factory. Unlike {@link InMemoryTestContainerFactory}, this factory
 * does not override set filters and can thus be used to test resources where custom filters are
 * needed.
 * 
 * @author michael.lex
 */
public class FilteringInMemoryTestContainerFactory implements TestContainerFactory
{

  private static class FilteringInMemoryTestContainer implements TestContainer
  {

    private static final Logger LOGGER = Logger.getLogger(FilteringInMemoryTestContainer.class
      .getName());

    final URI baseUri;

    final ResourceConfig resourceConfig;

    final WebApplication webApp;

    /**
     * Creates an instance of {@link FilteringInMemoryTestContainer}
     * 
     * @param baseUri
     *          URI of the application
     * @param ad
     *          instance of {@link LowLevelAppDescriptor}
     */
    private FilteringInMemoryTestContainer(final URI baseUri, final LowLevelAppDescriptor ad)
    {
      this.baseUri = UriBuilder.fromUri(baseUri).build();

      LOGGER.info("Creating low level InMemory test container configured at the base URI " +
        this.baseUri);

      this.resourceConfig = ad.getResourceConfig();
      this.webApp = initiateWebApplication(resourceConfig);
    }

    @Override
    public URI getBaseUri()
    {
      return baseUri;
    }

    @Override
    public Client getClient()
    {
      ClientConfig clientConfig = null;
      final Set<Object> providerSingletons = resourceConfig.getProviderSingletons();

      if (providerSingletons.size() > 0)
      {
        clientConfig = new DefaultClientConfig();
        for (final Object providerSingleton : providerSingletons)
        {
          clientConfig.getSingletons().add(providerSingleton);
        }
      }

      final Client client = (clientConfig == null) ? new Client(new TestResourceClientHandler(
        baseUri, webApp))
        : new Client(new TestResourceClientHandler(baseUri, webApp), clientConfig);

      return client;
    }

    @Override
    public void start()
    {
      if (!webApp.isInitiated())
      {
        LOGGER.info("Starting low level InMemory test container");

        webApp.initiate(resourceConfig);
      }
    }

    @Override
    public void stop()
    {
      if (webApp.isInitiated())
      {
        LOGGER.info("Stopping low level InMemory test container");

        webApp.destroy();
      }
    }

    private WebApplication initiateWebApplication(final ResourceConfig rc)
    {
      final WebApplication webapp = WebApplicationFactory.createWebApplication();
      return webapp;
    }

  }

  @Override
  public TestContainer create(final URI baseUri, final AppDescriptor ad)
  {
    if (!(ad instanceof LowLevelAppDescriptor))
    {
      throw new IllegalArgumentException(
        "The application descriptor must be an instance of LowLevelAppDescriptor");
    }

    return new FilteringInMemoryTestContainer(baseUri, (LowLevelAppDescriptor) ad);
  }

  @Override
  public Class<LowLevelAppDescriptor> supports()
  {
    return LowLevelAppDescriptor.class;
  }
}