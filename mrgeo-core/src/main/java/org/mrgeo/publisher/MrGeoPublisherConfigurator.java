package org.mrgeo.publisher;

import java.util.Properties;

/**
 * Created by ericwood on 8/15/16.
 */
public interface MrGeoPublisherConfigurator
{

/**
 * Configure a publisher based, optionally using any properties defined for a profile
 *
 * @param publisher         The publisher to configure
 * @param profileProperties A properties object that contains properties to use to configure the publisher.  An implementation
 *                          may use all, some, or none of these properties to configure the publisher.
 */
void configure(MrGeoPublisher publisher, Properties profileProperties);

}
