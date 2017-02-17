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
