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

package org.mrgeo.resources.serviceproviders;

import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;

import org.mrgeo.services.Configuration;
import org.mrgeo.services.mrspyramid.MrsPyramidService;

import com.sun.jersey.spi.inject.SingletonTypeInjectableProvider;

/**
 * @author Steve Ingram
 *         Date: 10/26/13
 */
@Provider
public class MrsPyramidServiceProvider extends SingletonTypeInjectableProvider<Context, MrsPyramidService> {

    private static final MrsPyramidService instance = new MrsPyramidService(Configuration.getInstance().getProperties());

    public MrsPyramidServiceProvider() {
        super(MrsPyramidService.class, instance);
    }
}
