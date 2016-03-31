/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.resources.serviceproviders;

import com.sun.jersey.spi.inject.SingletonTypeInjectableProvider;
import org.mrgeo.services.tms.TmsService;

import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;

/**
 * @author Steve Ingram
 *         Date: 11/1/13
 */
@Provider
public class TmsServiceContextProvider extends SingletonTypeInjectableProvider<Context, TmsService> {

    private static final TmsService instance = new TmsService();

    public TmsServiceContextProvider() {
        super(TmsService.class, instance);
    }
}
