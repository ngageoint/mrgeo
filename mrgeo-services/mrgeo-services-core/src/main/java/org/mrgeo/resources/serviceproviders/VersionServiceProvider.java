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

import org.mrgeo.services.version.VersionService;

import javax.ws.rs.core.Context;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

/**
 * @author Steve Ingram
 *         Date: 11/18/13
 */
@Provider
public class VersionServiceProvider implements
    ContextResolver<VersionService>
{
    private static final VersionService instance = new VersionService();

@Override
public VersionService getContext(Class<?> type)
{
  return instance;
}
}
