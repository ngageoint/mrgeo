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

package org.mrgeo.application;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.reflections.Reflections;

import javax.ws.rs.ApplicationPath;
import java.util.HashSet;
import java.util.Set;

@SuppressFBWarnings(value = "NM_SAME_SIMPLE_NAME_AS_SUPERCLASS", justification = "Used in WEB-INF only")
@ApplicationPath("/")
public class Application extends javax.ws.rs.core.Application
{

  @Override
  public Set<Class<?>> getClasses()
  {
    Reflections reflections = new Reflections("org.mrgeo.resources");
    Set<Class<?>> results = reflections.getTypesAnnotatedWith(javax.ws.rs.Path.class);
    for (Class<?> clz : results)
    {
      System.out.println("Found MrGeo resource: " + clz.getName());
    }
    return results;
  }

    /**
     * Get a set of root resource and provider instances. Fields and properties
     * of returned instances are injected with their declared dependencies
     * (see {@link javax.ws.rs.core.Context}) by the runtime prior to use.
     * <p/>
     * <p>Implementations should warn about and ignore classes that do not
     * conform to the requirements of root resource or provider classes.
     * Implementations should flag an error if the returned set includes
     * more than one instance of the same class. Implementations MUST
     * NOT modify the returned set.</p>
     * <p/>
     * <p>The default implementation returns an empty set.</p>
     *
     * @return a set of root resource and provider instances. Returning null
     *         is equivalent to returning an empty set.
     */
    @Override
    public Set<Object> getSingletons() {
        Set<Object> retval = new HashSet<Object>();
        Reflections reflections = new Reflections("org.mrgeo.resources");
        Set<Class<?>> results = reflections.getTypesAnnotatedWith(javax.ws.rs.ext.Provider.class);
        for (Class<?> clz : results)
        {
          System.out.println("Found MrGeo provider: " + clz.getName());
            try {
                retval.add( clz.newInstance() );
            } catch (InstantiationException e) {
                System.out.println("Unable to instantiate " + clz.getSimpleName() + ": " + e.getMessage());
            } catch (IllegalAccessException e) {
                System.out.println("Unable to access to instantiate " + clz.getSimpleName() + ": " + e.getMessage());
            } catch (Exception e) {
                System.out.println("Error instantiating " + clz.getSimpleName() + ": " + e.getMessage());
                e.printStackTrace();
            }
        }
        return retval;
    }
}
