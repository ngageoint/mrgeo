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

package org.mrgeo.utils

import java.io.File

import org.apache.spark.Logging
import org.mrgeo.core.{MrGeoConstants, MrGeoProperties}
import org.opencv.core.Core

/**
  * Created by tim.tisler on 3/2/16.
  */
object OpenCVUtils extends Logging {

  private var initialized = false

  if (!initialized) {
    initialized = true
    initializeOpenCV()
  }

  // empty method to force static initializer
  def register() = {}

  private def initializeOpenCV() = {
    // Monkeypatch the system library path to use the additional paths (for loading libraries)
    MrGeoProperties.getInstance().getProperty(MrGeoConstants.GDAL_PATH, "").
        split(File.pathSeparator).foreach(path => {
      ClassLoaderUtil.addLibraryPath(path)
    })

    System.loadLibrary(Core.NATIVE_LIBRARY_NAME)
  }

}
