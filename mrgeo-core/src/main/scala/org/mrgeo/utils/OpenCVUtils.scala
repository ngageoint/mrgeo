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
