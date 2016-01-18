package org.mrgeo.python

import org.mrgeo.job.{JobArguments, MrGeoDriver}

class PythonDriver extends MrGeoDriver {
  override def setup(job: JobArguments): Boolean = true
}
