#!/usr/bin/python

# This script provides more intuitive formatting for input options but may only be used when running Python 2.7 or later.

import libxml2
import sys
import urllib
import urllib2
import time
from WpsTools import *
from xml.sax.saxutils import escape
import argparse

# Utility function to skip any characters before the leading < char.
# Sometimes tomcat gives back XML with a garbage character before the
# actual XML. This may have something to do with chunking.
def cleanXmlString(xml):
    index = xml.find("<")
    if (index >= 0):
        xml = xml[index:]
    return xml

#
# Uncomment one of the following settings for mrgeoUrl depending on the
# environment you want to run in. When this file is committed, the
# setting for production environments should be the one that is uncommented.
#
# The following is for production environment
mrgeoUrl = "http://localhost:8080/mrgeo/services"
# The following is for capstone
#mrgeoUrl = "http://capdevmrgeo.geoeye.com:8080/mrgeo/services"
# The following is for developer machines
#mrgeoUrl = "http://localhost:8080/mrgeo-services/services"

parser = argparse.ArgumentParser(description="Executes a map algebra expression and writes the product output to the specified location")
parser.add_argument("expression", help="a map algebra expression or a path to a file name containing map algebra with a .ma extension")
parser.add_argument("productoutputpath", help="product output directory on the server")
group = parser.add_mutually_exclusive_group()
group.add_argument("-bp", "--buildpyramids", help="Builds pyramid levels on the raster product output; defaults to true and overrides the previewmode setting", action="store_true")
group.add_argument("-pm", "--previewmode", help="Generates a low resolution raster product; defaults to false and is overrided by the buildpyramids setting", action="store_true")
parser.add_argument("-cs", "--colorscale", help="The server-side color scale file used to display the job output raster with; if none is entered or the text \"default\" is entered, the system default color scale is used", default="default")
parser.add_argument("-pd", "--polldelay", help="The amount of time to pause while checking for job progress updates, in seconds", type=int, default=10)
args = parser.parse_args()

exp = args.expression
if exp.endswith(".ma"):
  exp = open(exp).read()
output = args.productoutputpath
buildPyramids = args.buildpyramids
if args.buildpyramids:
  buildPyramids = "true"
else:
  buildPyramids = "false"
if args.previewmode:
  previewMode = "true"
else:
  previewMode = "false"
if args.previewmode:
  previewMode = "true"
else:
  previewMode = "false"
if args.colorscale:
  colorScale = args.colorscale
pollDelay = args.polldelay

dataContent = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\
<wps:Execute service="WPS" version="1.0.0"\
  xmlns:wps="http://www.opengis.net/wps/1.0.0"\
  xmlns:ows="http://www.opengis.net/ows/1.1"\
  xmlns:xlink="http://www.w3.org/1999/xlink"\
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\
  xsi:schemaLocation="http://www.opengis../wpsExecute_request.xsd">\
  <ows:Identifier>MapAlgebraProcess</ows:Identifier>\
  <wps:DataInputs>\
    <wps:Input>\
      <ows:Identifier>expression</ows:Identifier>\
      <wps:Data>\
        <wps:LiteralData>\
%(expression)s\
        </wps:LiteralData>\
      </wps:Data>\
    </wps:Input>\
    <wps:Input>\
      <ows:Identifier>output</ows:Identifier>\
      <wps:Data>\
        <wps:LiteralData>%(output)s</wps:LiteralData>\
      </wps:Data>\
    </wps:Input>\
    <wps:Input>\
      <ows:Identifier>pyramid</ows:Identifier>\
      <wps:Data>\
        <wps:LiteralData>%(pyramid)s</wps:LiteralData>\
      </wps:Data>\
    </wps:Input>\
    <wps:Input>\
      <ows:Identifier>preview</ows:Identifier>\
      <wps:Data>\
        <wps:LiteralData>%(preview)s</wps:LiteralData>\
      </wps:Data>\
    </wps:Input>\
    <wps:Input>\
      <ows:Identifier>colorscale</ows:Identifier>\
      <wps:Data>\
        <wps:LiteralData>%(colorScale)s</wps:LiteralData>\
      </wps:Data>\
    </wps:Input>\
  </wps:DataInputs>\
  <wps:ResponseForm>\
    <wps:ResponseDocument storeExecuteResponse="true" status="true">\
      <wps:Output>\
        <ows:Identifier>summary</ows:Identifier>\
      </wps:Output>\
      <wps:Output>\
        <ows:Identifier>kml</ows:Identifier>\
      </wps:Output>\
      <wps:Output>\
        <ows:Identifier>wms</ows:Identifier>\
      </wps:Output>\
    </wps:ResponseDocument>\
  </wps:ResponseForm>\
</wps:Execute>\
' %{"expression": escape(exp),
     "output": escape(output),
     "pyramid": buildPyramids,
     "preview": previewMode,
     "colorscale": escape(colorScale)
    }

headers = {
  'Content-Type': 'application/xml; charset=utf-8'
}
req = urllib2.Request(mrgeoUrl, dataContent, headers);

try:
  result = urllib2.urlopen(req);
except urllib2.HTTPError, err:
  print "Error Connecting to MrGeo: "
  print "  " + mrgeoUrl + ":",
  print  err
  sys.exit(1)

xml = cleanXmlString(result.read())
info = result.info();

error = getError(xml)
if error != None:
        print error
        sys.exit(0)

jobUrl = "";
wmsUrl = ""
error = ""
t0 = time.time()
progress = "0";
while ((not wmsUrl) and (not error)):
    doc = parseString(xml)      
    wmsUrl = getLiteralData('wms', xml)
    if not wmsUrl:
        try:
            element = doc.getElementsByTagNameNS('*', 'ProcessStarted')
            if (element != None and len(element) > 0):
                progress = str(element[0].getAttribute('percentCompleted'))
                print "Job progress:  " + progress + "% complete.\nJob for expression:\n" + exp  + "\nhas been running for " + str(time.time() - t0) + " seconds."
            element = doc.getElementsByTagNameNS('*', 'ExecuteResponse')
            if (element != None and len(element) > 0):
                jobUrl = str(element[0].getAttribute('statusLocation'))
            print "\nWaiting for " + str(pollDelay) + " seconds...\n"
            time.sleep(pollDelay)
        except:
            pass
    else:
        print "Job for expression:\n" + exp  + "\ncompleted in " + '%.2f' % (time.time() - t0) + "seconds.\nWMS product URL: " + str(wmsUrl)

    print "Sending request to: " + str(jobUrl)
    try:
      result = urllib2.urlopen(jobUrl);
    except urllib2.HTTPError, err:
      print "Error requesting progress: "
      print "  " + jobUrl + ":",
      print  err
    xml = cleanXmlString(result.read())
    #print 'Response: ' + str(xml)
    error = getError(xml)
    if error != None:
        print "Job error: " + str(error)

