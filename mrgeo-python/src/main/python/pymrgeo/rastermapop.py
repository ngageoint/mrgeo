import copy
import json

from py4j.java_gateway import java_import
from pymrgeo.instance import is_instance_of as iio

class RasterMapOp(object):
    mapop = None
    gateway = None
    context = None
    job = None

    def __init__(self, gateway=None, context=None, mapop=None, job=None):
        self.gateway = gateway
        self.context = context
        self.mapop = mapop
        self.job = job

    @staticmethod
    def nan():
        return float('nan')

    def clone(self):
        return copy.copy(self)

    def is_instance_of(self, java_object, java_class):
        return iio(self.gateway, java_object, java_class)

    def metadata(self):
        if self.mapop is None:
            return None

        jvm = self.gateway.jvm

        java_import(jvm, "org.mrgeo.mapalgebra.raster.RasterMapOp")
        java_import(jvm, "org.mrgeo.image.MrsPyramidMetadata")

        if self.mapop.metadata().isEmpty():
            return None

        meta = self.mapop.metadata().get()

        java_import(jvm, "com.fasterxml.jackson.databind.ObjectMapper")

        mapper = jvm.com.fasterxml.jackson.databind.ObjectMapper()
        jsonstr = mapper.writeValueAsString(meta)

        # print(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(meta))

        return json.loads(jsonstr)
