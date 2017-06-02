import copy
import json

from py4j.java_gateway import java_import
from pymrgeo.instance import is_instance_of as iio


class VectorMapOp(object):
    mapop = None
    gateway = None
    context = None
    job = None

    def __init__(self, gateway=None, context=None, mapop=None, job=None):
        self.gateway = gateway
        self.context = context
        self.mapop = mapop
        self.job = job

    def clone(self):
        return copy.copy(self)

    def is_instance_of(self, java_object, java_class):
        return iio(self.gateway, java_object, java_class)

    def ssave(self, name):
        import copy
        from py4j.java_gateway import JavaClass
        cls = JavaClass('org.mrgeo.mapalgebra.save.SaveMapOp', gateway_client=self.gateway._gateway_client)
        if hasattr(self, 'mapop') and self.is_instance_of(self.mapop, 'org.mrgeo.mapalgebra.MapOp') and type(
                name) is str:
            op = cls.create(self.mapop, str(name), False)
        else:
            raise Exception('input types differ (TODO: expand this message!)')
        if (op.setup(self.job, self.context.getConf()) and
                op.execute(self.context) and
                op.teardown(self.job, self.context.getConf())):
            new_resource = copy.copy(self)
            new_resource.mapop = op
            return new_resource
        return

    def metadata(self):
        if self.mapop is None:
            return None

        jvm = self.gateway.jvm

        java_import(jvm, "org.mrgeo.mapalgebra.vector.VectorMapOp")
        java_import(jvm, "org.mrgeo.data.vector.VectorMetadata")

        if self.mapop.metadata().isEmpty():
            return None

        meta = self.mapop.metadata().get()

        java_import(jvm, "com.fasterxml.jackson.databind.ObjectMapper")

        mapper = jvm.com.fasterxml.jackson.databind.ObjectMapper()
        jsonstr = mapper.writeValueAsString(meta)

        print(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(meta))

        return json.loads(jsonstr)

