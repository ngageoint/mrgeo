
from py4j.java_gateway import JavaClass, JavaObject

import copy
from py4j.java_gateway import JavaClass

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
        if isinstance(java_class, basestring):
            name = java_class
        elif isinstance(java_class, JavaClass):
            name = java_class._fqn
        elif isinstance(java_class, JavaObject):
            name = java_class.getClass()
        else:
            raise Exception("java_class must be a string, a JavaClass, or a JavaObject")

        jvm = self.gateway.jvm

        name = jvm.Class.forName(name).getCanonicalName()

        if isinstance(java_object, JavaClass):
            cls = jvm.Class.forName(java_object._fqn)
        elif isinstance(java_object, JavaObject):
            cls = java_object.getClass()
        else:
            return False
            #raise Exception("java_object must be a JavaClass, or a JavaObject")

        if cls.getCanonicalName() == name:
            return True

        return self._is_instance_of(cls.getSuperclass(), name)

    def _is_instance_of(self, clazz, name):
        if clazz:
            if clazz.getCanonicalName() == name:
                return True

            return self._is_instance_of(clazz.getSuperclass(), name)

        return False

    def ssave(self,name):
        import copy
        from numbers import Number
        from py4j.java_gateway import JavaClass
        cls = JavaClass('org.mrgeo.mapalgebra.save.SaveMapOp', gateway_client=self.gateway._gateway_client)
        if hasattr(self, 'mapop') and self.is_instance_of(self.mapop, 'org.mrgeo.mapalgebra.MapOp') and type(name) is str:
            op = cls.create(self.mapop, str(name))
        else:
            raise Exception('input types differ (TODO: expand this message!)')
        if (op.setup(self.job, self.context.getConf()) and
                op.execute(self.context) and
                op.teardown(self.job, self.context.getConf())):
            new_resource = copy.copy(self)
            new_resource.mapop = op
            return new_resource
        return None
