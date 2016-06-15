from py4j.java_gateway import JavaClass, JavaObject


def _is_instance_of(clazz, name):
    if clazz:
        if clazz.getCanonicalName() == name:
            return True

        return _is_instance_of(clazz.getSuperclass(), name)

    return False


def is_instance_of(gateway, java_object, java_class):
    if isinstance(java_class, str):
        name = java_class
    elif isinstance(java_class, JavaClass):
        name = java_class._fqn
    elif isinstance(java_class, JavaObject):
        name = java_class.getClass()
    else:
        raise Exception("java_class must be a string, a JavaClass, or a JavaObject")

    jvm = gateway.jvm
    name = jvm.Class.forName(name).getCanonicalName()

    if isinstance(java_object, JavaClass):
        cls = jvm.Class.forName(java_object._fqn)
    elif isinstance(java_object, JavaObject):
        cls = java_object.getClass()
    else:
        raise Exception("java_object must be a JavaClass, or a JavaObject")

    if cls.getCanonicalName() == name:
        return True

    return _is_instance_of(cls.getSuperclass(), name)
