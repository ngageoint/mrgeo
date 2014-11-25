package org.mrgeo.core.mrsvector;

/**
 * This is a backward compatibility class only. There are existing MrGeo
 * outputs that reference this class. The actual class was moved to
 * org.mrgeo.vector.mrsvector package so in order for the existing sequence files
 * to be properly loaded, we include this class.
 */
@Deprecated
public class VectorTileWritable extends org.mrgeo.vector.mrsvector.VectorTileWritable
{
}
