package org.mrgeo.spark

/*  Copied almost verbatim from Sparks JavaSerializer.scala (v 1.2.0), and
*   modified to add exception handling and DEBUG output.
*/

import java.io._
import java.nio.ByteBuffer

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.serializer.DeserializationStream
import org.apache.spark.serializer.SerializationStream
import org.apache.spark.serializer.Serializer
import org.apache.spark.serializer.SerializerInstance
//import org.apache.spark.serializer._

import scala.reflect.ClassTag

import org.apache.spark.{Logging, SparkConf}

import scala.util.control.NonFatal

private[spark] class JavaSerializationStream(out: OutputStream, counterReset: Int)
    extends SerializationStream {
  private val objOut = new ObjectOutputStream(out)
  private var counter = 0

  /**
   * Calling reset to avoid memory leak:
   * http://stackoverflow.com/questions/1281549/memory-leak-traps-in-the-java-standard-api
   * But only call it every 100th time to avoid bloated serialization streams (when
   * the stream 'resets' object class descriptions have to be re-written)
   */
  def writeObject[T: ClassTag](t: T): SerializationStream = {
    objOut.writeObject(t)
    counter += 1
    if (counterReset > 0 && counter >= counterReset) {
      objOut.reset()
      counter = 0
    }
    this
  }

  def flush() { objOut.flush() }
  def close() { objOut.close() }
}

private[spark] class JavaDeserializationStream(in: InputStream, loader: ClassLoader)
    extends DeserializationStream {
  private val objIn = new ObjectInputStream(in) {
    override def resolveClass(desc: ObjectStreamClass) =
      Class.forName(desc.getName, false, loader)
  }

  def readObject[T: ClassTag](): T = objIn.readObject().asInstanceOf[T]
  def close() { objIn.close() }
}


private[spark] class JavaSerializerInstance(counterReset: Int, defaultClassLoader: ClassLoader)
    extends SerializerInstance {

  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    val bos = new ByteArrayOutputStream()
    val out = serializeStream(bos)
    out.writeObject(t)
    out.close()
    ByteBuffer.wrap(bos.toByteArray)
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    val bis = new ByteBufferBackedInputStream(bytes)
    val in = deserializeStream(bis)
    in.readObject()
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    val bis = new ByteBufferBackedInputStream(bytes)
    val in = deserializeStream(bis, loader)
    in.readObject()
  }

  override def serializeStream(s: OutputStream): SerializationStream = {
    new JavaSerializationStream(s, counterReset)
  }

  def getContextOrSparkClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new JavaDeserializationStream(s, getContextOrSparkClassLoader)
  }

  def deserializeStream(s: InputStream, loader: ClassLoader): DeserializationStream = {
    new JavaDeserializationStream(s, loader)
  }
}

@DeveloperApi
class JavaSerializer(conf: SparkConf) extends org.apache.spark.serializer.Serializer
                                              with Logging with Serializable with Externalizable{
  private var counterReset = conf.getInt("spark.serializer.objectStreamReset", 100)

  /**
   * Execute a block of code that evaluates to Unit, re-throwing any non-fatal uncaught
   * exceptions as IOException.  This is used when implementing Externalizable and Serializable's
   * read and write methods, since Java's serializer will not report non-IOExceptions properly;
   * see SPARK-4080 for more context.
   */
  def tryOrIOException(block: => Unit) {
    try {
      block
    }
    catch {
      case e: IOException => throw e
      case NonFatal(t) => throw new IOException(t)
    }
  }

  override def newInstance(): SerializerInstance = {
    // defaultClassloader is missing from org.apache.spark.serializer.Serializer in Spark 1.0.0, so
    // we use reflection to try to load it, then fallback is we get an exception.  This mimics the
    // behavior if the member exists
    var classLoader = Thread.currentThread.getContextClassLoader
    try {
      val cl = getClass.getField("defaultClassLoader").asInstanceOf[Option[ClassLoader]]
      classLoader = cl.getOrElse(Thread.currentThread.getContextClassLoader)
    }
    new JavaSerializerInstance(counterReset, classLoader)
  }

  override def writeExternal(out: ObjectOutput): Unit = tryOrIOException {
    out.writeInt(counterReset)
  }

  override def readExternal(in: ObjectInput): Unit = tryOrIOException {
    counterReset = in.readInt()
  }
}

class ByteBufferBackedInputStream(val buffer:ByteBuffer) extends InputStream {

  override def read(): Int =
    if (buffer == null || !buffer.hasRemaining) {
      -1
    }
    else {
      buffer.get() & 0xFF
    }

  override def read(dest: Array[Byte]): Int = {
    read(dest, 0, dest.length)
  }

  override def read(dest: Array[Byte], offset: Int, length: Int): Int = {
    if (buffer == null || !buffer.hasRemaining) {
      -1
    }
    else {
      val amountToGet = math.min(buffer.remaining(), length)
      buffer.get(dest, offset, amountToGet)
      amountToGet
    }
  }

  override def skip(bytes: Long): Long = {
    if (buffer != null) {
      val amountToSkip = math.min(bytes, buffer.remaining).toInt
      buffer.position(buffer.position + amountToSkip)
      amountToSkip
    } else {
      0L
    }
  }

}