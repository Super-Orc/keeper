//package com.orco.spark.serializer
//
//import java.io._
//import java.nio.ByteBuffer
//
//import com.orco.spark.util.NextIterator
//
//import scala.reflect.ClassTag
//
//abstract class SerializerInstance {
//  def serialize[T: ClassTag](t: T): ByteBuffer
//
//  def deserialize[T: ClassTag](bytes: ByteBuffer): T
//
//  def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T
//
//  def serializeStream(s: OutputStream): SerializationStream
//
//  def deserializeStream(s: InputStream): DeserializationStream
//}
//abstract class SerializationStream extends Closeable {
//  /** The most general-purpose method to write an object. */
//  def writeObject[T: ClassTag](t: T): SerializationStream
//  /** Writes the object representing the key of a key-value pair. */
//  def writeKey[T: ClassTag](key: T): SerializationStream = writeObject(key)
//  /** Writes the object representing the value of a key-value pair. */
//  def writeValue[T: ClassTag](value: T): SerializationStream = writeObject(value)
//  def flush(): Unit
//  override def close(): Unit
//
//  def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
//    while (iter.hasNext) {
//      writeObject(iter.next())
//    }
//    this
//  }
//}
//abstract class DeserializationStream extends Closeable {
//  /** The most general-purpose method to read an object. */
//  def readObject[T: ClassTag](): T
//  /** Reads the object representing the key of a key-value pair. */
//  def readKey[T: ClassTag](): T = readObject[T]()
//  /** Reads the object representing the value of a key-value pair. */
//  def readValue[T: ClassTag](): T = readObject[T]()
//  override def close(): Unit
//
//  /**
//    * Read the elements of this stream through an iterator. This can only be called once, as
//    * reading each element will consume data from the input source.
//    */
//  def asIterator: Iterator[Any] = new NextIterator[Any] {
//    override protected def getNext() = {
//      try {
//        readObject[Any]()
//      } catch {
//        case eof: EOFException =>
//          finished = true
//          null
//      }
//    }
//
//    override protected def close() {
//      DeserializationStream.this.close()
//    }
//  }
//
//  /**
//    * Read the elements of this stream through an iterator over key-value pairs. This can only be
//    * called once, as reading each element will consume data from the input source.
//    */
//  def asKeyValueIterator: Iterator[(Any, Any)] = new NextIterator[(Any, Any)] {
//    override protected def getNext() = {
//      try {
//        (readKey[Any](), readValue[Any]())
//      } catch {
//        case eof: EOFException =>
//          finished = true
//          null
//      }
//    }
//
//    override protected def close() {
//      DeserializationStream.this.close()
//    }
//  }
//}
