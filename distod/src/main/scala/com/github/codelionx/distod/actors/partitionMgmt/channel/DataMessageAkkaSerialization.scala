package com.github.codelionx.distod.actors.partitionMgmt.channel

import java.io.ByteArrayOutputStream
import java.nio.ByteOrder
import java.util.zip.{Deflater, Inflater}

import akka.actor.typed.ActorSystem
import akka.serialization.{SerializationExtension, Serializers}
import akka.util.{ByteString, ByteStringBuilder}
import com.github.codelionx.util.TryWithResource

import scala.util.{Failure, Success}


trait DataMessageAkkaSerialization {

  def serialize(m: DataMessage)(implicit system: ActorSystem[_]): ByteString = {
    implicit val byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN
    val serialization = SerializationExtension(system)
    val serializer = serialization.findSerializerFor(m)
    val manifest = Serializers.manifestFor(serializer, m)
    val manifestBytes = ByteString(manifest)

    val data = serializer.toBinary(m)
    val compressedData = compress(data)

    val b = new ByteStringBuilder
    b.putInt(serializer.identifier)
    b.putInt(manifestBytes.size)
    b.append(manifestBytes)
    b.putBytes(compressedData)
    b.result().compact
  }

  private def compress(data: Array[Byte]): Array[Byte] = {
    val deflater = new Deflater()
    deflater.setLevel(Deflater.BEST_COMPRESSION)
    deflater.setStrategy(Deflater.DEFAULT_STRATEGY)
    deflater.setInput(data)
    deflater.finish()

    val bytes = TryWithResource.withResource(new ByteArrayOutputStream(data.length)) { out =>
      val buffer = Array.ofDim[Byte](1024)
      while (!deflater.finished()) {
        val count = deflater.deflate(buffer)
        out.write(buffer, 0, count)
      }
      out.toByteArray
    }
    deflater.end()
    bytes
  }

  def deserialize(s: ByteString)(implicit system: ActorSystem[_]): DataMessage = {
    implicit val byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN
    val serialization = SerializationExtension(system)
    val (header, other) = s.splitAt(8)
    val (idBytes, manifestSizeBytes) = header.splitAt(4)

    val serializerId = idBytes.toByteBuffer.getInt
    val manifestSize = manifestSizeBytes.toByteBuffer.getInt
    val (manifestBytes, contentBytes) = other.splitAt(manifestSize)
    val manifest = manifestBytes.utf8String
    val decompressedContent = decompress(contentBytes.toArray[Byte])
    serialization.deserialize(decompressedContent, serializerId, manifest) match {
      case Success(value) => value.asInstanceOf[DataMessage]
      case Failure(cause) => throw new RuntimeException("Failed to deserialize data message", cause)
    }
  }

  private def decompress(data: Array[Byte]): Array[Byte] = {
    val inflater = new Inflater()
    inflater.setInput(data)
    val bytes = TryWithResource.withResource(new ByteArrayOutputStream(data.length)) { out =>
      val buffer = Array.ofDim[Byte](1024)
      while (!inflater.finished()) {
        val count = inflater.inflate(buffer)
        out.write(buffer, 0, count)
      }
      out.toByteArray
    }
    inflater.end()
    bytes
  }
}
