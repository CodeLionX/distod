package com.github.codelionx.distod.actors.partitionMgmt.channel

import java.nio.ByteOrder

import akka.actor.typed.ActorSystem
import akka.serialization.{SerializationExtension, Serializers}
import akka.util.{ByteString, ByteStringBuilder}


trait DataMessageAkkaSerialization {

  def serialize(m: DataMessage)(implicit system: ActorSystem[_]): ByteString = {
    implicit val byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN
    val serialization = SerializationExtension(system)
    val serializer = serialization.findSerializerFor(m)
    val manifest = Serializers.manifestFor(serializer, m)
    val manifestBytes = ByteString(manifest)

    val b = new ByteStringBuilder
    b.putInt(serializer.identifier)
    b.putInt(manifestBytes.size)
    b.append(manifestBytes)
    b.putBytes(serializer.toBinary(m))
    b.result().compact
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
    serialization.deserialize(contentBytes.toArray[Byte], serializerId, manifest).get.asInstanceOf[DataMessage]
  }
}
