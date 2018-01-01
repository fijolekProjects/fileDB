package pl.fijolek.filedb.storage

import java.nio.ByteBuffer

case class Page(headerBytes: Array[Byte], dataBytes: Array[Byte]) {

  def add(dataEntries: List[Array[Byte]]): Page = {
    dataEntries.foldLeft(this) { case (newPage, data) =>
      newPage.add(data)
    }
  }

  def add(data: Array[Byte]): Page = {
    this.copy(headerBytes = this.header.addDataBytes(data.length).toBytes, dataBytes = dataBytes ++ data)
  }

  def remove(dataIndices: List[Int], dataSize: Int): Page = {
    dataIndices.foldLeft(this) { case (newPage, dataIndex) =>
      newPage.remove(dataIndex, dataSize)
    }
  }

  def remove(dataStartOffset: Int, dataSize: Int): Page = {
    val rangeToRemove = Range(dataStartOffset, dataStartOffset + dataSize)
    val newDataBytes = dataBytes.zipWithIndex.map { case (byt, index) =>
      if (rangeToRemove.contains(index)) 0: Byte
      else byt
    }
    this.copy(dataBytes = newDataBytes)
  }

  def bytes: Array[Byte] = {
    val headerBytes = header.toBytes
    headerBytes ++ dataBytes
  }

  def spareBytesAtTheEnd: Int = {
    header.spareBytesAtTheEnd
  }

  def pageId = header.pageId
  def offset = header.offset

  private def header: PageHeader = {
    PageHeader.fromBytes(headerBytes)
  }

}

object Page {

  def apply(bytes: Array[Byte]): Page = {
    val headerBytes = java.util.Arrays.copyOfRange(bytes, 0, DbConstants.pageHeaderSize)
    val header = PageHeader.fromBytes(headerBytes)
    val dataBytes = java.util.Arrays.copyOfRange(bytes, DbConstants.pageHeaderSize, bytes.length - header.spareBytesAtTheEnd)
    new Page(headerBytes = headerBytes, dataBytes = dataBytes)
  }

  def newPage(fileId: Long, offset: Long): Page = {
    val headerBytes = PageHeader.newHeader(fileId, offset).toBytes
    val dataBytes = new Array[Byte](0)
    new Page(headerBytes = headerBytes, dataBytes = dataBytes)
  }

}

case class PageId(fileId: Long, offset: Long) {
  def toBytes = {
    ByteBuffer.allocate(DbConstants.pageIdSize)
      .putLong(fileId)
      .putLong(offset)
      .array()
  }
}

object PageId {
  def fromBytes(bytes: Array[Byte]): PageId = {
    val buffer = ByteBuffer.wrap(bytes)
    val fileId = buffer.getLong
    val offset = buffer.getLong
    PageId(fileId = fileId, offset = offset)
  }
}

case class PageHeader(pageId: PageId, spareBytesAtTheEnd: Int) {
  def offset = pageId.offset

  def toBytes: Array[Byte] = {
    val buffer = ByteBuffer.allocate(DbConstants.pageHeaderSize)
      .put(pageId.toBytes)
      .putInt(spareBytesAtTheEnd)
    buffer.array()
  }

  def addDataBytes(dataLength: Int): PageHeader = {
    this.copy(spareBytesAtTheEnd = spareBytesAtTheEnd - dataLength)
  }

}

object PageHeader {
  def fromBytes(bytes: Array[Byte]): PageHeader = {
    val (pageIdBytes, restBytes) = (bytes.take(DbConstants.pageIdSize), bytes.drop(DbConstants.pageIdSize))
    PageHeader(pageId = PageId.fromBytes(pageIdBytes), spareBytesAtTheEnd = ByteBuffer.wrap(restBytes).getInt)
  }

  def newHeader(fileId: Long, offset: Long): PageHeader = {
    val pageId = PageId(fileId = fileId, offset = offset)
    new PageHeader(pageId = pageId, spareBytesAtTheEnd = DbConstants.pageSize - DbConstants.pageHeaderSize)
  }
}