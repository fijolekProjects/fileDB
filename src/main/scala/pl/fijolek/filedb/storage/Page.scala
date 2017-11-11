package pl.fijolek.filedb.storage

import java.nio.ByteBuffer

case class Page(headerBytes: Array[Byte], recordBytes: Array[Byte]) {

  def add(records: List[Array[Byte]]): Page = {
    records.foldLeft(this) { case (newPage, record) =>
      newPage.add(record)
    }
  }

  def add(record: Array[Byte]): Page = {
    this.copy(headerBytes = this.header.addRecord(record.length).toBytes, recordBytes = recordBytes ++ record)
  }

  def remove(recordIndices: List[Int], recordSize: Int): Page = {
    recordIndices.foldLeft(this) { case (newPage, recordIndex) =>
      newPage.remove(recordIndex, recordSize)
    }
  }

  def remove(recordStartOffset: Int, recordSize: Int): Page = {
    val rangeToRemove = Range(recordStartOffset, recordStartOffset + recordSize)
    val newRecordBytes = recordBytes.zipWithIndex.map { case (byt, index) =>
      if (rangeToRemove.contains(index)) 0: Byte
      else byt
    }
    this.copy(recordBytes = newRecordBytes)
  }

  def bytes: Array[Byte] = {
    val headerBytes = header.toBytes
    headerBytes ++ recordBytes
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
    val recordBytes = java.util.Arrays.copyOfRange(bytes, DbConstants.pageHeaderSize, bytes.length - header.spareBytesAtTheEnd)
    new Page(headerBytes = headerBytes, recordBytes = recordBytes)
  }

  def newPage(fileId: Long, offset: Long): Page = {
    val headerBytes = PageHeader.newHeader(fileId, offset).toBytes
    val recordBytes = new Array[Byte](0)
    new Page(headerBytes = headerBytes, recordBytes = recordBytes)
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

  def addRecord(recordLength: Int): PageHeader = {
    this.copy(spareBytesAtTheEnd = spareBytesAtTheEnd - recordLength)
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