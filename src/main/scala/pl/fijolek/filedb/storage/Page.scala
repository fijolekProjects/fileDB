package pl.fijolek.filedb.storage

import java.nio.ByteBuffer

case class Page(headerBytes: Array[Byte], recordBytes: Array[Byte], filePath: String, offset: Long) {

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

  def write(): Unit = {
    val headerBytes = header.toBytes
    val toWrite = headerBytes ++ recordBytes
    FileUtils.write(filePath, offset, toWrite)
  }

  def spareBytesAtTheEnd: Int = {
    header.spareBytesAtTheEnd
  }

  private def header: PageHeader = {
    PageHeader.fromBytes(headerBytes)
  }

}

object Page {

  def apply(bytes: Array[Byte], filePath: String, offset: Long): Page = {
    val headerBytes = java.util.Arrays.copyOfRange(bytes, 0, DbConstants.pageHeaderSize)
    val header = PageHeader.fromBytes(headerBytes)
    val recordBytes = java.util.Arrays.copyOfRange(bytes, DbConstants.pageHeaderSize, bytes.length - header.spareBytesAtTheEnd)
    new Page(headerBytes = headerBytes, recordBytes = recordBytes, filePath, offset)
  }

  def newPage(tableData: TableData, filePath: String, offset: Long): Page = {
    val headerBytes = PageHeader.newHeader.toBytes
    val recordBytes = new Array[Byte](0)
    new Page(headerBytes = headerBytes, recordBytes = recordBytes, filePath, offset)
  }

  def lastPage(filePath: String): Option[Page] = {
    FileUtils.withFileOpen(filePath) { file =>
      val fileSize = file.length()
      if (fileSize == 0) {
        None
      } else {
        val pageOffset = (fileSize / DbConstants.pageSize) * DbConstants.pageSize
        file.seek(pageOffset)
        val pageBytes = new Array[Byte](DbConstants.pageSize)
        val bytesRead = file.read(pageBytes)
        Some(Page(pageBytes.take(bytesRead), filePath, pageOffset))
      }
    }

  }

}

case class PageHeader(spareBytesAtTheEnd: Int) {
  def toBytes: Array[Byte] = {
    ByteBuffer.allocate(4).putInt(spareBytesAtTheEnd).array()
  }

  def addRecord(recordLength: Int): PageHeader = {
    this.copy(spareBytesAtTheEnd = spareBytesAtTheEnd - recordLength)
  }

}

object PageHeader {
  def fromBytes(bytes: Array[Byte]): PageHeader = {
    val spareBytes = ByteBuffer.wrap(bytes).getInt
    PageHeader(spareBytes)
  }

  def newHeader = {
    new PageHeader(DbConstants.pageSize - DbConstants.pageHeaderSize)
  }
}