package pl.fijolek.filedb.storage

import java.util

case class TableData(name: String, columnsDefinition: List[Column], indices: List[Index] = List.empty) {
  val recordSize: Int = {
    columnsDefinition.map(_.typ.sizeInBytes).sum
  }

  def column(columnName: String): Column = {
    columnsDefinition.find(_.name == columnName).get
  }

  def readRecords(page: Page): List[Record] = {
    val recordsInBuffer = page.dataBytes.length / recordSize
    (0 until recordsInBuffer).flatMap { index =>
      val record = readRecord(page.dataBytes, index)
      record
    }.toList
  }

  def prepareDelete(record: Record, page: Page): Page = {
    val recordsInBuffer = page.dataBytes.length / recordSize
    val deleteBufferOffsets = (0 until recordsInBuffer).flatMap { index =>
      val recordRead = readRecord(page.dataBytes, index)
      recordRead.flatMap { rec =>
        if (rec == record) {
          val currentOffset = index * recordSize
          Some(currentOffset)
        } else {
          None
        }
      }
    }.toList
    page.remove(deleteBufferOffsets, recordSize)
  }

  private def readRecord(buffer: Array[Byte], recordIndex: Int): Option[Record] = {
    val recordBytes = util.Arrays.copyOfRange(buffer, recordIndex * recordSize, recordIndex * recordSize + recordSize)
    Record.fromBytes(recordBytes, columnsDefinition)
  }

}

case class Index(column: String)