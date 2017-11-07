package pl.fijolek.filedb.storage

import java.util

import scala.collection.mutable.ArrayBuffer

case class TableData(name: String, columnsDefinition: List[Column]) {
  val recordSize: Int = {
    columnsDefinition.map(_.typ.sizeInBytes).sum
  }

  def column(columnName: String): Column = {
    columnsDefinition.find(_.name == columnName).get
  }

  def readRecords(page: Page): List[Record] = {
    val recordsInBuffer = page.recordBytes.length / recordSize
    (0 until recordsInBuffer).flatMap { index =>
      val record = readRecord(page.recordBytes, index)
      record
    }.toList
  }

  def prepareDelete(record: Record, page: Page): Page = {
    val recordsInBuffer = page.recordBytes.length / recordSize
    val deleteBufferOffsets = (0 until recordsInBuffer).flatMap { index =>
      val recordRead = readRecord(page.recordBytes, index)
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

object TableData {
  def readRecords(tableData: TableData, filePath: String): List[Record] = {
    val records = new ArrayBuffer[Record]()
    FileUtils.traverse(filePath) { page =>
      val recordsRead = tableData.readRecords(page)
      records ++= recordsRead
    }
    records.toList
  }

}