package pl.fijolek.filedb.storage

import java.io.{File, RandomAccessFile}
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.file.Paths
import java.util

import pl.fijolek.filedb.storage.ColumnTypes.{ColumnType, Varchar}
import pl.fijolek.filedb.storage.DbConstants.pageSize

import scala.collection.mutable.ArrayBuffer

// - record has to be smaller than buffer
// - fixed-length records only
// - heap file organization
// - unspanned records
class FileManager(systemCatalogManager: SystemCatalogManager){
  val recordsIO = new RecordsIO

  def insertRecords(tableName: String, records: List[Record]): Unit = {
    val storedTableData = systemCatalogManager.readCatalog.tablesByName(tableName)
    recordsIO.insertRecords(storedTableData.data, storedTableData.filePath, records)
  }

  //TODO make it lazy? or fetch just n buffers
  def readRecords(tableName: String): List[Record] = {
    val storedTableData = systemCatalogManager.readCatalog.tablesByName(tableName)
    recordsIO.readRecords(storedTableData.data, storedTableData.filePath)
  }

  def deleteRecord(tableName: String, record: Record): Unit = {
    val storedTableData = systemCatalogManager.readCatalog.tablesByName(tableName)
    recordsIO.delete(storedTableData.data, storedTableData.filePath, record)
  }

}

class RecordsIO {

  def readRecords(tableData: TableData, filePath: String): List[Record] = {
    val records = new ArrayBuffer[Record]()
    FileUtils.traverse(filePath) { page =>
      val recordsRead = tableData.readRecords(page)
      records ++= recordsRead
    }
    records.toList
  }

  def insertRecords(data: TableData, filePath: String, records: List[Record]): Unit = {
    val recordsToWriteSize = records.length * data.recordSize
    val lastPage = Page.lastPage(filePath).getOrElse(Page.newPage(data, filePath, 0))
    val pagesToWrite = if (lastPage.spareBytesAtTheEnd > recordsToWriteSize) {
      List(lastPage.add(records.map(_.toBytes)))
    } else {
      val recordsThatFitsInLastPageCount = lastPage.spareBytesAtTheEnd / data.recordSize
      val recordsToWriteInOtherPagesCount = records.length - recordsThatFitsInLastPageCount
      val recordsInSinglePageCount = (DbConstants.pageSize - DbConstants.pageHeaderSize) / data.recordSize
      val newPagesCount = Math.ceil(recordsToWriteInOtherPagesCount.toDouble / recordsInSinglePageCount.toDouble).toInt
      val lastPageFull = lastPage.add(records.take(recordsThatFitsInLastPageCount).map(_.toBytes))
      val newPages = (0 until newPagesCount).toList.map { i =>
        val pageOffset = lastPage.offset + (i + 1) * DbConstants.pageSize
        val recordsOffset = i * recordsInSinglePageCount + recordsThatFitsInLastPageCount
        val newPageRecords = records.slice(recordsOffset, recordsOffset + recordsInSinglePageCount)
        val newPageRecordsBytes = newPageRecords.map(_.toBytes)
        val newPage = Page.newPage(data, filePath, pageOffset)
        newPage.add(newPageRecordsBytes)
      }
      lastPageFull :: newPages
    }

    pagesToWrite.foreach { page =>
      page.write()
    }
    ()
  }

  def delete(data: TableData, filePath: String, record: Record): Unit = {
    FileUtils.traverse(filePath) { page =>
      val newPage = data.prepareDelete(record, page)
      newPage.write()
    }
  }

}

object DbConstants {
  val pageSize = 4096
  val pageHeaderSize = 4
}

object SystemCatalogManager {

  val tableTable = TableData(
    name = "table",
    columnsDefinition = List(
      Column("name", ColumnTypes.Varchar(32)),
      Column("filePath", ColumnTypes.Varchar(100))
    )
  )

  val columnTable = TableData(
    name = "column",
    columnsDefinition = List(
      Column("tableId", ColumnTypes.Varchar(32)),
      Column("name", ColumnTypes.Varchar(32)),
      Column("type", ColumnTypes.Varchar(32))
    )
  )

  def toColumnRecord(columnDef: Column, tableId: String): Record = {
    Record(List(
      Value(columnTable.column("tableId"), tableId),
      Value(columnTable.column("name"), columnDef.name),
      Value(columnTable.column("type"), columnDef.typ.toString)
    ))
  }

  def toTableRecord(tableData: TableData, tableFile: File): Record = {
    Record(List(
      Value(tableTable.column("name"), tableData.name),
      Value(tableTable.column("filePath"), tableFile.getAbsolutePath)
    ))
  }

  def toTable(tableRecord: Record): (String, String) = {
    val tableName = tableRecord.values(0).value.toString
    val filePath = tableRecord.values(1).value.toString
    (tableName, filePath)
  }

  def toColumn(columnRecord: Record): (String, Column) = {
    val tableId = columnRecord.values(0).value.toString
    val columnName = columnRecord.values(1).value.toString
    val columnType = columnRecord.values(2).value.toString
    //TODO varchar only for now
    val varcharLength = "Varchar\\((.*)\\)".r.findAllMatchIn(columnType).toList(0).group(1).toInt
    val column = Column(columnName, Varchar(varcharLength))
    (tableId, column)
  }

}

class SystemCatalogManager(val basePath: String) {
  import SystemCatalogManager._

  val recordsReader = new RecordsIO

  def createTable(tableData: TableData): Unit = {
    val tableFile = tableData.path(basePath)
    FileUtils.touchFile(tableFile)
    val tableRecord = toTableRecord(tableData, tableFile)
    val columnsRecords = tableData.columnsDefinition.map { columnDef => toColumnRecord(columnDef, tableData.name) }
    recordsReader.insertRecords(tableTable, tableTable.path(basePath).getAbsolutePath, List(tableRecord))
    recordsReader.insertRecords(columnTable, columnTable.path(basePath).getAbsolutePath, columnsRecords)
    ()
  }

  def readCatalog: SystemCatalog = {
    val tables = read(tableTable)
    val columns = read(columnTable)
    val tablesData = tables.map { tableRecord =>
      val (tableName, filePath) = toTable(tableRecord)
      tableName -> filePath
    }.toMap
    val columnsData = columns.foldLeft(Map.empty[String, List[Column]]) { case (tableColumnDefinition, columnRecord) =>
      val (tableId, column) = toColumn(columnRecord)
      val columns = tableColumnDefinition.getOrElse(tableId, List.empty[Column])
      tableColumnDefinition.updated(tableId, columns ++ List(column))
    }
    val tableInfo = tablesData.map { case (tableName, filePath) =>
      StoredTableData(TableData(tableName, columnsData(tableName)), filePath)
    }.toList ++ List(
      StoredTableData(tableTable, tableTable.path(basePath).getAbsolutePath),
      StoredTableData(columnTable, columnTable.path(basePath).getAbsolutePath)
    )
    //add internal tables?
    SystemCatalog(tableInfo)
  }

  private def read(tableData: TableData): List[Record] = {
    val file = tableData.path(basePath)
    if (!file.exists()) {
      FileUtils.touchFile(file)
    }
    recordsReader.readRecords(tableData, file.getAbsolutePath)
  }

}

case class SystemCatalog(tables: List[StoredTableData]) {
  val tablesByName = tables.map(table => (table.data.name, table)).toMap
}
case class StoredTableData(data: TableData, filePath: String)
case class TableData(name: String, columnsDefinition: List[Column]) {
  val recordSize: Int = {
    columnsDefinition.map(_.typ.sizeInBytes).sum
  }

  def path(basePath: String): File = {
    Paths.get(basePath, name).toFile
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
case class Column(name: String, typ: ColumnType)
object ColumnTypes {
  object ColumnType {
    def numberWithPrecisionByteSize(precision: Int): Int = {
      BigDecimal((1 to precision).map(_ => "9").mkString).underlying().unscaledValue().toByteArray.length
    }
  }
  abstract sealed class ColumnType(val sizeInBytes: Int) {
    type baseType
  }
  case class Numeric(precision: Int, scale: Int) extends ColumnType(ColumnType.numberWithPrecisionByteSize(precision)) {
    override type baseType = BigDecimal
  }
  case class Varchar(length: Int) extends ColumnType(length) {
    override type baseType = String
  }
}


case class Record(values: List[Value]) {

  def toBytes: Array[Byte] = {
    Record.toBytes(values)
  }

}

object Record {

  def fromBytes(recordBytes: Array[Byte], columnsDefinition: List[Column]): Option[Record] = {
    if (recordBytes.forall(_ == 0)) {
      None
    } else {
      val (recordFields, _) = columnsDefinition.foldLeft((List.empty[Value], 0)) { case ((values, offset), colDef) =>
        val columnBytes = util.Arrays.copyOfRange(recordBytes, offset, offset + colDef.typ.sizeInBytes)
        val value: Any = colDef.typ match {
          case _: ColumnTypes.Varchar =>
            new String(columnBytes.takeWhile(_ != 0))
          case numType: ColumnTypes.Numeric =>
            val unscaledValue = BigDecimal(new BigInteger(columnBytes))
            unscaledValue / BigDecimal(10).pow(numType.scale)
        }
        (values :+ Value(colDef, value), offset + colDef.typ.sizeInBytes)
      }
      Some(Record(recordFields))
    }
  }

  def toBytes(values: List[Value]): Array[Byte] = {
    val bytes = values.toArray.flatMap { value =>
      val sizeInBytes = value.column.typ.sizeInBytes
      value.column.typ match {
        case _: ColumnTypes.Varchar =>
          val stringBytes = value.value.asInstanceOf[String].getBytes
          util.Arrays.copyOf(stringBytes, sizeInBytes)
        case numType: ColumnTypes.Numeric =>
          //          val bytes = value.value.asInstanceOf[BigDecimal].setScale(numType.scale).underlying().unscaledValue().toByteArray
          //          watch out for little/big endian - BigInteger assumes big-endian
          //          System.arraycopy(bytes, 0, bytesToWrite, sizeInBytes - bytes.length, bytes.length)
          //          bytesToWrite
          ???
      }
    }
    bytes
  }

}
//TODO make column type and value type equal
case class Value(column: Column, value: Any)


object FileUtils {

  def traverse(filePath: String)(process: Page => Unit): Unit = {
    withFileOpen(filePath) { file =>
      val buffer = new Array[Byte](DbConstants.pageSize)
      var bytesRead = -1
      var pageCount = 0
      while ( {
        bytesRead = file.read(buffer)
        bytesRead != -1
      }) {
        val page = Page.apply(buffer, filePath, pageCount * DbConstants.pageSize)
        process(page)
        pageCount += 1
      }
    }
  }

  def write(filePath: String, offset: Long, bytes: Array[Byte]): Unit = {
    withFileOpen(filePath) { file =>
      file.seek(offset)
      file.write(bytes)
    }
  }

  def withFileOpen[T](filePath: String)(process: (RandomAccessFile) => T): T = {
    val file = new RandomAccessFile(filePath, "rw")
    try {
      process(file)
    } finally {
      file.close()
    }
  }

  def touchFile(file: File): Unit = {
    file.getParentFile.mkdirs()
    file.createNewFile()
    ()
  }
}

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
    val headerBytes = util.Arrays.copyOfRange(bytes, 0, DbConstants.pageHeaderSize)
    val header = PageHeader.fromBytes(headerBytes)
    val recordBytes = util.Arrays.copyOfRange(bytes, DbConstants.pageHeaderSize, bytes.length - header.spareBytesAtTheEnd)
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
        val pageOffset = (fileSize / pageSize) * pageSize
        file.seek(pageOffset)
        val pageBytes = new Array[Byte](pageSize)
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