package pl.fijolek.filedb.storage

import java.io.{File, RandomAccessFile}
import java.math.BigInteger
import java.nio.file.Paths
import java.util

import pl.fijolek.filedb.storage.ColumnTypes.{ColumnType, Varchar}
import pl.fijolek.filedb.storage.DbConstants.bufferSize

import scala.collection.immutable.TreeMap
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
    val file = new RandomAccessFile(filePath, "rw")
    val records = new ArrayBuffer[Record]()
    FileUtils.traverse(file, bufferSize) { (buffer, bytesRead) =>
      val recordsRead = tableData.readRecords(buffer, bytesRead)
      records ++= recordsRead
    }
    records.toList
  }

  def insertRecords(data: TableData, filePath: String, records: List[Record]): Unit = {
    val file = new RandomAccessFile(filePath, "rw") //it creates file when it does not exists
    val fileSize = file.length()
    val bytesToWrite = data.prepareWrite(records, fileSize, bufferSize)
    FileUtils.append(file, bytesToWrite)
    ()
  }

  def delete(data: TableData, filePath: String, record: Record): Unit = {
    val file = new RandomAccessFile(filePath, "rw")
    var currentFileOffset = 0
    FileUtils.traverse(file, bufferSize) { (buffer, bytesRead) =>
      val bufferWithDeletedRecord = data.prepareDelete(record, buffer, bytesRead)
      file.seek(currentFileOffset)
      file.write(bufferWithDeletedRecord)
      currentFileOffset += bytesRead
    }
  }

}

object DbConstants {
  val bufferSize = 4096
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

  def readRecords(buffer: Array[Byte], bytesRead: Int): List[Record] = {
    val recordsInBuffer = bytesRead / recordSize
    (0 until recordsInBuffer).flatMap { index =>
      val record = readRecord(buffer, index)
      record
    }.toList
  }

  private def readRecord(buffer: Array[Byte], recordIndex: Int): Option[Record] = {
    val recordBytes = util.Arrays.copyOfRange(buffer, recordIndex * recordSize, recordIndex * recordSize + recordSize)
    Record.fromBytes(recordBytes, columnsDefinition)
  }

  def prepareWrite(records: List[Record], fileSize: Long, bufferSize: Int): List[Array[Byte]] = {
    val toWrite = records.zipWithIndex.map { case (record, index) =>
      val offsetStart = fileSize + recordSize * index
      val offsetAfterWrite = offsetStart + recordSize
      val lastPageNumber = offsetStart / bufferSize
      if (offsetAfterWrite / bufferSize == lastPageNumber) {
        (lastPageNumber, record.toBytes)
      } else {
        val offsetEndOfPage = (lastPageNumber + 1) * bufferSize
        val freeSpaceSize = (offsetEndOfPage - offsetStart).toInt
        val freeSpace = new Array[Byte](freeSpaceSize)
        (lastPageNumber + 1, freeSpace ++ record.toBytes)
      }
    }
    val toWriteMap = toWrite.groupBy(_._1).mapValues(_.flatMap(_._2).toArray)
    val sortedMap = TreeMap(toWriteMap.toSeq: _*)
    sortedMap.values.toList
  }

  def prepareDelete(record: Record, buffer: Array[Byte], bytesRead: Int): Array[Byte] = {
    val recordsInBuffer = bytesRead / recordSize
    val deleteBufferOffsets = (0 until recordsInBuffer).flatMap { index =>
      val recordRead = readRecord(buffer, index)
      recordRead.flatMap { rec =>
        if (rec == record) {
          val currentOffset = index * recordSize
          Some(currentOffset)
        } else {
          None
        }
      }
    }
    val bufferWithDeletedRecord = deleteBufferOffsets.foldLeft(util.Arrays.copyOf(buffer, buffer.length)) { case (bufferWithDeleted, offset) =>
      val emptyBytes = new Array[Byte](recordSize)
      System.arraycopy(emptyBytes, 0, bufferWithDeleted, offset, emptyBytes.length)
      bufferWithDeleted
    }
    bufferWithDeletedRecord
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
  def traverse(file: RandomAccessFile, bufferSize: Int)(process: (Array[Byte], Int) => Unit): Unit = {
    val buffer = new Array[Byte](bufferSize)
    var bytesRead = -1
    try {
      while ( {
        bytesRead = file.read(buffer)
        bytesRead != -1
      }) {
        process(buffer, bytesRead)
      }
    } finally {
      file.close()
    }
  }

  def append(file: RandomAccessFile, pages: List[Array[Byte]]): Unit = {
    val fileSize = file.length()
    try {
      file.seek(fileSize)
      pages.foreach { page =>
        file.write(page)
      }
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