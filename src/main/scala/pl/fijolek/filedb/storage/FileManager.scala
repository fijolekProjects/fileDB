package pl.fijolek.filedb.storage

import java.io.RandomAccessFile
import java.math.BigInteger
import java.nio.file.Paths
import java.util

import pl.fijolek.filedb.storage.ColumnTypes.ColumnType

import scala.collection.mutable.ArrayBuffer

// - record ahs to be smaller than buffer
// - fixed-length records only
// - heap file organization
class FileManager(systemCatalogManager: SystemCatalogManager) {
  val buffer = new Array[Byte](4096)

  def insertRecords(tableName: String, records: List[Record]): Unit = {
    val storedTableData = systemCatalogManager.readCatalog.tablesByName(tableName)
    val file = new RandomAccessFile(storedTableData.filePath, "rw")
    file.seek(file.length())
    val bytesToWrite = records.toArray.flatMap(_.toBytes)
    try {
      file.write(bytesToWrite)
    } finally {
      file.close()
    }
  }

  //TODO make it lazy? or fetch just n buffers
  def readRecords(tableName: String): List[Record] = {
    val storedTableData = systemCatalogManager.readCatalog.tablesByName(tableName)
    val file = new RandomAccessFile(storedTableData.filePath, "rw")
    val records = new ArrayBuffer[Record]()
    var bytesRead = -1
    try {
      while ( {
        bytesRead = file.read(buffer)
        bytesRead != -1
      }) {
        val recordsInBuffer = bytesRead / storedTableData.data.recordSize
        (0 until recordsInBuffer).foreach { index =>
          val record = storedTableData.data.readRecord(buffer, index)
          record.foreach(records += _)
        }
      }
    } finally {
      file.close()
    }
    records.toList
  }

  def deleteRecord(tableName: String, record: Record): Unit = {
    val storedTableData = systemCatalogManager.readCatalog.tablesByName(tableName)
    val file = new RandomAccessFile(storedTableData.filePath, "rw")
    var bytesRead = -1
    var currentFileOffset = 0

    try {
      while ( {
        bytesRead = file.read(buffer)
        bytesRead != -1
      }) {
        val recordsInBuffer = bytesRead / storedTableData.data.recordSize
        (0 until recordsInBuffer).foreach { index =>
          val readRecord = storedTableData.data.readRecord(buffer, index)
          readRecord.foreach { rec =>
            if (rec == record) {
              val emptyBytes = new Array[Byte](storedTableData.data.recordSize)
              val currentOffset = index * storedTableData.data.recordSize
              System.arraycopy(emptyBytes, 0, buffer, currentOffset, emptyBytes.length)
            }
          }
        }
        file.seek(currentFileOffset)
        file.write(buffer)
        currentFileOffset += bytesRead
      }
    } finally {
      file.close()
    }
  }

}

class SystemCatalogManager(basePath: String) {
  //FIXME this is in memory for now :)
  private var catalog = SystemCatalog(List.empty)
  def readCatalog: SystemCatalog = {
    catalog
  }

  def addTable(tableData: TableData): Unit = {
    val tableFile = Paths.get(basePath, tableData.name).toFile
    tableFile.getParentFile.mkdirs()
    tableFile.createNewFile()
    val storedTable = StoredTableData(tableData, tableFile.getAbsolutePath)
    catalog = catalog.copy(tables = storedTable :: catalog.tables)
  }
}



case class SystemCatalog(tables: List[StoredTableData]) {
  val tablesByName = tables.map(table => (table.data.name, table)).toMap
}
case class StoredTableData(data: TableData, filePath: String)
case class TableData(name: String, columnsDefinition: List[Column], searchKey: String) {
  val recordSize: Int = {
    columnsDefinition.map(_.typ.sizeInBytes).sum
  }

  def readRecord(buffer: Array[Byte], recordIndex: Int): Option[Record] = {
    val recordBuffer = util.Arrays.copyOfRange(buffer, recordIndex * recordSize, recordIndex * recordSize + recordSize)
    if (recordBuffer.forall(_ == 0)) {
      None
    } else {
      val (recordFields, _) = columnsDefinition.foldLeft((List.empty[Value], 0)) { case ((values, offset), colDef) =>
        val columnBytes = util.Arrays.copyOfRange(recordBuffer, offset, offset + colDef.typ.sizeInBytes)
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
    val toInsert = values.toArray.flatMap { value =>
      val sizeInBytes = value.column.typ.sizeInBytes
      value.column.typ match {
        case _: ColumnTypes.Varchar =>
          val bytes = value.value.asInstanceOf[String].getBytes
          util.Arrays.copyOf(bytes, sizeInBytes)
        case numType: ColumnTypes.Numeric =>
//          val bytes = value.value.asInstanceOf[BigDecimal].setScale(numType.scale).underlying().unscaledValue().toByteArray
//          watch out for litte/big endian - BigInteger assumes big-endian
//          System.arraycopy(bytes, 0, bytesToWrite, sizeInBytes - bytes.length, bytes.length)
//          bytesToWrite
          ???
      }
    }
    toInsert
  }

}
//TODO make column type and value type equal
case class Value(column: Column, value: Any)
