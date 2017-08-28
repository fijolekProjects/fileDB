package pl.fijolek.filedb.storage

import java.io.RandomAccessFile
import java.math.BigInteger
import java.nio.file.Paths
import java.util

import pl.fijolek.filedb.storage.ColumnTypes.ColumnType

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer

// - record has to be smaller than buffer
// - fixed-length records only
// - heap file organization
// - unspanned records
class FileManager(systemCatalogManager: SystemCatalogManager) {
  private val bufferSize = 4096

  def insertRecords(tableName: String, records: List[Record]): Unit = {
    val storedTableData = systemCatalogManager.readCatalog.tablesByName(tableName)
    val file = new RandomAccessFile(storedTableData.filePath, "rw")
    val fileSize = file.length()
    val bytesToWrite = storedTableData.data.prepareWrite(records, fileSize, bufferSize)
    FileUtils.append(file, bytesToWrite)
    ()
  }

  //TODO make it lazy? or fetch just n buffers
  def readRecords(tableName: String): List[Record] = {
    val storedTableData = systemCatalogManager.readCatalog.tablesByName(tableName)
    val file = new RandomAccessFile(storedTableData.filePath, "rw")
    val records = new ArrayBuffer[Record]()
    FileUtils.traverse(file, bufferSize) { (buffer, bytesRead) =>
      val recordsRead = storedTableData.data.readRecords(buffer, bytesRead)
      records ++= recordsRead
    }
    records.toList
  }

  def deleteRecord(tableName: String, record: Record): Unit = {
    val storedTableData = systemCatalogManager.readCatalog.tablesByName(tableName)
    val file = new RandomAccessFile(storedTableData.filePath, "rw")
    var currentFileOffset = 0
    FileUtils.traverse(file, bufferSize) { (buffer, bytesRead) =>
      val bufferWithDeletedRecord = storedTableData.data.prepareDelete(record, buffer, bytesRead)
      file.seek(currentFileOffset)
      file.write(bufferWithDeletedRecord)
      currentFileOffset += bytesRead
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
case class TableData(name: String, columnsDefinition: List[Column]) {
  val recordSize: Int = {
    columnsDefinition.map(_.typ.sizeInBytes).sum
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
    val toInsert = values.toArray.flatMap { value =>
      val sizeInBytes = value.column.typ.sizeInBytes
      value.column.typ match {
        case _: ColumnTypes.Varchar =>
          val bytes = value.value.asInstanceOf[String].getBytes
          util.Arrays.copyOf(bytes, sizeInBytes)
        case numType: ColumnTypes.Numeric =>
//          val bytes = value.value.asInstanceOf[BigDecimal].setScale(numType.scale).underlying().unscaledValue().toByteArray
//          watch out for little/big endian - BigInteger assumes big-endian
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
}