package pl.fijolek.filedb.storage

import java.io.File
import java.nio.file.Paths

import pl.fijolek.filedb.storage.ColumnTypes.ColumnType

object SystemCatalogManager {

  val fileTableFileId = -1L
  val tableTableFileId = -2L
  val columnTableFileId = -3L

  val fileTable = TableData(
    name = "file",
    columnsDefinition = List(
      Column("id", ColumnTypes.BigInt),
      Column("filePath", ColumnTypes.Varchar(DbConstants.filePathSize))
    )
  )

  val tableTable = TableData(
    name = "table",
    columnsDefinition = List(
      Column("name", ColumnTypes.Varchar(32)),
      Column("fileId", ColumnTypes.BigInt)
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

  val internalFiles: Map[Long, TableData] = Map(
    fileTableFileId -> fileTable,
    tableTableFileId -> tableTable,
    columnTableFileId -> columnTable
  )

  def toColumnRecord(columnDef: Column, tableId: String): Record = {
    Record(List(
      Value(columnTable.column("tableId"), tableId),
      Value(columnTable.column("name"), columnDef.name),
      Value(columnTable.column("type"), columnDef.typ.toString)
    ))
  }

  def toTableRecord(tableData: TableData, fileId: Long): Record = {
    Record(List(
      Value(tableTable.column("name"), tableData.name),
      Value(tableTable.column("fileId"), fileId)
    ))
  }

  def toFileRecord(fileId: Long, file: File): Record = {
    Record(List(
      Value(fileTable.column("id"), fileId),
      Value(fileTable.column("filePath"), file.getAbsolutePath)
    ))
  }

  def toTable(tableRecord: Record, fileIdToPath: Map[Long, String]): (String, String) = {
    val tableName = tableRecord.values(0).value.toString
    val fileId = tableRecord.values(1).value.asInstanceOf[Long]
    val filePath = fileIdToPath(fileId)
    (tableName, filePath)
  }

  def toColumn(columnRecord: Record): (String, Column) = {
    val tableId = columnRecord.values(0).value.toString
    val columnName = columnRecord.values(1).value.toString
    val columnTypeString = columnRecord.values(2).value.toString
    val columnType = ColumnType.fromString(columnTypeString)
    val column = Column(columnName, columnType)
    (tableId, column)
  }

}

class SystemCatalogManager(val basePath: String, recordsIO: RecordsIO, fileIdMapper: FileIdMapper) {
  import SystemCatalogManager._

  def init(): Unit = {
    createTable(fileTable, fileTableFileId)
    createTable(tableTable, tableTableFileId)
    createTable(columnTable, columnTableFileId)
  }

  def createTable(tableData: TableData): Unit = {
    val fileId = fileIdMapper.maxFileId + 1
    createTable(tableData, fileId)
  }

  private def createTable(tableData: TableData, fileId: Long): Unit = {
    val tableFile = filePath(tableData)
    FileUtils.touchFile(tableFile)
    val fileRecord = toFileRecord(fileId, tableFile)
    val tableRecord = toTableRecord(tableData, fileId)
    val columnsRecords = tableData.columnsDefinition.map { columnDef => toColumnRecord(columnDef, tableData.name) }
    recordsIO.insertRecords(StoredTableData(fileTable, fileTableFileId), List(fileRecord))
    recordsIO.insertRecords(StoredTableData(tableTable, tableTableFileId), List(tableRecord))
    recordsIO.insertRecords(StoredTableData(columnTable, columnTableFileId), columnsRecords)
    ()
  }

  def readCatalog: SystemCatalog = {
    val tables = readInternalTable(StoredTableData(tableTable, tableTableFileId))
    val columns = readInternalTable(StoredTableData(columnTable, columnTableFileId))
    val tablesData = tables.map { tableRecord =>
      val tableName = tableRecord.values(0).value.toString
      val fileId = tableRecord.values(1).value.asInstanceOf[Long]
      tableName -> fileId
    }.toMap
    val columnsData = columns.foldLeft(Map.empty[String, List[Column]]) { case (tableColumnDefinition, columnRecord) =>
      val (tableId, column) = toColumn(columnRecord)
      val columns = tableColumnDefinition.getOrElse(tableId, List.empty[Column])
      tableColumnDefinition.updated(tableId, columns ++ List(column))
    }
    val tableInfo = tablesData.map { case (tableName, fileId) =>
      StoredTableData(TableData(tableName, columnsData(tableName)), fileId)
    }.toList
    SystemCatalog(tableInfo)
  }

  private def readInternalTable(storedTableData: StoredTableData): List[Record] = {
    recordsIO.readRecords(storedTableData)
  }

  private def filePath(tableData: TableData): File = {
    Paths.get(basePath, tableData.name).toFile
  }

}

case class SystemCatalog(tables: List[StoredTableData]) {
  val tablesByName = tables.map(table => (table.data.name, table)).toMap
}

case class StoredTableData(data: TableData, fileId: Long)
