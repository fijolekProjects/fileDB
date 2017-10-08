package pl.fijolek.filedb.storage

import java.io.File

import pl.fijolek.filedb.storage.ColumnTypes.ColumnType

object SystemCatalogManager {

  val fileTable = TableData(
    name = "file",
    columnsDefinition = List(
      Column("id", ColumnTypes.BigInt),
      Column("filePath", ColumnTypes.Varchar(100))
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

class SystemCatalogManager(val basePath: String) {
  import SystemCatalogManager._

  val recordsReader = new RecordsIO

  def createTable(tableData: TableData): Unit = {
    //fixme internal tables in `file` table?
    val tableFile = tableData.path(basePath)
    FileUtils.touchFile(tableFile)
    val fileId = maxFileId + 1
    val fileRecord = toFileRecord(fileId, tableFile)
    val tableRecord = toTableRecord(tableData, fileId)
    val columnsRecords = tableData.columnsDefinition.map { columnDef => toColumnRecord(columnDef, tableData.name) }
    recordsReader.insertRecords(fileTable, fileTable.path(basePath).getAbsolutePath, List(fileRecord))
    recordsReader.insertRecords(tableTable, tableTable.path(basePath).getAbsolutePath, List(tableRecord))
    recordsReader.insertRecords(columnTable, columnTable.path(basePath).getAbsolutePath, columnsRecords)
    ()
  }

  private def maxFileId: Long = {
    val fileIdToPath = readFileIdToPath
    val ids = fileIdToPath.keys.toList
    if (ids.isEmpty) -1 else ids.max
  }

  def readCatalog: SystemCatalog = {
    val tables = read(tableTable)
    val columns = read(columnTable)
    val tablesData = tables.map { tableRecord =>
      val (tableName, filePath) = toTable(tableRecord, readFileIdToPath)
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
      StoredTableData(fileTable, fileTable.path(basePath).getAbsolutePath),
      StoredTableData(tableTable, tableTable.path(basePath).getAbsolutePath),
      StoredTableData(columnTable, columnTable.path(basePath).getAbsolutePath)
    )
    //fixme add internal tables to `table` table?
    SystemCatalog(tableInfo)
  }

  private def readFileIdToPath: Map[Long, String] = {
    val files = read(fileTable)
    val fileIdToPath = files.map { record =>
      val id = record.values.find(_.column.name == "id").get.value.asInstanceOf[Long]
      val filePath = record.values.find(_.column.name == "filePath").get.value.asInstanceOf[String]
      id -> filePath
    }.toMap
    fileIdToPath
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
