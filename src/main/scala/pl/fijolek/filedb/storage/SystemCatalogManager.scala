package pl.fijolek.filedb.storage

import java.io.File

import pl.fijolek.filedb.storage.ColumnTypes.Varchar

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
