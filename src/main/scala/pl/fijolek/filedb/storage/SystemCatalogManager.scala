package pl.fijolek.filedb.storage

import java.io.File
import java.nio.file.Paths

import pl.fijolek.filedb.storage.ColumnTypes.ColumnType
import pl.fijolek.filedb.storage.bplustree.{BPlusTreeProtocol, DiskBasedBPlusTree}
import pl.fijolek.filedb.storage.bplustree.CollectionImplicits._

object SystemCatalogManager {

  val fileTableFileId = -1L
  val tableTableFileId = -2L
  val columnTableFileId = -3L
  val indexTableFileId = -4L

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
      Column("tableName", ColumnTypes.Varchar(32)),
      Column("name", ColumnTypes.Varchar(32)),
      Column("type", ColumnTypes.Varchar(32))
    )
  )

  val indexTable = TableData(
    name = "index",
    columnsDefinition = List(
      Column("tableName", ColumnTypes.Varchar(32)),
      Column("name", ColumnTypes.Varchar(32)), //TODO reference column by columnId?
      Column("fileId", ColumnTypes.BigInt)
    )
  )

  val internalFiles: Map[Long, TableData] = Map(
    fileTableFileId -> fileTable,
    tableTableFileId -> tableTable,
    columnTableFileId -> columnTable,
    indexTableFileId -> indexTable
  )

  def toColumnRecord(columnDef: Column, tableId: String): Record = {
    Record(List(
      Value(columnTable.column("tableName"), tableId),
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

  def toIndexRecord(tableName: String, columnName: String, indexFileId: Long): Record = {
    Record(List(
      Value(indexTable.column("tableName"), tableName),
      Value(indexTable.column("name"), columnName),
      Value(indexTable.column("fileId"), indexFileId)
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

  def toIndex(indexRecord: Record): (String, String, Long) = {
    val tableName = indexRecord.values(0).value.toString
    val columnName = indexRecord.values(1).value.toString
    val indexFileId = indexRecord.values(2).value.asInstanceOf[Long]
    (tableName, columnName, indexFileId)
  }

}

class SystemCatalogManager(val basePath: String, recordsIO: RecordsIO, fileIdMapper: FileIdMapper, pageIO: PageIO) {
  import SystemCatalogManager._

  def init(): Unit = {
    createTable(fileTable, fileTableFileId, Map.empty)
    createTable(tableTable, tableTableFileId, Map.empty)
    createTable(columnTable, columnTableFileId, Map.empty)
    createTable(indexTable, indexTableFileId, Map.empty)
  }

  def createTable(tableData: TableData): Unit = {
    val currentMaxFileId = fileIdMapper.maxFileId
    val fileId = currentMaxFileId + 1
    val maxFileId = fileId
    val indexFileIds = tableData.indices.zipWithIndex.map { case (index, zippedIndex) =>
      val nextMaxFileId = maxFileId + zippedIndex + 1
      index -> nextMaxFileId
    }.toMap
    createTable(tableData, fileId, indexFileIds)
  }

  private def createTable(tableData: TableData, tableFileId: Long, indexFileIds: Map[Index, Long]): Unit = {
    val tableFile = filePath(tableData)
    val tableFileRecord = toFileRecord(tableFileId, tableFile)
    val indexFileRecords = indexFileIds.map { case (index, fileId) =>
      val file = filePath(tableData, index)
      (file, toFileRecord(fileId, file))
    }
    val indexRecords = indexFileIds.map { case (index, fileId) =>
      toIndexRecord(tableData.name, index.column, fileId)
    }
    (indexFileRecords.keys ++ List(tableFile)).foreach { file =>
      FileUtils.touchFile(file)
    }
    val tableRecord = toTableRecord(tableData, tableFileId)
    val columnsRecords = tableData.columnsDefinition.map { columnDef => toColumnRecord(columnDef, tableData.name) }
    recordsIO.insertRecords(StoredTableData(fileTable, fileTableFileId, Map.empty), List(tableFileRecord) ++ indexFileRecords.values)
    recordsIO.insertRecords(StoredTableData(tableTable, tableTableFileId, Map.empty), List(tableRecord))
    recordsIO.insertRecords(StoredTableData(columnTable, columnTableFileId, Map.empty), columnsRecords)
    recordsIO.insertRecords(StoredTableData(indexTable, indexTableFileId, Map.empty), indexRecords.toList)
    indexFileIds.foreach { case (_, fileId) =>
      val initialIndex = DiskBasedBPlusTree.apply(DbConstants.bPlusTreeDegree, pageIO, fileId)
      val page = BPlusTreeProtocol.nodeToPage(initialIndex.root.node, initialIndex.root.refId, fileId)
      pageIO.writePage(page)
    }
    ()
  }

  def readCatalog: SystemCatalog = {
    val tables = readInternalTable(StoredTableData(tableTable, tableTableFileId, Map.empty))
    val columns = readInternalTable(StoredTableData(columnTable, columnTableFileId, Map.empty))
    val indices = readInternalTable(StoredTableData(indexTable, indexTableFileId, Map.empty))
    val tablesData = tables.map { tableRecord =>
      val tableName = tableRecord.values(0).value.toString
      val fileId = tableRecord.values(1).value.asInstanceOf[Long]
      tableName -> fileId
    }.toMap
    val columnsData = columns.foldLeft(Map.empty[String, List[Column]]) { case (tableColumnDefinition, columnRecord) =>
      val (tableName, column) = toColumn(columnRecord)
      val columns = tableColumnDefinition.getOrElse(tableName, List.empty[Column])
      tableColumnDefinition.updated(tableName, columns ++ List(column))
    }
    val indicesData = indices.map { indexRecord =>
      val (tableName, columnName, indexFileId) = toIndex(indexRecord)
      val indexPage = pageIO.read(PageId(indexFileId, 0))
      val root = BPlusTreeProtocol.pageToRoot(indexPage)
      val indexTree = DiskBasedBPlusTree.apply(root, pageIO, indexFileId)
      tableName -> (columnName -> indexTree)
    }
    val indicesDataMap = indicesData.tupleListToMap.mapValuesNow(_.toMap)
    val tableInfo = tablesData.map { case (tableName, fileId) =>
      val tableIndices = indicesDataMap.getOrElse(tableName, Map.empty)
      StoredTableData(TableData(tableName, columnsData(tableName)), fileId, tableIndices)
    }.toList
    SystemCatalog(tableInfo)
  }

  private def readInternalTable(storedTableData: StoredTableData): List[Record] = {
    recordsIO.readRecords(storedTableData).toList
  }

  private def filePath(tableData: TableData): File = {
    Paths.get(basePath, tableData.name).toFile
  }

  private def filePath(tableData: TableData, index: Index): File = {
    Paths.get(basePath, s"${tableData.name}-index-${index.column}").toFile
  }

}

case class SystemCatalog(tables: List[StoredTableData]) {
  private val tablesByName = tables.map(table => (table.data.name.toUpperCase, table)).toMap
  def table(name: String) = tablesByName(name.toUpperCase)
}

case class StoredTableData(data: TableData, fileId: Long, indices: Map[String, DiskBasedBPlusTree])
