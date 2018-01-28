package pl.fijolek.filedb.storage

import java.nio.file.Paths

import pl.fijolek.filedb.storage.SystemCatalogManager.fileTable

import scala.collection.mutable.ArrayBuffer

class FileIdMapper(basePath: String) {

  def path(fileId: Long): String = {
    internalFilePath(fileId)
      .getOrElse(fileIdToPath(fileId))
  }

  private def internalFilePath(fileId: Long): Option[String] = {
    SystemCatalogManager.internalFiles.get(fileId).map(tableData => Paths.get(basePath, tableData.name).toFile.getAbsolutePath)
  }

  def fileId(path: String): Long = {
    val fileIdMap = fileIdToPath
    fileIdMap.find(_._2 == path).get._1
  }

  def fileIdToPath: Map[Long, String] = {
    val files = readRecords(fileTable, Paths.get(basePath, fileTable.name).toFile.getAbsolutePath)
    val fileIdToPath = files.map { record =>
      val id = record.values.find(_.column.name == "id").get.value.asInstanceOf[Long]
      val filePath = record.values.find(_.column.name == "filePath").get.value.asInstanceOf[String]
      id -> filePath
    }.toMap
    fileIdToPath
  }

  def maxFileId: Long = {
    val files = readRecords(fileTable, Paths.get(basePath, fileTable.name).toFile.getAbsolutePath)
    val ids = files.map { record =>
      val id = record.values.find(_.column.name == "id").get.value.asInstanceOf[Long]
      id
    }
    ids.max
  }

  private def readRecords(tableData: TableData, filePath: String): List[Record] = {
    val records = new ArrayBuffer[Record]()
    FileUtils.traverse(filePath) { page =>
      val recordsRead = tableData.readRecords(page)
      records ++= recordsRead
    }
    records.toList
  }


}
