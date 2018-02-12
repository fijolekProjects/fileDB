package pl.fijolek.filedb.storage

import java.nio.file.{Files, Paths}

object TestFactory {
  val basePath = "/tmp/filedb"
  private val fileIdMapper = new FileIdMapper(basePath)
  private val pageIO = new PageIO(fileIdMapper)
  private val recordsIO = new RecordsIO(fileIdMapper, pageIO)
  val systemCatalogManager = new SystemCatalogManager(basePath, recordsIO, fileIdMapper, pageIO)
  val fileManager = new FileManager(systemCatalogManager, recordsIO)

  def cleanAllTables(userTables: List[String], path: String = basePath): Unit = {
    userTables.foreach { name =>
      Files.deleteIfExists(Paths.get(path, name))
    }
    Files.deleteIfExists(Paths.get(path, "file"))
    Files.deleteIfExists(Paths.get(path, "table"))
    Files.deleteIfExists(Paths.get(path, "column"))
    Files.deleteIfExists(Paths.get(path, "index"))
    ()
  }

}

object TestData {
  val instructorTableData = TableData(
    name = "instructor",
    columnsDefinition = List(
      Column("ID", ColumnTypes.BigInt),
      Column("name", ColumnTypes.Varchar(20))
    )
  )

  val largeInstructorTableData = TableData(
    name = "instructor",
    columnsDefinition = List(
      Column("ID", ColumnTypes.BigInt),
      Column("name", ColumnTypes.Varchar(2000))
    )
  )

  def instructorRecord(id: Long, name: String): Record = {
    Record(List(
      Value(Column("ID", ColumnTypes.BigInt), id),
      Value(Column("name", ColumnTypes.Varchar(20)), name)
    ))
  }

  def nameRecord(name: String): Record = {
    Record(List(
      Value(Column("name", ColumnTypes.Varchar(20)), name)
    ))
  }


  def largeInstructorRecord(id: Long, name: String): Record = {
    Record(List(
      Value(Column("ID", ColumnTypes.BigInt), id),
      Value(Column("name", ColumnTypes.Varchar(2000)), name)
    ))
  }
}

