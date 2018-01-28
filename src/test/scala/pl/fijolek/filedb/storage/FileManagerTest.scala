package pl.fijolek.filedb.storage

import java.nio.file.{Files, Paths}

import org.scalatest.{BeforeAndAfterEach, FeatureSpec, Matchers}
import pl.fijolek.filedb.storage.ColumnTypes.Varchar
import com.github.ghik.silencer.silent

class FileManagerTest extends FeatureSpec with Matchers with BeforeAndAfterEach {

  val basePath = "/tmp/filedb"
  val fileIdMapper = new FileIdMapper(basePath)
  val pageIO = new PageIO(fileIdMapper)
  val recordsIO = new RecordsIO(fileIdMapper, pageIO)
  val systemCatalogManager = new SystemCatalogManager(basePath, recordsIO, fileIdMapper, pageIO)
  val fileManager = new FileManager(systemCatalogManager, recordsIO)

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    cleanTables()
    systemCatalogManager.init()
  }
  override protected def afterEach(): Unit = {
    super.afterEach()
    cleanTables()
  }

  @silent
  private def cleanTables(): Unit = {
    Files.deleteIfExists(Paths.get(basePath, "instructor"))
    Files.deleteIfExists(Paths.get(basePath, "file"))
    Files.deleteIfExists(Paths.get(basePath, "table"))
    Files.deleteIfExists(Paths.get(basePath, "column"))
    Files.deleteIfExists(Paths.get(basePath, "index"))
  }

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

  val internalFileRecords = List(
    Record(List(Value(Column("id", ColumnTypes.BigInt), -1), Value(Column("filePath", Varchar(100)), "/tmp/filedb/file"))),
    Record(List(Value(Column("id", ColumnTypes.BigInt), -2), Value(Column("filePath", Varchar(100)), "/tmp/filedb/table"))),
    Record(List(Value(Column("id", ColumnTypes.BigInt), -3), Value(Column("filePath", Varchar(100)), "/tmp/filedb/column"))),
    Record(List(Value(Column("id", ColumnTypes.BigInt), -4), Value(Column("filePath", Varchar(100)), "/tmp/filedb/index"))),
  )
  val internalTableRecords = List(
    Record(List(Value(Column("name", Varchar(32)), "file"), Value(Column("fileId", ColumnTypes.BigInt), -1))),
    Record(List(Value(Column("name", Varchar(32)), "table"), Value(Column("fileId", ColumnTypes.BigInt), -2))),
    Record(List(Value(Column("name", Varchar(32)), "column"), Value(Column("fileId", ColumnTypes.BigInt), -3))),
    Record(List(Value(Column("name", Varchar(32)), "index"), Value(Column("fileId", ColumnTypes.BigInt), -4)))
  )
  val internalColumnRecords = List(
    Record(List(Value(Column("tableName", Varchar(32)), "file"), Value(Column("name", Varchar(32)), "id"), Value(Column("type", Varchar(32)), "BigInt"))),
    Record(List(Value(Column("tableName", Varchar(32)), "file"), Value(Column("name", Varchar(32)), "filePath"), Value(Column("type", Varchar(32)), "Varchar(100)"))),
    Record(List(Value(Column("tableName", Varchar(32)), "table"), Value(Column("name", Varchar(32)), "name"), Value(Column("type", Varchar(32)), "Varchar(32)"))),
    Record(List(Value(Column("tableName", Varchar(32)), "table"), Value(Column("name", Varchar(32)), "fileId"), Value(Column("type", Varchar(32)), "BigInt"))),
    Record(List(Value(Column("tableName", Varchar(32)), "column"), Value(Column("name", Varchar(32)), "tableName"), Value(Column("type", Varchar(32)), "Varchar(32)"))),
    Record(List(Value(Column("tableName", Varchar(32)), "column"), Value(Column("name", Varchar(32)), "name"), Value(Column("type", Varchar(32)), "Varchar(32)"))),
    Record(List(Value(Column("tableName", Varchar(32)), "column"), Value(Column("name", Varchar(32)), "type"), Value(Column("type", Varchar(32)), "Varchar(32)"))),
    Record(List(Value(Column("tableName", Varchar(32)), "index"), Value(Column("name", Varchar(32)), "tableName"), Value(Column("type", Varchar(32)), "Varchar(32)"))),
    Record(List(Value(Column("tableName", Varchar(32)), "index"), Value(Column("name", Varchar(32)), "name"), Value(Column("type", Varchar(32)), "Varchar(32)"))),
    Record(List(Value(Column("tableName", Varchar(32)), "index"), Value(Column("name", Varchar(32)), "fileId"), Value(Column("type", Varchar(32)), "BigInt")))
  )
  feature("system catalog") {
    scenario("should store table metadata") {
      val defaultCatalog = systemCatalogManager.readCatalog

      defaultCatalog shouldBe SystemCatalog(
        List(
          StoredTableData(SystemCatalogManager.fileTable, -1, Map.empty),
          StoredTableData(SystemCatalogManager.tableTable, -2, Map.empty),
          StoredTableData(SystemCatalogManager.columnTable, -3, Map.empty),
          StoredTableData(SystemCatalogManager.indexTable, -4, Map.empty)
        )
      )
      fileManager.readRecords("file") shouldBe internalFileRecords
      fileManager.readRecords("table") shouldBe internalTableRecords
      fileManager.readRecords("column") shouldBe internalColumnRecords
    }

    scenario("should add table data to catalog") {
      val defaultCatalog = systemCatalogManager.readCatalog

      systemCatalogManager.createTable(instructorTableData)

      systemCatalogManager.readCatalog.tables.toSet shouldBe
        (StoredTableData(instructorTableData, 0, Map.empty) :: defaultCatalog.tables).toSet
    }

    scenario("should store catalog data") {
      systemCatalogManager.createTable(instructorTableData)

      fileManager.readRecords("table") shouldBe internalTableRecords ++ List(
        Record(List(Value(Column("name", Varchar(32)), "instructor"), Value(Column("fileId", ColumnTypes.BigInt), 0L)))
      )
      fileManager.readRecords("file") shouldBe internalFileRecords ++ List(
        Record(List(Value(Column("id", ColumnTypes.BigInt), 0L), Value(Column("filePath", Varchar(100)), "/tmp/filedb/instructor")))
      )
      fileManager.readRecords("column") shouldBe internalColumnRecords ++ List(
        Record(List(
          Value(Column("tableName", Varchar(32)), "instructor"),
          Value(Column("name", Varchar(32)), "ID"),
          Value(Column("type", Varchar(32)), "BigInt"))
        ),
        Record(List(
          Value(Column("tableName", Varchar(32)), "instructor"),
          Value(Column("name", Varchar(32)), "name"),
          Value(Column("type", Varchar(32)), "Varchar(20)"))
        )
      )
    }

  }

  feature("file manager") {
    scenario("should be able to insert some record and retrieve it") {
      val record = instructorRecord(123, "abc")
      val records = List(record)
      systemCatalogManager.createTable(instructorTableData)

      fileManager.insertRecords(instructorTableData.name, records)

      val recordRead = fileManager.readRecords(instructorTableData.name)
      recordRead shouldBe records
    }

    scenario("should be able to insert 2 records and retrieve them") {
      val record = instructorRecord(123, "abc")
      val record2 = instructorRecord(234, "bcd")
      val records = List(record, record2)
      systemCatalogManager.createTable(instructorTableData)

      fileManager.insertRecords(instructorTableData.name, records)

      val recordsRead = fileManager.readRecords(instructorTableData.name)
      recordsRead shouldBe records
    }

    scenario("should be able to delete record") {
      val record = instructorRecord(123, "abc")
      val record2 = instructorRecord(234, "bcd")
      val record3 = instructorRecord(345, "cde")
      val records = List(record, record2, record3)
      systemCatalogManager.createTable(instructorTableData)

      fileManager.insertRecords(instructorTableData.name, records)

      val recordsRead = fileManager.readRecords(instructorTableData.name)
      recordsRead shouldBe records

      fileManager.deleteRecord(instructorTableData.name, record2)

      val recordsReadAfterDeletion = fileManager.readRecords(instructorTableData.name)
      recordsReadAfterDeletion shouldBe List(record, record3)
    }

    scenario("should be able to store more than one page data (page is 4k)") {
      val record = largeInstructorRecord(123, "abc")
      val record2 = largeInstructorRecord(234, "bcd")
      val record3 = largeInstructorRecord(345, "cde")

      val records = List(record, record2, record3)
      systemCatalogManager.createTable(largeInstructorTableData)

      fileManager.insertRecords(largeInstructorTableData.name, records)

      val recordsRead = fileManager.readRecords(largeInstructorTableData.name)
      recordsRead shouldBe records
    }

    scenario("should be able to store more than one page data (page is 4k) when data inserted in separate batches") {
      val record = largeInstructorRecord(123, "abc")
      val record2 = largeInstructorRecord(234, "bcd")
      val record3 = largeInstructorRecord(345, "cde")

      val records = List(record, record2, record3)
      systemCatalogManager.createTable(largeInstructorTableData)

      fileManager.insertRecords(largeInstructorTableData.name, List(record, record2))
      fileManager.insertRecords(largeInstructorTableData.name, List(record3))

      val recordsRead = fileManager.readRecords(largeInstructorTableData.name)
      recordsRead shouldBe records
    }

    scenario("should be able to retrieve key using index") {
      val record = instructorRecord(123, "abc")
      val record2 = instructorRecord(234, "bcd")
      val record3 = instructorRecord(345, "cde")
      val records = List(record, record2, record3)

      systemCatalogManager.createTable(instructorTableData.copy(indices = List(Index("ID"))))
      fileManager.insertRecords(instructorTableData.name, records)

      fileManager.searchRecord(instructorTableData.name, "ID", 345L) shouldBe Some(record3)
    }

    scenario("should be able to retrieve key using index with large number of records") {
      val records = (1 to 100).toList.map { i =>
        instructorRecord(i.toLong, i.toString)
      }

      systemCatalogManager.createTable(instructorTableData.copy(indices = List(Index("ID"))))
      fileManager.insertRecords(instructorTableData.name, records)

      val id = 50L
      fileManager.searchRecord(instructorTableData.name, "ID", id) shouldBe Some(instructorRecord(id, id.toString))
    }

    ignore("should be able to retrieve key using index with large number of records #2") {
      val records = (1 to 500).toList.map { i =>
        instructorRecord(i.toLong, i.toString)
      }

      systemCatalogManager.createTable(instructorTableData.copy(indices = List(Index("ID"))))
      fileManager.insertRecords(instructorTableData.name, records)

      val id = 250L
      fileManager.searchRecord(instructorTableData.name, "ID", id) shouldBe Some(instructorRecord(id, id.toString))
    }

    ignore("should be able to retrieve key using large index") {
      val record = largeInstructorRecord(123, "abc")
      val record2 = largeInstructorRecord(234, "bcd")
      val record3 = largeInstructorRecord(345, "cde")

      systemCatalogManager.createTable(largeInstructorTableData.copy(indices = List(Index("ID"))))
      fileManager.insertRecords(largeInstructorTableData.name, List(record, record2, record3))

      fileManager.searchRecord(largeInstructorTableData.name, "ID", 123L) shouldBe Some(record)
    }
  }

  def instructorRecord(id: Long, name: String): Record = {
    Record(List(
      Value(Column("ID", ColumnTypes.BigInt), id),
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
