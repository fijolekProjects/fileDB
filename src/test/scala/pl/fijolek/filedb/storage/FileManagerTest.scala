package pl.fijolek.filedb.storage

import java.nio.file.{Files, Paths}

import org.scalatest.{BeforeAndAfterEach, FeatureSpec, Matchers}
import pl.fijolek.filedb.storage.ColumnTypes.Varchar

class FileManagerTest extends FeatureSpec with Matchers with BeforeAndAfterEach {

  val basePath = "/tmp/filedb"
  val systemCatalogManager = new SystemCatalogManager(basePath)
  val fileManager = new FileManager(systemCatalogManager)

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    cleanTables
    systemCatalogManager.init()
  }
  override protected def afterEach(): Unit = {
    super.afterEach()
    cleanTables
  }

  private def cleanTables = {
    Files.deleteIfExists(Paths.get(basePath, "instructor"))
    Files.deleteIfExists(Paths.get(basePath, "file"))
    Files.deleteIfExists(Paths.get(basePath, "table"))
    Files.deleteIfExists(Paths.get(basePath, "column"))
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
    Record(List(Value(Column("id", ColumnTypes.BigInt), -3), Value(Column("filePath", Varchar(100)), "/tmp/filedb/column")))
  )
  val internalTableRecords = List(
    Record(List(Value(Column("name", Varchar(32)), "file"), Value(Column("fileId", ColumnTypes.BigInt), -1))),
    Record(List(Value(Column("name", Varchar(32)), "table"), Value(Column("fileId", ColumnTypes.BigInt), -2))),
    Record(List(Value(Column("name", Varchar(32)), "column"), Value(Column("fileId", ColumnTypes.BigInt), -3)))
  )
  val internalColumnRecords = List(
    Record(List(Value(Column("tableId", Varchar(32)), "file"), Value(Column("name", Varchar(32)), "id"), Value(Column("type", Varchar(32)), "BigInt"))),
    Record(List(Value(Column("tableId", Varchar(32)), "file"), Value(Column("name", Varchar(32)), "filePath"), Value(Column("type", Varchar(32)), "Varchar(100)"))),
    Record(List(Value(Column("tableId", Varchar(32)), "table"), Value(Column("name", Varchar(32)), "name"), Value(Column("type", Varchar(32)), "Varchar(32)"))),
    Record(List(Value(Column("tableId", Varchar(32)), "table"), Value(Column("name", Varchar(32)), "fileId"), Value(Column("type", Varchar(32)), "BigInt"))),
    Record(List(Value(Column("tableId", Varchar(32)), "column"), Value(Column("name", Varchar(32)), "tableId"), Value(Column("type", Varchar(32)), "Varchar(32)"))),
    Record(List(Value(Column("tableId", Varchar(32)), "column"), Value(Column("name", Varchar(32)), "name"), Value(Column("type", Varchar(32)), "Varchar(32)"))),
    Record(List(Value(Column("tableId", Varchar(32)), "column"), Value(Column("name", Varchar(32)), "type"), Value(Column("type", Varchar(32)), "Varchar(32)")))
  )
  feature("system catalog") {
    scenario("should store table metadata") {
      val defaultCatalog = systemCatalogManager.readCatalog

      defaultCatalog shouldBe SystemCatalog(
        List(
          StoredTableData(SystemCatalogManager.fileTable, "/tmp/filedb/file"),
          StoredTableData(SystemCatalogManager.tableTable, "/tmp/filedb/table"),
          StoredTableData(SystemCatalogManager.columnTable, "/tmp/filedb/column")
        )
      )
      fileManager.readRecords("file") shouldBe internalFileRecords
      fileManager.readRecords("table") shouldBe internalTableRecords
      fileManager.readRecords("column") shouldBe internalColumnRecords
    }

    scenario("should add table data to catalog") {
      val defaultCatalog = systemCatalogManager.readCatalog

      systemCatalogManager.createTable(instructorTableData)

      systemCatalogManager.readCatalog shouldBe SystemCatalog(
        defaultCatalog.tables ++ List(
          StoredTableData(instructorTableData, "/tmp/filedb/instructor")
        )
      )
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
          Value(Column("tableId", Varchar(32)), "instructor"),
          Value(Column("name", Varchar(32)), "ID"),
          Value(Column("type", Varchar(32)), "BigInt"))
        ),
        Record(List(
          Value(Column("tableId", Varchar(32)), "instructor"),
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
