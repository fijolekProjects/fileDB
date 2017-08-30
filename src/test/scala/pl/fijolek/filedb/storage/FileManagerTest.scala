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
    Files.deleteIfExists(Paths.get(basePath, "instructor"))
    Files.deleteIfExists(Paths.get(basePath, "table"))
    Files.deleteIfExists(Paths.get(basePath, "column"))
  }
  override protected def afterEach(): Unit = {
    super.afterEach()
    Files.deleteIfExists(Paths.get(basePath, "instructor"))
    Files.deleteIfExists(Paths.get(basePath, "table"))
    Files.deleteIfExists(Paths.get(basePath, "column"))
  }

  val instructorTableData = TableData(
    name = "instructor",
    columnsDefinition = List(
      Column("ID", ColumnTypes.Varchar(5)),
      Column("name", ColumnTypes.Varchar(20))
    )
  )

  val largeInstructorTableData = TableData(
    name = "instructor",
    columnsDefinition = List(
      Column("ID", ColumnTypes.Varchar(5)),
      Column("name", ColumnTypes.Varchar(2000))
    )
  )

  feature("system catalog") {
    scenario("should store table metadata") {
      val defaultCatalog = systemCatalogManager.readCatalog

      defaultCatalog shouldBe SystemCatalog(
        List(
          StoredTableData(SystemCatalogManager.tableTable, "/tmp/filedb/table"),
          StoredTableData(SystemCatalogManager.columnTable, "/tmp/filedb/column")
        )
      )
    }

    scenario("should add table data to catalog") {
      val defaultCatalog = systemCatalogManager.readCatalog

      systemCatalogManager.createTable(instructorTableData)

      systemCatalogManager.readCatalog shouldBe SystemCatalog(
        List(
          StoredTableData(instructorTableData, "/tmp/filedb/instructor")
        ) ++ defaultCatalog.tables
      )
    }

    scenario("should store catalog data") {
      systemCatalogManager.createTable(instructorTableData)

      fileManager.readRecords("table") shouldBe List(
        Record(List(Value(Column("name", Varchar(32)), "instructor"), Value(Column("filePath", Varchar(100)), "/tmp/filedb/instructor")))
      )
      fileManager.readRecords("column") shouldBe List(
        Record(List(
          Value(Column("tableId", Varchar(32)), "instructor"),
          Value(Column("name", Varchar(32)), "ID"),
          Value(Column("type", Varchar(32)), "Varchar(5)"))
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
      val record = instructorRecord("123", "abc")
      val records = List(record)
      systemCatalogManager.createTable(instructorTableData)

      fileManager.insertRecords(instructorTableData.name, records)

      val recordRead = fileManager.readRecords(instructorTableData.name)
      recordRead shouldBe records
    }

    scenario("should be able to insert 2 records and retrieve them") {
      val record = instructorRecord("123", "abc")
      val record2 = instructorRecord("234", "bcd")
      val records = List(record, record2)
      systemCatalogManager.createTable(instructorTableData)

      fileManager.insertRecords(instructorTableData.name, records)

      val recordsRead = fileManager.readRecords(instructorTableData.name)
      recordsRead shouldBe records
    }

    scenario("should be able to delete record") {
      val record = instructorRecord("123", "abc")
      val record2 = instructorRecord("234", "bcd")
      val record3 = instructorRecord("345", "cde")
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
      val record = largeInstructorRecord("123", "abc")
      val record2 = largeInstructorRecord("234", "bcd")
      val record3 = largeInstructorRecord("345", "cde")

      val records = List(record, record2, record3)
      systemCatalogManager.createTable(largeInstructorTableData)

      fileManager.insertRecords(largeInstructorTableData.name, records)

      val recordsRead = fileManager.readRecords(largeInstructorTableData.name)
      recordsRead shouldBe records
    }

    scenario("should be able to store more than one page data (page is 4k) when data inserted in separate batches") {
      val record = largeInstructorRecord("123", "abc")
      val record2 = largeInstructorRecord("234", "bcd")
      val record3 = largeInstructorRecord("345", "cde")

      val records = List(record, record2, record3)
      systemCatalogManager.createTable(largeInstructorTableData)

      fileManager.insertRecords(largeInstructorTableData.name, List(record, record2))
      fileManager.insertRecords(largeInstructorTableData.name, List(record3))

      val recordsRead = fileManager.readRecords(largeInstructorTableData.name)
      recordsRead shouldBe records
    }

  }

  def instructorRecord(id: String, name: String): Record = {
    Record(List(
      Value(Column("ID", ColumnTypes.Varchar(5)), id),
      Value(Column("name", ColumnTypes.Varchar(20)), name)
    ))
  }

  def largeInstructorRecord(id: String, name: String): Record = {
    Record(List(
      Value(Column("ID", ColumnTypes.Varchar(5)), id),
      Value(Column("name", ColumnTypes.Varchar(2000)), name)
    ))
  }


}
