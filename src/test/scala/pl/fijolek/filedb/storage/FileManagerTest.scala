package pl.fijolek.filedb.storage

import java.nio.file.{Files, Paths}

import org.scalatest.{BeforeAndAfterEach, FeatureSpec, Matchers}

class FileManagerTest extends FeatureSpec with Matchers with BeforeAndAfterEach {

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    Files.deleteIfExists(Paths.get("/tmp/filedb/instructor"))
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    Files.deleteIfExists(Paths.get("/tmp/filedb/instructor"))
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
    scenario("should add table to catalog") {
      val systemCatalogManager = new SystemCatalogManager("/tmp/filedb")

      systemCatalogManager.addTable(instructorTableData)

      systemCatalogManager.readCatalog shouldBe SystemCatalog(
        List(StoredTableData(instructorTableData, "/tmp/filedb/instructor"))
      )
    }
  }

  feature("file manager") {
    scenario("should be able to insert some record and retrieve it") {
      val systemCatalogManager = new SystemCatalogManager("/tmp/filedb")
      val fileManager = new FileManager(systemCatalogManager)
      val record = instructorRecord("123", "abc")
      val records = List(record)
      systemCatalogManager.addTable(instructorTableData)

      fileManager.insertRecords(instructorTableData.name, records)

      val recordRead = fileManager.readRecords(instructorTableData.name)
      recordRead shouldBe records
    }

    scenario("should be able to insert 2 records and retrieve them") {
      val systemCatalogManager = new SystemCatalogManager("/tmp/filedb")
      val fileManager = new FileManager(systemCatalogManager)
      val record = instructorRecord("123", "abc")
      val record2 = instructorRecord("234", "bcd")
      val records = List(record, record2)
      systemCatalogManager.addTable(instructorTableData)

      fileManager.insertRecords(instructorTableData.name, records)

      val recordsRead = fileManager.readRecords(instructorTableData.name)
      recordsRead shouldBe records
    }

    scenario("should be able to delete record") {
      val systemCatalogManager = new SystemCatalogManager("/tmp/filedb")
      val fileManager = new FileManager(systemCatalogManager)
      val record = instructorRecord("123", "abc")
      val record2 = instructorRecord("234", "bcd")
      val record3 = instructorRecord("345", "cde")
      val records = List(record, record2, record3)
      systemCatalogManager.addTable(instructorTableData)

      fileManager.insertRecords(instructorTableData.name, records)

      val recordsRead = fileManager.readRecords(instructorTableData.name)
      recordsRead shouldBe records

      fileManager.deleteRecord(instructorTableData.name, record2)

      val recordsReadAfterDeletion = fileManager.readRecords(instructorTableData.name)
      recordsReadAfterDeletion shouldBe List(record, record3)
    }

    scenario("should be able to store more than one page data (page is 4k)") {
      val systemCatalogManager = new SystemCatalogManager("/tmp/filedb")
      val fileManager = new FileManager(systemCatalogManager)
      val record = largeInstructorRecord("123", "abc")
      val record2 = largeInstructorRecord("234", "bcd")
      val record3 = largeInstructorRecord("345", "cde")

      val records = List(record, record2, record3)
      systemCatalogManager.addTable(largeInstructorTableData)

      fileManager.insertRecords(largeInstructorTableData.name, records)

      val recordsRead = fileManager.readRecords(largeInstructorTableData.name)
      recordsRead shouldBe records
    }

    scenario("should be able to store more than one page data (page is 4k) when data inserted in separate batches") {
      val systemCatalogManager = new SystemCatalogManager("/tmp/filedb")
      val fileManager = new FileManager(systemCatalogManager)
      val record = largeInstructorRecord("123", "abc")
      val record2 = largeInstructorRecord("234", "bcd")
      val record3 = largeInstructorRecord("345", "cde")

      val records = List(record, record2, record3)
      systemCatalogManager.addTable(largeInstructorTableData)

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
