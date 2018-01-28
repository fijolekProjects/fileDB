package pl.fijolek.filedb.storage

// - record has to be smaller than buffer
// - fixed-length records only
// - heap file organization
// - unspanned records
class FileManager(systemCatalogManager: SystemCatalogManager, recordsIO: RecordsIO){

  def insertRecords(tableName: String, records: List[Record]): Unit = {
    val storedTableData = systemCatalogManager.readCatalog.tablesByName(tableName)
    recordsIO.insertRecords(storedTableData, records)
  }

  //TODO make it lazy? or fetch just n buffers
  def readRecords(tableName: String): List[Record] = {
    val storedTableData = systemCatalogManager.readCatalog.tablesByName(tableName)
    recordsIO.readRecords(storedTableData)
  }

  //TODO assumption - no duplicates
  def searchRecord(tableName: String, column: String, key: Any): Option[Record] = {
    val storedTableData = systemCatalogManager.readCatalog.tablesByName(tableName)
    val index = storedTableData.indices(column)
    val indexRecord = index.search(key.asInstanceOf[Long])
    indexRecord.flatMap { indexRec =>
      val pageId = PageId(storedTableData.fileId, indexRec.value)
      recordsIO.readPageRecords(storedTableData, pageId).find { record =>
        record.values.exists(value => value.column.name == column && value.value == key)
      }
    }
  }

  def deleteRecord(tableName: String, record: Record): Unit = {
    val storedTableData = systemCatalogManager.readCatalog.tablesByName(tableName)
    recordsIO.delete(storedTableData, record)
  }

}