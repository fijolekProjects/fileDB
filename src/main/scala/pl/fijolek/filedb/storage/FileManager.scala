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

  def deleteRecord(tableName: String, record: Record): Unit = {
    val storedTableData = systemCatalogManager.readCatalog.tablesByName(tableName)
    recordsIO.delete(storedTableData, record)
  }

}