package pl.fijolek.filedb.storage

import scala.collection.mutable.ArrayBuffer

class RecordsIO(fileIdMapper: FileIdMapper, pageIO: PageIO) {

  def readRecords(storedTableData: StoredTableData): List[Record] = {
    val tableData = storedTableData.data
    val records = new ArrayBuffer[Record]()
    val filePath = fileIdMapper.path(storedTableData.fileId)
    FileUtils.traverse(filePath) { page =>
      val recordsRead = tableData.readRecords(page)
      records ++= recordsRead
    }
    records.toList
  }

  def insertRecords(tableData: StoredTableData, records: List[Record]): Unit = {
    val recordSize = tableData.data.recordSize
    val fileId = tableData.fileId
    val recordsToWriteSize = records.length * recordSize
    val lastPage = pageIO.lastPage(fileId).getOrElse(Page.newPage(fileId, 0))
    val pagesToWrite = if (lastPage.spareBytesAtTheEnd > recordsToWriteSize) {
      List(lastPage.add(records.map(_.toBytes)))
    } else {
      val recordsThatFitsInLastPageCount = lastPage.spareBytesAtTheEnd / recordSize
      val recordsToWriteInOtherPagesCount = records.length - recordsThatFitsInLastPageCount
      val recordsInSinglePageCount = (DbConstants.pageSize - DbConstants.pageHeaderSize) / recordSize
      val newPagesCount = Math.ceil(recordsToWriteInOtherPagesCount.toDouble / recordsInSinglePageCount.toDouble).toInt
      val lastPageFull = lastPage.add(records.take(recordsThatFitsInLastPageCount).map(_.toBytes))
      val newPages = (0 until newPagesCount).toList.map { i =>
        val pageOffset = lastPage.offset + (i + 1) * DbConstants.pageSize
        val recordsOffset = i * recordsInSinglePageCount + recordsThatFitsInLastPageCount
        val newPageRecords = records.slice(recordsOffset, recordsOffset + recordsInSinglePageCount)
        val newPageRecordsBytes = newPageRecords.map(_.toBytes)
        val newPage = Page.newPage(fileId, pageOffset)
        newPage.add(newPageRecordsBytes)
      }
      lastPageFull :: newPages
    }

    pagesToWrite.foreach { page =>
      pageIO.writePage(page)
    }
    ()
  }

  def delete(storedTableData: StoredTableData, record: Record): Unit = {
    val data = storedTableData.data
    val filePath = fileIdMapper.path(storedTableData.fileId)
    FileUtils.traverse(filePath) { page =>
      val newPage = data.prepareDelete(record, page)
      pageIO.writePage(newPage)
    }
  }


}