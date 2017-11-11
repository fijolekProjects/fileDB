package pl.fijolek.filedb.storage

class RecordsIO(basePath: String) {
  private val fileIdMapper = new FileIdMapper(basePath)

  def readRecords(tableData: TableData, filePath: String): List[Record] = {
    TableData.readRecords(tableData, filePath)
  }

  def insertRecords(data: TableData, filePath: String, records: List[Record]): Unit = {
    val fileId = fileIdMapper.fileId(filePath)
    insertRecords(data, filePath, records, fileId)
  }

  def insertRecords(data: TableData, filePath: String, records: List[Record], fileId: Long): Unit = {
    val recordsToWriteSize = records.length * data.recordSize
    val lastPage = Page.lastPage(filePath).getOrElse(Page.newPage(fileId, 0))
    val pagesToWrite = if (lastPage.spareBytesAtTheEnd > recordsToWriteSize) {
      List(lastPage.add(records.map(_.toBytes)))
    } else {
      val recordsThatFitsInLastPageCount = lastPage.spareBytesAtTheEnd / data.recordSize
      val recordsToWriteInOtherPagesCount = records.length - recordsThatFitsInLastPageCount
      val recordsInSinglePageCount = (DbConstants.pageSize - DbConstants.pageHeaderSize) / data.recordSize
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
      writePage(page)
    }
    ()
  }

  def delete(data: TableData, filePath: String, record: Record): Unit = {
    FileUtils.traverse(filePath) { page =>
      val newPage = data.prepareDelete(record, page)
      writePage(newPage)
    }
  }

  private def writePage(page: Page): Unit = {
    val filePath = fileIdMapper.path(page.pageId.fileId)
    val toWrite = page.bytes
    FileUtils.write(filePath, page.pageId.offset, toWrite)
  }


}