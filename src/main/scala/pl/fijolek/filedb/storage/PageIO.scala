package pl.fijolek.filedb.storage

class PageIO(fileIdMapper: FileIdMapper) {

  def writePage(page: Page): Unit = {
    val filePath = fileIdMapper.path(page.pageId.fileId)
    val toWrite = page.bytes
    FileUtils.write(filePath, page.pageId.offset, toWrite)
  }

  def lastPage(fileId: Long): Option[Page] = {
    val filePath = fileIdMapper.path(fileId)
    FileUtils.withFileOpen(filePath) { file =>
      val fileSize = file.length()
      if (fileSize == 0) {
        None
      } else {
        val pageOffset = (fileSize / DbConstants.pageSize) * DbConstants.pageSize
        file.seek(pageOffset)
        val pageBytes = new Array[Byte](DbConstants.pageSize)
        file.read(pageBytes)
        Some(Page(pageBytes))
      }
    }
  }

}
