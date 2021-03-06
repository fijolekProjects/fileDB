package pl.fijolek.filedb.storage

object DbConstants {
  val pageSize = 4096
  val filePathSize = 100
  val pageIdSize = java.lang.Long.BYTES + java.lang.Long.BYTES
  val pageHeaderSize = pageIdSize + java.lang.Integer.BYTES
  val pageDataSize = DbConstants.pageSize - DbConstants.pageHeaderSize
  val bPlusTreeDegree = 4 //TODO optimize
}
