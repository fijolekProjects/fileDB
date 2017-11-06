package pl.fijolek.filedb.storage

object DbConstants {
  val pageSize = 4096
  val filePathSize = 100
  val pageIdSize = filePathSize + java.lang.Long.BYTES
  val pageHeaderSize = pageIdSize + java.lang.Integer.BYTES
}
