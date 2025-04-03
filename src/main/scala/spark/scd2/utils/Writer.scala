package spark.scd2.utils

import org.apache.spark.sql.{SaveMode, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.Try

private object BackupManager {

  private val currentDate = LocalDate.parse(LocalDate.now().toString, DateTimeFormatter.ISO_DATE_TIME)

  def formBackupTableName(table: String): String = s"${table}_backup_$currentDate"
}

object Writer {

}

private class Writer(spark: SparkSession) {
  private def safetyOverwrite(table: String): Try[Unit] = Try {
    createBackUp(table)
    
  }

  private def createBackUp(table: String): Unit = {
    spark.table(table).
      write.
      mode(SaveMode.Overwrite).
      saveAsTable(BackupManager.formBackupTableName(table))
  }
}
