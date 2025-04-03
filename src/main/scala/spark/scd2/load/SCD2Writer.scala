package spark.scd2.load

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import spark.scd2.utils.{BackupManager, ErrorCollector, Filters, HdfsBackupManager, HdfsFileManager, Location}

import java.net.URI
import java.nio.file.Paths
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.{Failure, Success, Try}

case class SCD2WriteConfig(
                            incomingScd2DF: DataFrame,
                            targetTableName: String,
                            partitionMapColumnValues: (String, Seq[Int]) = "active_flg" -> Seq(0, 1)
                          )
object SCD2Writer {
  def write(config: SCD2WriteConfig, spark: SparkSession): Unit = {

    // Формирование необходимых директорий для работы функционала
    val tableLocation = Location.getTableLocation(config.targetTableName, spark)
    val targetPartition = Location.getSpecPartitionLocation(config.targetTableName, Map("active_flg" -> "1"), spark)

    val localDateTime = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyyMMdd"))

    val tempPartition = Paths.get(tableLocation, s"_temp_active_$localDateTime").toString
    val backupPartition = Paths.get(tableLocation, s"_backup_active_$localDateTime").toString

    val targetPath = new Path(targetPartition)
    val tempPath = new Path(tempPartition)
    val backupPath = new Path(backupPartition)

    val fs = FileSystem.get(new URI(tableLocation), spark.sparkContext.hadoopConfiguration)

    // Инициализация объекта класса сбора ошибок.
    val errorCollector = new ErrorCollector()
    // Инициализация объекта класса по работе файловой системой
    val hdfsFileManager = new HdfsFileManager(fs)
    // Инициализация объекта класса по работе с бэкапом
    val hdfsBackupManager = new HdfsBackupManager(hdfsFileManager)
    // Инициализация объекта скрытого класса SCD2Writer
    val scd2Writer = new SCD2Writer(fs, hdfsBackupManager)


    // 1. Отбор активных и неактивных записей из инкремента.
    val activeRecordsDF = scd2Writer.
      filterActive(config.incomingScd2DF, config.partitionMapColumnValues._1)

    val nonActiveRecordsDF = scd2Writer.
      filterNonActive(config.incomingScd2DF, config.partitionMapColumnValues._1)

    Try {
      // 2. Запись исторических данных
      scd2Writer.processHistoricalData(nonActiveRecordsDF, config.targetTableName)

      // 3. Запись актуальных данных
      scd2Writer.processActiveData(
        activeRecordsDF,
        targetPath,
        tempPath,
        backupPath
      )

    } match {
      case Success(_) =>
        hdfsFileManager.deletePath(tempPath)
        hdfsFileManager.deletePath(backupPath)

      case Failure(e) =>
        errorCollector.addError(e.getMessage)
        hdfsBackupManager.restore(backupPath, targetPath, tempPath, fs)
    }

    // Выход из приложения с ошибкой, если такова была в процессе работы.
    errorCollector.throwIfErrorsExists()
  }
}

private[scd2] class SCD2Writer(fs: FileSystem, backupManager: BackupManager) {

  /** Формирование временной таблицы */
  private def createTempTable(incomingDF: DataFrame, tempPath: String): Unit = {
    incomingDF.
      write.
      option("mergeSchema", "true"). // Для совместимости схем
      mode(SaveMode.Overwrite).
      parquet(tempPath)
  }
  /** Фильтрация неактивных записей */
  private def filterNonActive(existingDF: DataFrame, partCol: String): DataFrame =
    existingDF.filter(Filters.isNonActualRecord(partCol))

  /** Фильтрация активных записей */
  private def filterActive(existingDF: DataFrame, partCol: String): DataFrame =
    existingDF.filter(Filters.isActualRecord(partCol))

  /**  Обработка архивных данных */
  private def processHistoricalData(incomingDF: DataFrame, tableName: String): Unit = {
    incomingDF.
      repartition(1).
      write.
      mode(SaveMode.Append).
      insertInto(tableName)
  }

  /** Обработка архивных данных */
  private def processActiveData(
                                 incomingDF: DataFrame,
                                 targetPath: Path,
                                 tempPath: Path,
                                 backupPath: Path
                               ): Unit = {
    // 1. Запись во временную директорию с проверкой схемы
    createTempTable(incomingDF, tempPath.toString)

    // 2. Создание бэкапа
    backupManager.createBackup(backupPath, targetPath, fs)

    // 3. Атомарная замена партиций
    if(!fs.rename(tempPath, targetPath)) {
      throw new RuntimeException("Atomic replace failed")
    }
  }
}
