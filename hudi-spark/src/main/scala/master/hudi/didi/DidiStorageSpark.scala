package master.hudi.didi
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
  * 滴滴海口出行运营数据分析，使用SparkSQL操作数据，先读取CSV文件，保存至Hudi表。
  *    -1. 数据集说明
  *        2017年5月1日-10月31日海口市每天的订单数据，包含订单的起终点经纬度以及订单类型、出行品类、乘车人数的订单属性数据。
  *        数据存储为CSV格式，首行为列名称
  *    -2. 开发主要步骤
  *      step1. 构建SparkSession实例对象（集成Hudi和HDFS）
  *      step2. 加载本地CSV文件格式滴滴出行数据
  *      step3. 滴滴出行数据ETL处理
  *      stpe4. 保存转换后数据至Hudi表
  *      step5. 应用结束关闭资源
  */
object DidiStorageSpark {
  // 滴滴数据路径
  val datasPath: String = "file:///D:\\Idea project\\dwv_order_make_haikou_1.txt"

  // Hudi中表的属性
  val hudiTableName: String = "tbl_didi_haikou"
  val hudiTablePath: String = "/hudi-warehouse/tbl_didi_haikou"

  /**
    * 读取CSV格式文本文件数据，封装到DataFrame数据集
    */
  def readCsvFile(spark: SparkSession, path: String): DataFrame = {
    spark.read
      // 设置分隔符为逗号
      .option("sep", "\\t")
      // 文件首行为列名称
      .option("header", "true")
      // 依据数值自动推断数据类型
      .option("inferSchema", "true")
      // 指定文件路径
      .csv(path)
  }
  /**
    * 对滴滴出行海口数据进行ETL转换操作：指定ts和partitionpath 列
    */
  def process(dataframe: DataFrame): DataFrame = {
    dataframe
      // 添加分区列：三级分区 -> yyyy/MM/dd
      .withColumn(
      "partitionpath",  // 列名称
      concat_ws("/", col("year"), col("month"), col("day")) //
    )
      // 删除列：year, month, day
      .drop("year", "month", "day")
      // 添加timestamp列，作为Hudi表记录数据与合并时字段，使用发车时间
      .withColumn(
      "ts",
      unix_timestamp(col("departure_time"), "yyyy-MM-dd HH:mm:ss")
    )
  }

  /**
    * 将数据集DataFrame保存至Hudi表中，表的类型为COW，属于批量保存数据，写少读多
    */
  def saveToHudi(dataframe: DataFrame, table: String, path: String): Unit = {
    // 导入包
    import org.apache.hudi.DataSourceWriteOptions._
    import org.apache.hudi.config.HoodieWriteConfig._

    // 保存数据
    dataframe.write
      .mode(SaveMode.Overwrite)
      .format("hudi")
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      // Hudi 表的属性值设置
      .option(RECORDKEY_FIELD.key(), "order_id")
      .option(PRECOMBINE_FIELD.key(), "ts")
      .option(PARTITIONPATH_FIELD.key(), "partitionpath")
      .option(TBL_NAME.key(), table)
      .save(path)
  }

  def main(args: Array[String]): Unit = {
    // step1. 构建SparkSession实例对象（集成Hudi和HDFS）
    val spark: SparkSession = SparkUtils.CreateSparkSession(this.getClass)
    import spark.implicits._

    // step2. 加载本地CSV文件格式滴滴出行数据
    val didiDF: DataFrame = readCsvFile(spark, datasPath)
//     didiDF.printSchema()
//     didiDF.show(10, truncate = false)

    // step3. 滴滴出行数据ETL处理并保存至Hudi表
    val etlDF: DataFrame = process(didiDF)
//    etlDF.printSchema()
//    etlDF.show(10, truncate = false)

    // stpe4. 保存转换后数据至Hudi表   Hudi名称和路径
    saveToHudi(etlDF, hudiTableName, hudiTablePath)

    // stpe5. 应用结束，关闭资源
    spark.stop()
  }

}
