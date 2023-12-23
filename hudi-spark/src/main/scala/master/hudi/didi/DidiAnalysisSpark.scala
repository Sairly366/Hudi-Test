package master.hudi.didi
import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
  * 滴滴海口出行运营数据分析，使用SparkSQL操作数据，从加载Hudi表数据，按照业务需求统计。
  *    -1. 数据集说明
  *        海口市每天的订单数据，包含订单的起终点经纬度以及订单类型、出行品类、乘车人数的订单属性数据。
  *        数据存储为CSV格式，首行为列名称
  *    -2. 开发主要步骤
  *      step1. 构建SparkSession实例对象（集成Hudi和HDFS）
  *      step2. 依据指定字段从Hudi表中加载数据
  *      step3. 按照业务指标进行数据统计分析
  *      step4. 应用结束关闭资源
  */
object DidiAnalysisSpark {
  // Hudi中表的属性,存储数据HDFS路径
  val hudiTablePath: String = "/hudi-warehouse/tbl_didi_haikou"

  /**
    * 从Hudi表加载数据，指定数据存在路径
    */
  def readFromHudi(spark: SparkSession, path: String): DataFrame = {
    // a. 指定路径，加载数据，封装至DataFrame
    val didiDF: DataFrame = spark.read.format("hudi").load(path)
      // b. 选择字段
      didiDF.select(
        "order_id", "product_id", "type", "traffic_type",
        "pre_total_fee", "start_dest_distance", "departure_time"
      )
    }
  /**
    *  订单类型统计，字段：product_id
    */
  def reportProduct(dataframe: DataFrame): Unit = {
    // a. 按照产品线ID分组统计
    val reportDF: DataFrame = dataframe.groupBy("product_id").count()

    // b. 自定义UDF函数，转换名称
    val to_name = udf(
      // 1滴滴专车， 2滴滴企业专车， 3滴滴快车， 4滴滴企业快车
      (productId: Int) => {
        productId match {
          case 1 =>  "滴滴专车"
          case 2 =>  "滴滴企业专车"
          case 3 =>  "滴滴快车"
          case 4 =>  "滴滴企业快车"
        }
      }
    )
    // c. 转换名称，应用函数
    val resultDF: DataFrame = reportDF.select(
      to_name(col("product_id")).as("order_type"), //
      col("count").as("total") //
    )
    resultDF.printSchema()
    resultDF.show(10, truncate = false)
  }
  /**
    *  订单时效性统计，字段：type
    */
  def reportType(dataframe: DataFrame): Unit = {
    // a. 按照产品线ID分组统计
    val reportDF: DataFrame = dataframe.groupBy("type").count()

    // b. 自定义UDF函数，转换名称
    val to_name = udf(
      // 0实时，1预约
      (realtimeType: Int) => {
        realtimeType match {
          case 0 =>  "实时"
          case 1 =>  "预约"
        }
      }
    )
    // c. 转换名称，应用函数
    val resultDF: DataFrame = reportDF.select(
      to_name(col("type")).as("order_realtime"), //
      col("count").as("total") //
    )
    resultDF.printSchema()
    resultDF.show(10, truncate = false)
  }
  /**
    *  交通类型统计，字段：traffic_type
    */
  def reportTraffic(dataframe: DataFrame): Unit = {
    // a. 按照产品线ID分组统计
    val reportDF: DataFrame = dataframe.groupBy("traffic_type").count()

    // b. 自定义UDF函数，转换名称
    val to_name = udf(
      // 1企业时租，2企业接机套餐，3企业送机套餐，4拼车，5接机，6送机，302跨城拼车
      (trafficType: Int) => {
        trafficType match {
          case 0 =>  "普通散客"
          case 1 =>  "企业时租"
          case 2 =>  "企业接机套餐"
          case 3 =>  "企业送机套餐"
          case 4 =>  "拼车"
          case 5 =>  "接机"
          case 6 =>  "送机"
          case 302 =>  "跨城拼车"
          case _ => "未知"
        }
      }
    )
    // c. 转换名称，应用函数
    val resultDF: DataFrame = reportDF.select(
      to_name(col("traffic_type")).as("traffic_type"), //
      col("count").as("total") //
    )
    resultDF.printSchema()
    resultDF.show(10, truncate = false)
  }
  /**
    * 订单价格统计，将价格分阶段统计，字段：pre_total_fee
    */
  def reportPrice(dataframe: DataFrame): Unit = {
    val resultDF: DataFrame = dataframe
      .agg(
        // 价格：0 ~ 15
        sum(
          when(
            col("pre_total_fee").between(0, 15), 1
          ).otherwise(0)
        ).as("0~15"),
        // 价格：16 ~ 30
        sum(
          when(
            col("pre_total_fee").between(16, 30), 1
          ).otherwise(0)
        ).as("16~30"),
        // 价格：31 ~ 50
        sum(
          when(
            col("pre_total_fee").between(31, 50), 1
          ).otherwise(0)
        ).as("31~50"),
        // 价格：50 ~ 100
        sum(
          when(
            col("pre_total_fee").between(51, 100), 1
          ).otherwise(0)
        ).as("51~100"),
        // 价格：100+
        sum(
          when(
            col("pre_total_fee").gt(100), 1
          ).otherwise(0)
        ).as("100+")
      )
    resultDF.printSchema()
    resultDF.show(10, truncate = false)
  }
  /**
    * 订单距离统计，将价格分阶段统计，字段：start_dest_distance
    */
  def reportDistance(dataframe: DataFrame): Unit = {
    val resultDF: DataFrame = dataframe
      .agg(
        // 价格：0 ~ 15
        sum(
          when(
            col("start_dest_distance").between(0, 10000), 1
          ).otherwise(0)
        ).as("0~10km"),
        // 价格：16 ~ 30
        sum(
          when(
            col("start_dest_distance").between(10001, 20000), 1
          ).otherwise(0)
        ).as("10~20km"),
        // 价格：31 ~ 50
        sum(
          when(
            col("start_dest_distance").between(200001, 30000), 1
          ).otherwise(0)
        ).as("20~30km"),
        // 价格：50 ~ 100
        sum(
          when(
            col("start_dest_distance").between(30001, 5000), 1
          ).otherwise(0)
        ).as("30~50km"),
        // 价格：100+
        sum(
          when(
            col("start_dest_distance").gt(50000), 1
          ).otherwise(0)
        ).as("50+km")
      )
    resultDF.printSchema()
    resultDF.show(10, truncate = false)
  }
  /**
    *  订单星期分组统计，字段：departure_time
    */
  def reportWeek(dataframe: DataFrame): Unit = {

    // a. 自定义UDF函数，转换日期为星期
    val to_week: UserDefinedFunction = udf(
      // 0实时，1预约
      (dateStr: String) => {
        val format: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd")
        val calendar: Calendar = Calendar.getInstance()

        val date: Date = format.parse(dateStr)
        calendar.setTime(date)
        val dayWeek: String = calendar.get(Calendar.DAY_OF_WEEK) match {
          case 1 => "星期日"
          case 2 => "星期一"
          case 3 => "星期二"
          case 4 => "星期三"
          case 5 => "星期四"
          case 6 => "星期五"
          case 7 => "星期六"
        }
        // 返回星期
        dayWeek
      }
    )
    // b. 转换日期为星期，并分组和统计
    val resultDF: DataFrame = dataframe
      .select(
        to_week(col("departure_time")).as("week")
      )
      .groupBy(col("week")).count()
      .select(
        col("week"), col("count").as("total") //
      )
    resultDF.printSchema()
    resultDF.show(10, truncate = false)
  }

  def main(args: Array[String]): Unit = {
    // step1. 构建SparkSession实例对象（集成Hudi和HDFS）
    val spark: SparkSession = SparkUtils.CreateSparkSession(this.getClass, partitions = 8)
    import spark.implicits._

    // step2. 依据指定字段从Hudi表中加载数据
    val hudiDF: DataFrame = readFromHudi(spark, hudiTablePath)
//    hudiDF.printSchema()
//    hudiDF.show(10,truncate = false)

    //由于数据使用多次，建议缓存
    hudiDF.persist(StorageLevel.MEMORY_AND_DISK)

    // step3. 按照业务指标进行数据统计分析
    // 指标1：订单类型统计
//     reportProduct(hudiDF)

    // 指标2：订单时效统计
//     reportType(hudiDF)

    // 指标3：交通类型统计
//    reportTraffic(hudiDF)

    // 指标4：订单价格统计
//    reportPrice(hudiDF)

    // 指标5：订单距离统计
//    reportDistance(hudiDF)

    // 指标6：日期类型：星期，进行统计
    reportWeek(hudiDF)

    //当数据不在使用时，释放资源
    hudiDF.unpersist()

    // step4. 应用结束关闭资源
    spark.stop()


  }

}
