package master.hudi.didi
import org.apache.spark.sql.SparkSession
//import org.apache.zookeeper.txn.CreateSessionTxn

/**
  * SparkSQL操作数据（加载读取和保存写入）时工具类，比如获取SparkSession实例对象等
  */
object SparkUtils {
  /**
    * 构建SparkSession实例对象，默认情况下本地模式运行
    */
  def CreateSparkSession(clazz: Class[_],master: String = "local[4]", partitions: Int = 4): SparkSession = {
    SparkSession.builder()
      .appName(clazz.getSimpleName.stripSuffix("$"))
      .master(master)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", partitions)
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    val spark = CreateSparkSession(this.getClass)
    println(spark)
    Thread.sleep(10000000)
    spark.stop()
  }
}
