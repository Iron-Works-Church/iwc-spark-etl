
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import java.io.File
import java.util.*

object Application {
  @JvmStatic
  fun main(args: Array<String>) {
    val sparkConf = SparkConf()
    val sc = SparkContext("local", "spark-mysql-test", sparkConf)
    val sqlContext = SQLContext(sc)

    val configProperties: Properties = File("config/config.properties").inputStream().use {
      Properties().apply {
        load(it)
      }
    }

// here you can run sql query
    val membershipdataTable = "IWC_MEMBERSHIPDATA"
// or use an existed table directly
// String sql = "table1";
    val mySqlUrl = "jdbc:mysql://${configProperties.getProperty("mysql.host")}:${configProperties.getProperty("mysql.port")}/${configProperties.getProperty("mysql.database")}?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true"
    val properties = Properties().apply {
      setProperty("user", configProperties.getProperty("mysql.user"))
      setProperty("password", configProperties.getProperty("mysql.password"))
    }

    csvToStage(sqlContext, mySqlUrl, membershipdataTable, properties, configProperties.getProperty("csvpath"))
    showStageTable(sqlContext, mySqlUrl, membershipdataTable, properties)
  }

  private fun showStageTable(sqlContext: SQLContext, mySqlUrl: String, membershipdataTable: String, properties: Properties) {
    val dataFrame = sqlContext
            .read()
            .jdbc(mySqlUrl, membershipdataTable, properties)
    dataFrame.show()
  }

  private fun csvToStage(sqlContext: SQLContext, mySqlUrl: String, membershipdataTable: String, properties: Properties, csvPath: String) {
    val df = sqlContext.read()
            .format("com.databricks.spark.csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .load(csvPath)

    df.write().mode(SaveMode.Overwrite).jdbc(mySqlUrl, membershipdataTable, properties)
  }

}