import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.apache.spark.sql.types.IntegerType
import java.sql.Date
import java.sql.Timestamp
import java.time.LocalDate
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable.HashMap

import org.apache.spark.sql.expressions.UserDefinedFunction

import scala.collection.JavaConverters._
object demo {
  var sc: SparkContext = null
  var sqlContext: SQLContext = null
  var spark: SparkSession = null
  def main(args:Array[String]): Unit ={
    println("Hello")
    spark = SparkSession.builder.appName("POS").config("spark.cassandra.connection.host", "5.9.58.93").master("local[3]").getOrCreate()
    spark.conf.set("spark.executor.memory", "2g")
    spark.conf.set("spark.driver.memory", "1g")
    spark.conf.set("spark.cassandra.connection.host", "5.9.58.93")
    val startDate: DateTime = DateTime.now()
    sc = spark.sparkContext
    sqlContext = new SQLContext(sc) //10.87.88.133



    var df = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").load("C:\\Users\\Pooja\\Desktop\\pokemon.csv")
    df.show()
    println("the number of record is " + df.count)
    df.printSchema()



    var GetTotalLogicMap:HashMap[String,String] = HashMap[String,String]()

    df.collect().foreach(
      x=>
        GetTotalLogicMap.put(x.getString(1),x.getString(4))
    )

    println(GetTotalLogicMap)

    var GetAvgDefecnce:HashMap[String,List[String]] = HashMap()



    df.collect().foreach(
      x=>
        GetAvgDefecnce.put(x.getString(0),List(x.getString(6),x.getString(7))
    ))
    println(GetAvgDefecnce)

    def CheckValiddata(GetTotalLogicMap:HashMap[String,String]): UserDefinedFunction = udf ((value:String) => {

      var result = 0

      var GetValueFromMap = GetTotalLogicMap.get(value).get.toInt

      if (GetValueFromMap > 200) {
        result = GetValueFromMap
      }

      else {
        result = 0
      }

      result
    }
    )

    def Avgr(GetAvgDefecnce:HashMap[String,List[String]]): UserDefinedFunction = udf ((value:String) => {

      var result:Float =0

      var list = GetAvgDefecnce.get(value).get
      var intlist= list.map(_.toInt)
      result = intlist.sum.toFloat/intlist.length.toFloat

      //result=((GetDefFromMap+GetAttFromMap)/2).toString()

      result
    }
    )


/*    def Avgr(GetDefecnce:HashMap[String,String],Getattack:HashMap[String,String]): UserDefinedFunction = udf ((value:String) => {

      var result =""

      var GetDefFromMap = GetDefecnce.get(value).get.toInt
      var GetAttFromMap = Getattack.get(value).get.toInt

      result=((GetDefFromMap+GetAttFromMap)/2).toString()

      result
    }
    )*/

    def R(): UserDefinedFunction = udf ((value:String) => {
      var result = ""
      if(value == null || value.trim.isEmpty || value.trim.equalsIgnoreCase("null"))
      {
        result = "EMPTY"
      }
      else
      {
        result = value.toString
      }
      result
    })


    def Rs(): UserDefinedFunction = udf((value:String,value1:String,value2:String)=>{
      var result: Float=0
      var H=value.toInt
      var D=value1.toInt
      var S=value2.toInt
      result=(H+D+S)/3
println("value= "+H)
      println("value1 "+D)
      println("value2 "+S)
println(result)

result

    })


    df = df.withColumn("Type 2",R()(df("Type 2")))

    df = df.withColumn("Result_Given", CheckValiddata(GetTotalLogicMap)(df("Name")))



    //df = df.withColumn("Average_Diff_Attack", Avgr(GetDefecnce,Getattack)(df("ID")))
    df = df.withColumn("Average_Diff_Attack", Avgr(GetAvgDefecnce)(df("ID")))
    df = df.withColumnRenamed("Sp. Def","SpDef")
    df = df.withColumnRenamed("Sp Atk","SpAtk")
df= df.withColumn("Result_Average", Rs()(df("Hp"),df("Defense"),df("SpAtk")))
    df.show(false)
    df.cache()
    df = df.withColumnRenamed("Type 2","Type2")
    df.registerTempTable("PokeMon")

    sqlContext.sql("select * From PokeMon").show()
//df.groupBy("Type 2").agg(count("Name") ).show()
sqlContext.sql("select(sum(Average_Diff_Attack))From PokeMon").show()
    sqlContext.sql("select count(Name), Type2 From PokeMon GROUP BY Type2").show()


  }
}