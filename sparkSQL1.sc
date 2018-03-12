/**IFT443SparkModules  in eclipsews2017,,  apps> sparkSQL.sc
Invoking the SparkSQL 2.1 module, ( has the latest API Datasets/DataFrames)
Ways to interact with SSQL: { DataFrames(DF), Datasets(DS), and SQL itself
Given a sqlContext, can create DatFrames -- >from { existing RDD< data sources, Hive table
DataFrames provide a DSL for structured data manipulation
NOTE: The case classes MUST be outside of this sparkSQL.sc object *** that is what
prevented my earlier code from working
rr. 2017-03-13/15/20
*/
package apps
import java.util.Date
import org.apache.log4j.{Level, Logger}
import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf

object sparkSQL1 {
Logger.getLogger("org").setLevel(Level.OFF)
 val spark = SparkSession.builder
             .master("local[*]")
             .appName("IFT443SparkModules")
             .getOrCreate()
  import spark.implicits._
  spark.version
  
  val dsp = Seq(Person("JO", 45), Person("kin", 58)).toDS()
  //oldstyle of creatinga DataFrame, using sparkContext and also
  // involving the  Row class to hold the column names, not types
  val fna1 = "C://aaprograms/datasets/people.txt"
  val lines =  spark.sparkContext.textFile(fna1)
    lines.collect()
   val columns = lines.map{ line => line.split(",") }
   val cleaned=columns.collect().filterNot(_.forall(_.isEmpty))
   val cleanRDD = spark.sparkContext.makeRDD(cleaned)
   //val cleanColumns = lines.map{line => line.split(",")} .filterNot(_.forall(_.isEmpty))
                     
  // val data = cleaned.map{ col => Row(col(0), col(1).trim.toInt) }
  //data.collect().foreach(println)
  
  //data.map{row => (row(0), row(1)) }.take(0)
   val row = Row(1,2,"DF",5.6)
val fn2 = "C://aaprograms/datasets/people.txt"
val peopleRDD = spark.sparkContext.textFile(fn2)
                .map{ line => line.split(",")}
                .map{ line => Person( line(0), line(1).trim.toLong) }
                
  
  
  
  
  /* NO OP THE REST FOR NOW ***********************
  
//   **********************************VERY NEW *********************
//See the case class Purchase in the package object
val x = spark.sparkContext.makeRDD(Array(
  Purchase(123, 234, "2007-12-12", "20:50", "UTC", 500.99),
  Purchase(123, 247, "2007-12-12", "15:30", "PST", 300.22),
  Purchase(189, 254, "2007-12-13", "00:50", "EST", 122.19),
  Purchase(187, 299, "2007-12-12", "07:30", "UTC", 524.37)
))
//WHOA**** transform straight from a case typed RDDto DS!!**********!!!
 val xds = x.toDS()

val dsp = spark.createDataFrame(x).as[Purchase]
dsp.registerTempTable("dspTable")
def makeDT(date: String, time : String, tz: String) = s"$date $time $tz"
spark.udf.register("makeDt", makeDT(_:String,_ :String, _ :String))
 spark.sql("Select makeDt(date,time,tz)  from dspTable").take(3)
 // ***NO WOrk!!  dsp.select("$customer_id").take(3)
  val makeDt = udf(makeDT(_:String,_:String,_:String))
// now this works
dsp.select($"customer_id", makeDt($"date", $"time", $"tz"), $"amount").take(3)
 
 
 
  
//   *********************************************NEW  03-20**************
  val id =       StructField("id",        DataTypes.IntegerType)
  val width =    StructField("width",     DataTypes.DoubleType)
  val height =   StructField("height",    DataTypes.DoubleType)
  val depth =    StructField("depth",     DataTypes.DoubleType)
  val material = StructField("material",      DataTypes.StringType)
  val color =    StructField("color",     DataTypes.StringType)
  val fields= Array(id,width, height, depth, material, color)
  val schema = StructType(fields)
 
 val fna = "C://aaprograms/datasets/bodies.csv"
 //I told spark the types per column, they are shown as int, string, .....
 val dfa = spark.read
           .schema(schema)
           .option("header", true)
           .csv(fna)
 //ok, plan B, I didn't tell spark the types and it inferred ALL to string!!
 val dfb = spark.read.option("header", true).csv(fna)
// Now construct 'Body' case class ( place it outside of this object, maybe in the package obj ?
//  Whoa, dfb was the 'all String' typing option, so it wouldn't upcast from string to int as the
//case class required...
//val dsa = dfb.as[Body]

val dsa = dfa.as[Body]
val colors = dsa.map(_.color).collect().foreach(println)
 
//TODO   TBD  type-safe aggregation and udfs
import org.apache.spark.sql.expressions.scalalang.typed.{
  count => typedCount,
  sum   => typedSum
 }
dsa.groupByKey( body => body.color)
val volumUdf = udf {
(width:Double, height: Double, depth : Double) => width * height * depth
}




 
//  **************************** OLD   03-16 **********************************
  //reading in a JSON file automatically creates a DataFrame ( I have a directory of sample data files)
  val fn = "c://aaprograms/datasets/person.json"
 //DataFrame programmatic APIs
  val peopleDF = spark.read.json(fn)

  peopleDF.show()
 peopleDF.printSchema()
 peopleDF.select("name").show()
peopleDF.select(peopleDF("name"), peopleDF("age")).show()
peopleDF.filter(peopleDF("age") > 50).show()
peopleDF.groupBy("age").count().show()


//Switch to SQL DSL , ( this is versus the programmatic API)
peopleDF.createOrReplaceTempView("people")
//   ****************** NOW run SQL queries programmatically **************
 val sqlDF = spark.sql("Select * From people where age > 50 ").show()

  

//   *********************Creating Datsets  Datasets section **********************

val fnjson = "c://aaprograms/datasets/people.json"
import spark.implicits._
val dfpeople = spark.read.json(fnjson)
dfpeople.show()
val peopleFromJSON = spark.sqlContext.read.json(fnjson).as[Person]


//Primitives can be directly convereted to Datasets, by built-in Spark encoders
//that seems to work but case classes don't*** They cant be in the same object **!!
//val peopleDS = dfpeople.as[Person]
val ds = Seq( "jpl", "cbo", "sdf").toDS()
 ds.map{ _.size }.collect()
 val ds1= List(1,2,3).toDS()

 //val classDS:Dataset[Bongo] = Seq(Bongo("jo",34), Bongo("mo", 55)).toDS()

 //??** NO WORK**Sequences of 'case classes' can be converted to Datasets directly as they have special 'encoders' allowing this

 //val personsDS = Seq(Person("jo", 67), Person("kim", 42), Person("gene",55) ).toDS()
 //val  personDS = Seq(Person("AAA", 34)).toDS()
// personsDS.show()
 //val dsp2 = persons.toDS()
  //dsp2.map{ p => p.name}.take(3).foreach(println)
 val dsp3 = Seq(Person("BongoBilly", 45), Person("AnnieOakley", 56),Person("Kim", 42), Person("Gene",55))
//val dsx =spark.sqlContext.createDataset(dsp3)
 
 
 //  *****************************  Converting RDD to Datasets ********************
 //create an RDD of Person objects and register it as a table
 val fnPeople = "c://aaprograms/datasets/people.txt"
 val peopleRDD =
   spark.sparkContext.textFile(fnPeople)
 val peopleRDDCased = peopleRDD.map{line => line.split(",")}.map{p => Person(p(0), p(1).trim.toLong)}
  //val peopleRDDDF = peopleRDDCased.toDF()
                                                  
  val name = StructField("name",  DataTypes.StringType)
  val age =  StructField("age",   DataTypes.LongType)
  val fields = Array(name,age)
  val schema = StructType(fields)
  
//val peopleRDDDF= peopleRDDCased.createDataFrame(peopleRDDCased, schema)

val fn1 =  "c://aaprograms/datasets/person1.csv"
val df = spark.read.schema(schema).option("header", true).csv(fn1)


  



//val caseClassDS = persons.toDS()
//var rdd = spark.sparkContext.makeRDD(persons)
//persons.toDS()
//peopleDF.as[Person]

//var ds2 = spark.createDateset(persons)


END NO  --  OP MATERIAL   */


spark.stop()
}





/*EXTRA STUFF
 Creating a Dataset from a Text file
Suppose instead you have data in a text file, in tab-separated (.tsv) format:

    Alice<tab>Math<tab>18
    Bob<tab>CS<tab>19
    Carl<tab>Math<tab>21


To create a Dataset from this text file:
// Read the lines of the file into a Dataset[String].
> val studentsFromText = sqlContext.read.text("students.tsv").as[String]
(result) studentsFromText: org.apache.spark.sql.Dataset[String] = [value: string]




// Functional programming to parse the lines into a Dataset[Student].
val students = studentsFromText.
  map(line => {
    val cols = line.split("\t") // parse each line
    Student(cols(0), cols(1), cols(2).toInt)
  })
(result) students: org.apache.spark.sql.Dataset[Student] = [name: string, dept: string, age: int]

// Show the contents of the Dataset.
> students.show()
| name|dept|age|
\
|Alice|Math| 18|
|  Bob|  CS| 19|
| Carl|Math| 21|
*/