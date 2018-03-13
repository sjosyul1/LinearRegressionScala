
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

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

object linearRegression {
Logger.getLogger("org").setLevel(Level.OFF)
 val spark = SparkSession.builder
             .master("local[*]")
             .appName("IFT443SparkModules")
             .getOrCreate()                       //> Using Spark's default log4j profile: org/apache/spark/log4j-defaults.propert
                                                  //| ies
                                                  //| spark  : org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSessi
                                                  //| on@3fabf088
  import spark.implicits._
  spark.version                                   //> res0: String = 2.1.0
  
/* I have no header for the wickens file, and, the csv read returns a DataFrame, but untyped
So Iconverted it to a types DataFrame by imposing a Schema on it with the
actual types explicitly shown..
Then,from the DataFrame I can convert to a Dataset,
so... I cooked up a case class and mapped the DF to that
Note: I also had to tell the csv reader what the types of the fields were,
and made  sure the names match!!
I put this case class in the package object
"case class WickensMulti( id: String, X1 : Double, X2: Double, Y : Double)"
not in this object, wontwork if you do that
*/
  val id = StructField("id",  DataTypes.StringType)
                                                  //> id  : org.apache.spark.sql.types.StructField = StructField(id,StringType,tr
                                                  //| ue)
  val x1 =  StructField("X1",     DataTypes.DoubleType)
                                                  //> x1  : org.apache.spark.sql.types.StructField = StructField(X1,DoubleType,tr
                                                  //| ue)
  val x2 =  StructField("X2",     DataTypes.DoubleType)
                                                  //> x2  : org.apache.spark.sql.types.StructField = StructField(X2,DoubleType,tr
                                                  //| ue)
  val y =  StructField("Y",       DataTypes.DoubleType)
                                                  //> y  : org.apache.spark.sql.types.StructField = StructField(Y,DoubleType,true
                                                  //| )
  val fields = Array(id, x1, x2, y)               //> fields  : Array[org.apache.spark.sql.types.StructField] = Array(StructField
                                                  //| (id,StringType,true), StructField(X1,DoubleType,true), StructField(X2,Doubl
                                                  //| eType,true), StructField(Y,DoubleType,true))
  val schema = StructType(fields)                 //> schema  : org.apache.spark.sql.types.StructType = StructType(StructField(id
                                                  //| ,StringType,true), StructField(X1,DoubleType,true), StructField(X2,DoubleTy
                                                  //| pe,true), StructField(Y,DoubleType,true))
 //This file has four columns: id,X1, X2, Y whereX1, x2 are the predictors
 //Y is the vector to be predicted. The result should show negative  regression coefficients
// I had to give the 'read' function a schema to work with.
val wickensDataFile =  "c://aaprograms/datasets/wickensMultivar.csv"
                                                  //> wickensDataFile  : String = c://aaprograms/datasets/wickensMultivar.csv

val dfWickens = spark.read.schema(schema).option("header", false).csv(wickensDataFile)
                                                  //> dfWickens  : org.apache.spark.sql.DataFrame = [id: string, X1: double ... 2
                                                  //|  more fields]
val ds = dfWickens.as[WickensMulti]               //> ds  : org.apache.spark.sql.Dataset[apps.WickensMulti] = [id: string, X1: do
                                                  //| uble ... 2 more fields]
val wickensObjectArray= ds.collect                //> wickensObjectArray  : Array[apps.WickensMulti] = Array(WickensMulti(d1,2.0,
                                                  //| 3.0,15.0), WickensMulti(d2,2.0,5.0,16.0), WickensMulti(d3,4.0,4.0,15.0), Wi
                                                  //| ckensMulti(d4,4.0,7.0,10.0), WickensMulti(d5,5.0,5.0,13.0), WickensMulti(d6
                                                  //| ,5.0,8.0,9.0), WickensMulti(d7,5.0,9.0,8.0), WickensMulti(d8,6.0,8.0,7.0), 
                                                  //| WickensMulti(d9,7.0,7.0,8.0), WickensMulti(d10,7.0,10.0,5.0), WickensMulti(
                                                  //| d11,8.0,11.0,4.0))
// Getting the vectors, X1,X2, Y from the WIckensMulti objects (
//Note, maybe it is possible to get these from the dataframe stage
 val x1Vec = (for{ d <- ds} yield d.X1).collect().toVector
                                                  //> x1Vec  : Vector[Double] = Vector(2.0, 2.0, 4.0, 4.0, 5.0, 5.0, 5.0, 6.0, 7.
                                                  //| 0, 7.0, 8.0)
 val x2Vec = (for{ d <- ds} yield d.X2).collect().toVector
                                                  //> x2Vec  : Vector[Double] = Vector(3.0, 5.0, 4.0, 7.0, 5.0, 8.0, 9.0, 8.0, 7.
                                                  //| 0, 10.0, 11.0)
 val yVec = (for{ d <- ds}  yield d.Y ).collect().toVector
                                                  //> yVec  : Vector[Double] = Vector(15.0, 16.0, 15.0, 10.0, 13.0, 9.0, 8.0, 7.0
                                                  //| , 8.0, 5.0, 4.0)
 
 x1Vec correlation x2Vec                          //> res1: Double = 0.8111071056538127
val m1= x1Vec.mean                                //> m1  : Double = 5.0
val m2 = x2Vec.mean                               //> m2  : Double = 7.0
val x11 = x1Vec - m1                              //> x11  : scala.collection.immutable.Vector[Double] = Vector(-3.0, -3.0, -1.0,
                                                  //|  -1.0, 0.0, 0.0, 0.0, 1.0, 2.0, 2.0, 3.0)
val x22 = x2Vec - m2                              //> x22  : scala.collection.immutable.Vector[Double] = Vector(-4.0, -2.0, -3.0,
                                                  //|  0.0, -2.0, 1.0, 2.0, 1.0, 0.0, 3.0, 4.0)
x11.norm                                          //> res2: Double = 6.164414002968976
x22.norm                                          //> res3: Double = 8.0
x11 correlation x22                               //> res4: Double = 0.8111071056538127
//val x1Standard = x1Vec.standardize
//val x2Standard = x2Vec.standardize
/*  *********************************TEST vectors below-
The plan is to test whether the MLlib LineraRegressionSGD
is giveing correct answers ( so far  NOT!!)
Below are  parameters I know, the parameters shown are correct,
for vx1, vy1 as calculated by my tiny stats library ( and text book agreement)
so I will go from there
 some test vectors from our first home work, from Wickens, BIvariate regression
where vx1 is used to predict vy1, as best as possible.
The prediction is called the regresion equation and consists of an intercept and
slope, 'a' and 'b1'
 val vx1= Vector(1.0,1,3,3,4,  4,  5,6,  6,7)
 val vy1= Vector(4.0,7,9,12,11,12,17,13,18,17)
*/
val vx1= Vector(1.0,1,3,3,4,4,5,6,6,7)            //> vx1  : scala.collection.immutable.Vector[Double] = Vector(1.0, 1.0, 3.0, 3.
                                                  //| 0, 4.0, 4.0, 5.0, 6.0, 6.0, 7.0)
val vy1= Vector(4.0,7,9,12,11,12,17,13,18,17)     //> vy1  : scala.collection.immutable.Vector[Double] = Vector(4.0, 7.0, 9.0, 12
                                                  //| .0, 11.0, 12.0, 17.0, 13.0, 18.0, 17.0)
 // will try this standardized 'x' vector next
// val vx1std = vx1.standardize.foreach(println)
val v1max= vx1.max                                //> v1max  : Double = 7.0
vx1.mean                                          //> res5: Double = 4.0
vy1.mean                                          //> res6: Double = 12.0
val unitizeXfeature= vx1.norm                     //> unitizeXfeature  : Double = 14.071247279470288
val vx1Unitized = vx1/unitizeXfeature             //> vx1Unitized  : scala.collection.immutable.Vector[Double] = Vector(0.0710669
                                                  //| 0545187015, 0.07106690545187015, 0.21320071635561044, 0.21320071635561044, 
                                                  //| 0.2842676218074806, 0.2842676218074806, 0.3553345272593507, 0.4264014327112
                                                  //| 209, 0.4264014327112209, 0.49746833816309105)
vx1 correlation vy1 // this is correct            //> res7: Double = 0.9039935293326326
// this is the raw( no scaling)  regression and is correct, intercept and slope
val (intercept, slope) = regressYX(vy1, vx1)      //> intercept  : Double = 4.0
                                                  //| slope  : Double = 2.0
//here is divided the X vector by the norm of that vector hoping that
//would match the MLlib LinearRegression SGD output,,, NOPE!!
 val (interceptU, slopeU) = regressYX(vy1, vx1/unitizeXfeature)
                                                  //> interceptU  : Double = 4.000000000000001
                                                  //| slopeU  : Double = 28.142494558940577
   //** just checking out the Mean Square Error using a function I just cooked up
   def testMSEFun(intercept: Double, slope: Double, vy: Vector[Double], vx: Vector[Double])={
    val pairs = vy zip vx
    val sumErrorSquares = pairs.map{case(y, x) => math.pow((y - ( intercept + slope * x)),2)}.sum
    val mse = sumErrorSquares/vy.size
    mse
   }                                              //> testMSEFun: (intercept: Double, slope: Double, vy: Vector[Double], vx: Vect
                                                  //| or[Double])Double
  testMSEFun (intercept, slope, vy1, vx1)         //> res8: Double = 3.4
  testMSEFun( interceptU, slopeU, vy1, vx1Unitized)
                                                  //> res9: Double = 3.4

/* ******* now use the linear regression package to **TRY** to verify this********
I will scale the X value with their norm*/
val rawPairsRDD =
   spark.sparkContext.textFile("c://aaprograms/datasets/wickensBivariate.txt")
                                                  //> rawPairsRDD  : org.apache.spark.rdd.RDD[String] = c://aaprograms/datasets/w
                                                  //| ickensBivariate.txt MapPartitionsRDD[13] at textFile at apps.linearRegressi
                                                  //| on.scala:114
rawPairsRDD.collect()                             //> res10: Array[String] = Array(1 4, 1 7, 3 9, 3 12, 4 11, 4 12, 5 17, 6 13, 6
                                                  //|  18, 7 17)
// trying to get the linearSGD to converge, so trying this preprocessig of the
//feature data vector, There is a built in Scalar utility that could be used
//but I am doing this directly
// this is really ad hoc since I already knew the vector I was going to use for X, 'vx1'
// Here I scale the X feature vector with its norm( length), so that the vector is unit length
//NOte that in the file, the right hand column is the Y vec while th epredictor is the left col.
// the result is not a terible MSE, but is twice a high as teh analytic solution above.
val parsedData  =  rawPairsRDD.map{line =>
              val data = line.split(" ")
             LabeledPoint(data(1).toDouble,
             Vectors.dense((data(0).toDouble)/unitizeXfeature)) // Vectors.dense(data(0).toDouble))
             }                                    //> parsedData  : org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.La
                                                  //| beledPoint] = MapPartitionsRDD[14] at map at apps.linearRegression.scala:12
                                                  //| 3
 parsedData.collect.foreach(println)              //> (4.0,[0.07106690545187015])
                                                  //| (7.0,[0.07106690545187015])
                                                  //| (9.0,[0.21320071635561044])
                                                  //| (12.0,[0.21320071635561044])
                                                  //| (11.0,[0.2842676218074806])
                                                  //| (12.0,[0.2842676218074806])
                                                  //| (17.0,[0.3553345272593507])
                                                  //| (13.0,[0.4264014327112209])
                                                  //| (18.0,[0.4264014327112209])
                                                  //| (17.0,[0.49746833816309105])
// Build the model
var regression = new LinearRegressionWithSGD().setIntercept(true)
                                                  //> regression  : org.apache.spark.mllib.regression.LinearRegressionWithSGD = o
                                                  //| rg.apache.spark.mllib.regression.LinearRegressionWithSGD@1f4d38f9
 // have tried many values of stepsize ??
regression.optimizer.setStepSize(2.0)             //> res11: org.apache.spark.mllib.optimization.GradientDescent = org.apache.spa
                                                  //| rk.mllib.optimization.GradientDescent@18463720

regression.optimizer.setNumIterations(600)        //> res12: org.apache.spark.mllib.optimization.GradientDescent = org.apache.spa
                                                  //| rk.mllib.optimization.GradientDescent@18463720
val model = regression.run(parsedData)            //> 17/03/27 16:44:09 WARN BLAS: Failed to load implementation from: com.github
                                                  //| .fommil.netlib.NativeSystemBLAS
                                                  //| 17/03/27 16:44:09 WARN BLAS: Failed to load implementation from: com.github
                                                  //| .fommil.netlib.NativeRefBLAS
                                                  //| model  : org.apache.spark.mllib.regression.LinearRegressionModel = org.apac
                                                  //| he.spark.mllib.regression.LinearRegressionModel: intercept = 6.508882762012
                                                  //| 921, numFeatures = 1
model.weights                                     //> res13: org.apache.spark.mllib.linalg.Vector = [19.47322528598724]
model.intercept                                   //> res14: Double = 6.508882762012921

val valuesAndPreds = parsedData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}                                                 //> valuesAndPreds  : org.apache.spark.rdd.RDD[(Double, Double)] = MapPartition
                                                  //| sRDD[509] at map at apps.linearRegression.scala:139
valuesAndPreds.collect()                          //> res15: Array[(Double, Double)] = Array((4.0,7.892784622255143), (7.0,7.8927
                                                  //| 84622255143), (9.0,10.660588342739587), (12.0,10.660588342739587), (11.0,12
                                                  //| .04449020298181), (12.0,12.04449020298181), (17.0,13.428392063224031), (13.
                                                  //| 0,14.812293923466253), (18.0,14.812293923466253), (17.0,16.196195783708475)
                                                  //| )
val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2) }.mean()
                                                  //> MSE  : Double = 4.844371665850742
println("training Mean Squared Error = " + MSE)   //> training Mean Squared Error = 4.844371665850742
/* ******************************  trying with standardized 'x' vector *** */
val rawPairsRDDStd =
   spark.sparkContext.textFile("c://aaprograms/datasets/wickensBivariatestandardized.txt")
                                                  //> rawPairsRDDStd  : org.apache.spark.rdd.RDD[String] = c://aaprograms/dataset
                                                  //| s/wickensBivariatestandardized.txt MapPartitionsRDD[513] at textFile at app
                                                  //| s.linearRegression.scala:148
rawPairsRDDStd.collect()                          //> res16: Array[String] = Array(1 -1.759765380256239, 1 1.0998533626601497, 3 
                                                  //| 0.6599120175960899, 3 0.0, 4 0.21997067253202995, 4 0.0, 5 1.09985336266014
                                                  //| , 6 0.219970672532029, 6 1.31982403519217, 7 1.0998533626601497)


val parsedDataStd  =  rawPairsRDDStd.map{line =>
                  val data = line.split(" ")
             LabeledPoint(data(1).toDouble, Vectors.dense(data(0).toDouble))
             }                                    //> parsedDataStd  : org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression
                                                  //| .LabeledPoint] = MapPartitionsRDD[514] at map at apps.linearRegression.scal
                                                  //| a:152
var regressionx = new LinearRegressionWithSGD().setIntercept(true)
                                                  //> regressionx  : org.apache.spark.mllib.regression.LinearRegressionWithSGD = 
                                                  //| org.apache.spark.mllib.regression.LinearRegressionWithSGD@5c190662
regressionx.optimizer.setStepSize(0.0000000001)   //> res17: org.apache.spark.mllib.optimization.GradientDescent = org.apache.spa
                                                  //| rk.mllib.optimization.GradientDescent@5d6de24e
regressionx.optimizer.setNumIterations(1000)      //> res18: org.apache.spark.mllib.optimization.GradientDescent = org.apache.spa
                                                  //| rk.mllib.optimization.GradientDescent@5d6de24e
val modelx = regression.run(parsedData)           //> modelx  : org.apache.spark.mllib.regression.LinearRegressionModel = org.apa
                                                  //| che.spark.mllib.regression.LinearRegressionModel: intercept = 6.50888276201
                                                  //| 2921, numFeatures = 1
                                       
modelx.weights                                    //> res19: org.apache.spark.mllib.linalg.Vector = [19.47322528598724]
modelx.intercept                                  //> res20: Double = 6.508882762012921





}/* end linear Regression  */

/*
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

// Load and parse the data
val data = sc.textFile("data/mllib/ridge-data/lpsa.data")
val parsedData = data.map { line =>
  val parts = line.split(',')
  LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
}.cache()

// Building the model
val numIterations = 100
val stepSize = 0.00000001
val model = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize)

// Evaluate model on training examples and compute training error
val valuesAndPreds = parsedData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2) }.mean()
println("training Mean Squared Error = " + MSE)

// Save and load model
model.save(sc, "target/tmp/scalaLinearRegressionWithSGDModel")
val sameModel = LinearRegressionModel.load(sc, "target/tmp/scalaLinearRegressionWithSGDModel")


import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors

// Load and parse the data
val data = sc.textFile("data/mllib/ridge-data/lpsa.data")
val parsedData = data.map { line =>
  val parts = line.split(',')
  LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
}.cache()

// Build the model
var regression = new LinearRegressionWithSGD().setIntercept(true)
regression.optimizer.setStepSize(0.1)
val model = regression.run(parsedData)

// Evaluate model on training examples and compute training error
val valuesAndPreds = parsedData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
println("training Mean Squared Error = " + MSE)
For another example on a more realistic dataset, see

https://github.com/selvinsource/spark-pmml-exporter-validator/blob/master/src/main/resources/datasets/winequalityred_linearregression.md

https://github.com/selvinsource/spark-pmml-exporter-validator/blob/master/src/main/resources/spark_shell_exporter/linearregression_winequalityred.scala
To rephrase your question, you want to find the intercept I and coefficients C_1 and C_2 that solve the equation: Y = I + C_1 * x_1 + C_2 * x_2 (where x_1 and x_2 are unscaled).
************************* Converting back from scaled data ***************
Let i be the intercept that mllib returns. Likewise let c_1 and c_2 be the coefficients (or weights) that mllib returns.

Let m_1 be the unscaled mean of x_1 and m_2 be the unscaled mean of x_2.

Let s_1 be the unscaled standard deviation of x_1 and s_2 be the unscaled standard deviation of x_2.

Then C_1 = (c_1 / s_1), C_2 = (c_2 / s_2), and

I = i - c_1 * m_1 / s_1 - c_2 * m_2 / s_2

This can easily be extended to 3 input variables:

C_3 = (c_3 / s_3) and I = i - c_1 * m_1 / s_1 - c_2 * m_2 / s_2 - c_3 * m_3 / s_3

*/
