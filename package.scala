/**package object apps -- in IFT443SparkModules -- copied over the
 * tiny stats implicit classes and methods from IFT443HomeWork
 * for initial exploratory look  at  linear regression
 * will use this library to do some initial exploration before invoking
 * the Spark MLLib functions
 *  2017-03-25 rob r.
 * The case classes are to do some example reads and conversions to Datasets
 * WickensMulti is used in the linearRegression object to convert a csv read in DataFrame
 * to a DS
 */

package object apps {  
  case class WickensMulti( id: String, X1 : Double, X2: Double, Y : Double) 

 case class Person(name: String, age: Long)
 case class Bongo(name: String, age : Long)
 case class Body(id : Int, width: Double, height: Double,
                   depth: Double, material: String, color: String)
  
  case class Purchase(customer_id: Int, purchase_id: Int,
       date: String, time: String, tz: String, amount:Double)
  case class Users(uid : Int, name : String, email : String, age: Int, latitude: Double,
      longitude: Double, subscribed: Boolean)
  case class Events(uid : Int, action : Int)
       
       // We want a Dataset of type "Student".
case class Student(name: String, dept: String, age:Int)

/*  *****************  rob's tiny stats package implicit classes and methods 
*  ( copied from  IFT443Homework  package object
*/
import scala.math._
type Vec = Vector[Double]
implicit class VectorEnrich(v: Vec){
//  TBD def zipOp(vec1 : Vec, vec2 : Vec, operation: String)= {/* a utility function */}
def +    ( other : Vec)        = (v zip other)   map { case (x1, x2) => x1 + x2 }
def -    ( other : Vec)        = (v zip other)   map { case (x1, x2) => x1 - x2 }
 // applying a scalar to the right hand side of a Vector   
def *    (scalar : Double)     =  v map { x => x * scalar} 
def /    (scalar : Double)     =  v map {x => x * 1.0/scalar}
def -    (scalar : Double)     =  v map { x => x - scalar}
def +    (scalar : Double)     =  v map { x => x + scalar}
def dot  ( other : Vec)        = (v zip other). map { case (x1, x2) => x1 * x2 }.sum 
def norm                       =   sqrt( (v.map(x => x * x)).sum )
def mean                       =     v.sum/v.size
def stdDev                     =  {  val mp = v.mean  
                                    val temp = v.map (x => (x - mp) * (x -mp))
                                    sqrt(temp.sum/(v.size -1))
                                 }
def standardize                = {   val m = v.mean
                                     val s = v.stdDev 
                                    v map( x => (x - m)/s)
                                  }
def center                     = { val m = v.mean
                                   v.map( x => x - m)
                                 }
def correlation (other: Vec) =   {val a1 =  (v.center) dot (other.center) 
                                  val b1 =  v.center.norm 
                                  val c1 =  other.center.norm
                                      a1/(b1 * c1)
                                 }
                                

/** z regressZXY (xVec, yVec) uses a linear combination of xVec and yVec to predict z
 * (intercept, slopeb1, slopeb2) is returned where  intercept  and the b1 and b2 coefficients of
 * the xVec and yVec vectors;   z = intercept + slopeb1 * xVec + slopeb2 * yVec
 *  xVec, yVec are the predictors and z is the vector to be predicted
 */
 
def regressZXY ( xVec: Vec, yVec : Vec) = {
  //note that because of implicit structure, the 'z' is the vectoron the left handside
  // of the expression
  //import breeze.linalg._  / for some reason this works, but for how long, I dont know
  //val t = DenseVector(1.0, 2.0)
  //val mat = DenseMatrix ( (1.0, 2.0), (6.0, -2.0))
 val z = v.center
 val x = xVec.center
 val y = yVec.center
 val a = x dot x
 val b = x dot y
 val c = b
 val d = y dot y
 val e = x dot z
 val f = y dot z
 val delta = a* d - c * c
 val slopeb1 = (d * e - b * f)/delta
 val slopeb2 = (a * f - c * e)/delta
 val intercept = v.mean - ( slopeb1 * xVec.mean + slopeb2 * yVec.mean)
 
 (intercept, slopeb1, slopeb2)
}

/* regressZXYW  is experimental and uses Breeze matrix functions,
but those seem incompatible with my Vectors!!
 it does seem to work but returns a DenseVector ....
 * 
 */
def regressZXYW( Z: Vec, X1: Vec, X2 : Vec, X3 : Vec)={ 

def solver3(Z: Vec, X:Vec, Y: Vec, W: Vec)={
     val z = Z.center 
     val x1 = X.center ;val x2 = Y.center; val x3 = W.center
    val a = x1 dot x1;  val b = x1 dot x2 ; val c = x1 dot x3
    val d = x2 dot x1 ; val e = x2 dot x2 ; val f = x2 dot x3
    val g = x3 dot x1 ; val h = x3 dot x2 ; val i = x3 dot x3
    val k = x1 dot z  ; val l = x2 dot z  ; val m = x3 dot z
 import breeze.linalg._
 val matrix = DenseMatrix( (a, b, c), (d,e,f), (g, h,i))
 val invMatrix = inv(matrix) 
 val rightHandSide = DenseVector(k, l, m)
  val result= invMatrix * rightHandSide
  result
   }
     solver3(Z, X1, X2, X3)
 }
}/* end VectorEnrich */
implicit class DoubleEnrich(d : Double){
// applying a scalar to the left hand side of a Vector
  def * (v : Vec)              = v map { x => x * d }
  def - (v : Vec)              = v map { x => d - x}
  // use with correlation coefficient to get degrees between  (v correlation w).angle
  def angle                    = acos(d).toDegrees
  }/* end DoubleEnrich */
/***************************UTILITY /REGRESSION Functions *******************  */
 /*  regressYX(yVec, xVec) uses xVec to predict yVec values. the intercept and slope are returned
 *  yhat = intercept + slopeb1 * xVec
 *  xVec is the predictor and yVec is the vector to be predicted
 */                                  
def  regressYX (yVec: Vector[Double], xVec: Vector[Double] ) =  {    
  val y = yVec.center
  val x = xVec.center
  val slopeb1 = (x dot y) / ( x dot x)
  val intercept = yVec.mean - slopeb1 * xVec.mean
  (intercept, slopeb1) 
}

} /* end package object apps*/
