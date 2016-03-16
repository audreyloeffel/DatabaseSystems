package main.scala
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object Project1 {
  
  
  
  def main(args: Array[String]){
      
   if(!(args.length == 2 || args.length == 3)){
     println("Bad arguments");
     exit(1)     
   }
     
    val customer = "customer.tbl"
    val orders = "orders.tbl"


    val conf = new SparkConf().setAppName("DS")
    val sCxt = new SparkContext(conf)
    
    // Read the data
    val customerCSV = sCxt.textFile(args(0)+"/"+customer)
    val customerData = customerCSV.map(line => line.split("\\|")) 
    
    val ordersCSV = sCxt.textFile(args(0)+"/"+orders)
    val ordersData = ordersCSV.map(line => line.split("\\|")) 
  
    // Query
   
    val colCust = customerData.map(t => (t.head.toInt, 0))
    val colOrd = ordersData.filter( t => !(t(8).matches(".*special.*requests.*"))).map(t => (t(1).toInt, 1)).reduceByKey(_+_)
    
   // form: (CUSTKEY, (Some(ORDEKEY))
    val inner = colCust.leftOuterJoin(colOrd)
       .mapValues{
          case (_, Some(v)) =>  v
          case (_, None) =>  0
          }

   // Now we have tuples in the form (Ckey, nbOrder) sorted by nbOrber
       
   val result = inner.map{case (_,v) => (v, 1)}
   .reduceByKey(_+_)
       
    //val result = inner.map{case (_, orders) => (orders, 1)}.reduceByKey(_+_)
    
       
   // Save
   
    val toSave = result.map(t => Array(t._1.toString, t._2.toString).mkString("|"))
    
    args.length match {
      case 2 => toSave.saveAsTextFile(args(1)+"/out")
      case 3 => toSave.saveAsTextFile(args(1)+"/out_"+args(2))
    }

  }
  
  
}