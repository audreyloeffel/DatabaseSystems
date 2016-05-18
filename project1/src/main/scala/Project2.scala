package main.scala
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object Project2 {
  
  
  
  def main(args: Array[String]){
      
   if(!(args.length == 3)){
     println("Bad arguments");
     exit(1)     
   }
     
   val customer = "customer.tbl"
   val orders = "orders.tbl"
   val conf = new SparkConf().setAppName("Project2")
   val sCxt = new SparkContext(conf)
   
   // Read the data

  
   val ordersCSV = sCxt.textFile(args(0)+"/"+orders)
   val ordersData = ordersCSV.map(line => line.split("\\|")) 
   
   args(2).toInt match{
     case 1 => 
             
        val customerCSV = sCxt.textFile(args(0)+"/"+customer)
        val customerData = customerCSV.map(line => line.split("\\|")) 
       
        val colCust = customerData.map(t => (t.head.toInt, 0))
        val colOrd = ordersData.filter( t => !(t(8).matches(".*special.*requests.*"))).map(t => (t(1).toInt, 1)).reduceByKey(_+_)
     
        val inner = colCust.leftOuterJoin(colOrd)
           .mapValues{
              case (_, Some(v)) =>  v
              case (_, None) =>  0
              }
               
       val result = inner.map{case (_,v) => (v, 1)}.reduceByKey(_+_)   
       val toSave = result.map(t => Array(t._1.toString, t._2.toString).mkString("|"))
        toSave.saveAsTextFile(args(1)+"/out_1")        
       
     case 2 => 
       
      val colOrd = ordersData.filter( t => !(t(8).matches(".*special.*requests.*"))).map(t => (t(1).toInt, 1)).reduceByKey(_+_) 
      val result = colOrd.map{case (_,v) => (v, 1)}.reduceByKey(_+_)   
      val toSave = result.map(t => Array(t._1.toString, t._2.toString).mkString("|"))
        toSave.saveAsTextFile(args(1)+"/out_2")
       
     case _ => exit(1)
   }
  }
}