package relation
package compiler

import scala.collection.mutable

import ch.epfl.data.sc.pardis
import pardis.optimization.RecursiveRuleBasedTransformer
import pardis.quasi.TypeParameters._
import pardis.types._
import PardisTypeImplicits._
import pardis.ir._

import relation.deep.RelationDSLOpsPackaged
import relation.shallow._  

/*
Audrey Loeffel 217360
*/
class ColumnStoreLowering(override val IR: RelationDSLOpsPackaged, 
  override val schemaAnalysis: SchemaAnalysis) extends RelationLowering(IR, 
  schemaAnalysis) {
  import IR.Predef._
  
  type Column = Array[String]
  type LoweredRelation =  (Array[Rep[Column]], Rep[Int]) 
  //(relation, count)
  
  def relationScan(scanner: Rep[RelationScanner], schema: Schema, size: Rep[Int], 
    resultSchema: Schema): LoweredRelation = {

    val nbColumns = schema.size
    val arr =  new Array[Rep[Column]](nbColumns)  

    val readLine: (Rep[Int] => Rep[Unit]) = (row: Rep[Int]) => {
      for(col <- 0 until nbColumns){
        val column = arr(col)
        dsl"""
          $column($row) = $scanner.next_string()
        """
        arr(col) = column
      }
      dsl"unit()"
    }     
  
    for (i <- 0 until nbColumns) {
      arr(i) = dsl"new Array[String]($size)"
    }  

    dsl"""
      var row = 0
      while($scanner.hasNext) {  
        $readLine(row)
        row = row + 1
      }
      
    """
    (arr, size)  }
  
  def relationProject(relation: Rep[Relation], schema: Schema, 
    resultSchema: Schema): LoweredRelation = {

    val (lowerRep, size) = getRelationLowered(relation)
    val indexesColToKeep: Array[Int] = resultSchema.columns.map(col => schema.indexOf(col)).toArray
    val outRelation = indexesColToKeep.map(index => lowerRep(index))

    (outRelation, size)    
  }
  
  def relationSelect(relation: Rep[Relation], field: String, value: Rep[String], 
    resultSchema: Schema): LoweredRelation = {
    
    val (lowerRep, size) = getRelationLowered(relation)
    val indexColumn = resultSchema.indexOf(field)
    val nbColumns = resultSchema.size
      
    // Count the number of selected row
    val selectedColumn = lowerRep(indexColumn)
    var nbRow = dsl"""
      var nbRow = 0
      for(idxRow <- 0 until $size){
        if($selectedColumn(idxRow) == $value){
          nbRow += 1
        }
      }
      nbRow
    """
     //Construct the empty relation

    val arr =  new Array[Rep[Column]](nbColumns) 
    for (i <- 0 until nbColumns) {
          arr(i) = dsl"new Array[String]($nbRow)"
    }

    val selectLine: ((Rep[Int], Rep[Int]) => Rep[Unit]) = (row: Rep[Int], lastIdx: Rep[Int]) => {
      for(idxCol <- 0 until nbColumns){
        val outputColumn = arr(idxCol)
        val relCol = lowerRep(idxCol)
        dsl"""
          val a = $relCol($row)
          $outputColumn($lastIdx) = a
        """
        arr(idxCol) = outputColumn
      }
      dsl"unit()"
    }
   
    /*
      Read each value in the selected column. 
      If it matches, then read the whole line and save it on the result array
    */
    dsl"""
      var lastIdx = 0
      for(idxRow <- 0 until $size){
        if($selectedColumn(idxRow) == $value){
          $selectLine(idxRow, lastIdx)
          lastIdx += 1
        }
      }
    """
    (arr, nbRow)
  }
  
  def relationJoin(leftRelation: Rep[Relation], rightRelation: Rep[Relation], 
    leftKey: String, rightKey: String, resultSchema: Schema): LoweredRelation = {
   
    val (lowerRep1, size1) = getRelationLowered(leftRelation)
    val (lowerRep2, size2) = getRelationLowered(rightRelation)
    val schema1 = getRelationSchema(leftRelation)
    val schema2 = getRelationSchema(rightRelation)
    
    val indexLeftKey = schema1.columns.indexOf(leftKey)
    val indexRightKey = schema2.columns.indexOf(rightKey)
    val leftColumn = lowerRep1(indexLeftKey)  
    val rightColumn = lowerRep2(indexRightKey)
    val nbColumnsLeft = schema1.size
    val nbColumnsRight = schema2.size
    val nbColumnsAfterJoin = nbColumnsLeft + nbColumnsRight - 1
    val arr =  new Array[Rep[Column]](nbColumnsAfterJoin) 

    // Count the number of new lines
    val nbRow = dsl"""
      var nbRow = 0
      for(idxRowLeft <- 0 until $size1){
        for(idxRowRight <- 0 until $size2){
          if($leftColumn(idxRowLeft) == $rightColumn(idxRowRight)){
            nbRow += 1
          }
        }        
      }
      nbRow 
    """
    // Create the empty arrays
    for (i <- 0 until nbColumnsAfterJoin) {
          arr(i) = dsl"new Array[String]($nbRow)"
    }

    val joinLine: ((Rep[Int], Rep[Int], Rep[Int]) => Rep[Unit]) = 
    (idxRowLeft: Rep[Int], idxRowRight: Rep[Int], idx: Rep[Int]) =>{
      
      for(column <- 0 until nbColumnsLeft) {
        val outputColumn = arr(column)
        val relColumn = lowerRep1(column)
        dsl"""
          val a = $relColumn($idxRowLeft)
          $outputColumn($idx) = a
        """
        arr(column) = outputColumn
      }

      var off = nbColumnsLeft

      for(column <- 0 until nbColumnsRight){
        val realColumn = column + off
        if(column == indexRightKey){
          off -= 1
        }else{
          val outputColumn = arr(realColumn)
          val relColumn = lowerRep2(column)
          dsl"""
            val a = $relColumn($idxRowRight)
            $outputColumn($idx) = a
          """
          arr(realColumn) = outputColumn
        }
      }
      dsl"unit()"
    } 

    // For each line in leftRel, search all lines in rightRel that match, then join line
    dsl"""
      var lastIdx = 0
      for(idxRowLeft <- 0 until $size1){
        for(idxRowRight <- 0 until $size2){
          if($leftColumn(idxRowLeft) == $rightColumn(idxRowRight)){
            $joinLine(idxRowLeft, idxRowRight, lastIdx)
            lastIdx += 1
          }
        }        
      }
    """
    (arr, nbRow)
  }
  
  def relationPrint(relation: Rep[Relation]): Unit = {

    val (lowerRep, size) = getRelationLowered(relation)
    val nbColumns = lowerRep.size

    val printer: (Rep[Int] => Rep[String]) = (row: Rep[Int]) => {
      var line: Rep[String] = dsl""" "" """
      for(col <- 0 until nbColumns){
        val column = lowerRep(col)
        line = dsl"""
          $line + $column($row)
        """
        if(col < nbColumns - 1){
          line = dsl"""
            $line + "|"     
          """   
        }
      }
      dsl"$line"
    }
   
  dsl"""
  for(row <- 0 until $size){
    println($printer(row))
  }
  """    
  }  
}
