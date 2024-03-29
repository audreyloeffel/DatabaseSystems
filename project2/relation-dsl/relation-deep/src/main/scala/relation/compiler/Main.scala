package relation
package compiler

import shallow._
import deep._

object Main extends App {
  
  implicit object Context extends RelationDSLOpsPackaged
  
  def pgrmA = dsl""" 
    val schema = Schema("number", "digit")
    val En = Relation.scan("data/En.csv", schema, "|")
    val selEn = En.select(x => x.getField(schema, "number") == "one")
    val projEn = selEn.project(Schema("number"))
    projEn.print
  """
  // zero

  def pgrmB = dsl"""
    val EnSchema = Schema("number", "digit")
    val En = Relation.scan("data/En.csv", EnSchema, "|")
    val FrSchema = Schema("digit", "nombre")
    val Fr = Relation.scan("data/Fr.csv", FrSchema, "|")
    val EnFr = En.join(Fr, "digit", "digit")
    EnFr.print
  """
  
  def pgrmC = dsl"""
    val EnSchema = Schema("number", "digit")
    val En = Relation.scan("data/En.csv", EnSchema, "|").select(x => x.getField(EnSchema, "number") == "one")
    val projEn = En.project(Schema("number"))
    projEn.print
  """
  // "1"
  def pgrmD = dsl""" 
    val schema = Schema("number", "digit")
    val En = Relation.scan("data/En.csv", schema, "|")
    val selEn = En.select(x => x.getField(schema, "number") == "zero")
    val projEn = selEn.project(Schema("digit"))
    projEn.print
  """ // "0"

  def pgrmE = dsl""" 
    val schema = Schema("number", "digit")
    val En = Relation.scan("data/En.csv", schema, "|")
    val selEn = En.select(x => x.getField(schema, "number") == "one")
    val projEn = selEn.project(Schema("digit"))
    projEn.print
  """ // "1"
  def pgrm = pgrmB
  
  val compiler = new RelationCompiler(Context)

  compiler.compile(pgrm) 
}
