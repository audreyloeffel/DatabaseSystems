import java.util._
import scala.collection.mutable.{Map, Set, MutableList}


// IMPORTANT -- THIS IS INDIVIDUAL WORK. ABSOLUTELY NO COLLABORATION!!!


// - implement a (main-memory) data store with OMVCC.
// - objects are <int, int> key-value pairs.
// - if an operation is to be refused by the OMVCC protocol,
//   undo its xact (what work does this take?) and throw an exception.
// - garbage collection of versions is optional.
// - throw exceptions when necessary, such as when we try to:
//   + execute an operation in a transaction that is not running
//   + read a nonexisting key
//   + delete a nonexisting key
//   + write into a key whe2re it already has an uncommitted version
// - you may but do not need to create different exceptions for operations that
//   are refused and for operations that are refused and cause the Xact to be
//   aborted. Keep it simple!
// - keep the interface, we want to test automatically!

object OMVCC {
  case class KeyNotFoundException(s: String) extends Exception
  case class NoCommittedVersionFound(s: String) extends Exception
  case class CannotBeWritten(s: String) extends Exception
  case class TransactionAlreadyFinished(s: String) extends Exception
  case class TransactionNotFound(s: String) extends Exception

  sealed trait ReadAction
  case class Read(key: Int, version: Long) extends ReadAction
  case class Write(key: Int, version: Long){
    var value: Int = -1
  }
  case class ModQuery(k: Int, mods: MutableList[Int]) extends ReadAction

  case class Xact(id: Long, timestamp: Long){
    var logRead: Set[ReadAction] = Set()
    var logWrite: Set[Write] = Set()
    var committed: Boolean = false
    var finished: Boolean = false
  }
  private var committedTransactions: Set[Xact] = Set()
  //Map[key, List(version, value)]
  private var versionedStore : Map[Int, MutableList[(Long, Int)]] = Map.empty
  private var transactions : Map[Long, Xact] = Map.empty

  private var startAndCommitTimestampGen: Long = 0
  private var transactionIdGen: Long = 1L << 62

  // returns transaction id == logical start timestamp
  def begin: Long = {
    startAndCommitTimestampGen += 1
    transactionIdGen += 1
    transactions.put(transactionIdGen, Xact(transactionIdGen, startAndCommitTimestampGen))
    transactionIdGen
  }

  // return value of object key in transaction xact
  @throws(classOf[Exception])
  def read(xact: Long, key: Int): Int = {
    if(versionedStore.isDefinedAt(key)){
      val pair = getLastCommittedValue(versionedStore(key), xact)
      pair match {
        case Some(p) => 
          transactions(xact).logRead.add(Read(key, xact))
          p._2
        case None => 
          throw new NoCommittedVersionFound(s"No committed value for key: $key")
      }

      
    }
    else{
      rollback(xact)
      throw new KeyNotFoundException(s"Key : $key not found")
    }  
  }

  def getLastCommittedValue(listValue: MutableList[(Long, Int)], xactID: Long): Option[(Long, Int)] = {
    val currentTimestamp = transactions(xactID).timestamp
    val reverseList = listValue.reverse.dropWhile{
      case (version, value) => (version > currentTimestamp && xactID != version)
      }
    reverseList.get(0)
  }

  // return the list of values of objects whose values mod k are zero.
  // this is our only kind of query / bulk read.
  @throws(classOf[Exception])
  def modquery(xact: Long, k: Int): java.util.List[Integer] = {
    val l = new java.util.ArrayList[Integer]
    val ml: MutableList[Int] = MutableList()
    val transaction = getTransaction(xact, true)
    val keys = versionedStore.keys
    keys.foreach{
      key => {
        val listTuple = versionedStore(key)
        val value = getLastCommittedValue(listTuple, xact).get._2
        val mod = value % k
        if(mod == 0){
          l.add(value)
          ml+=value
        }
      }
    }
    val modQ = ModQuery(k, ml)
    transaction.logRead.add(modQ)
    l
  }

  // update the value of an existing object identified by key
  // or insert <key,value> for a non-existing key in transaction xact
  @throws(classOf[Exception])
  def write(xact: Long, key: Int, value: Int) {
    var transaction = getTransaction(xact, true)
    var optKeyList = versionedStore.get(key)
    
    optKeyList match {
      case Some(listTuple) => 

        // check if the value can be written
        if(canBeWritten(transaction, listTuple)){

          // check if the value is overriden
          if(listTuple.exists(_._1 == xact)){
            val index = listTuple.indexWhere(_._1 == xact)
            listTuple.update(index, (xact, value))
          }else{
            listTuple+=((xact, value))
          }
          versionedStore.put(key, listTuple.sortBy(_._1))
        }else{
          rollback(xact)
          throw new CannotBeWritten(s"$xact cannot write $value")
        }
      case None => 
        versionedStore.put(key, MutableList((xact, value)))
    }
    // now the value is written, we need to keep track of this write
    val c = Write(key, xact)
    c.value = value
    transaction.logWrite+=c
    transactions.update(key,transaction)
  }
  // Can write a value only if there not exits an uncommitted version or a newest comitted version
  def canBeWritten(transaction: Xact, listTuple: MutableList[(Long, Int)]): Boolean = {
    val currentTimestamp = transaction.timestamp
    val currentVersion = transaction.id
    listTuple.forall{case (version, value) => currentTimestamp > version || currentVersion == version}
  }

   // delete the object identified by key in transaction xact
  @throws(classOf[Exception])
  def delete(xact: Long, key: Int) {
    val transaction = getTransaction(key, true)
    versionedStore.get(key) match {
      case Some(listTuple: MutableList[(Long, Int)]) =>
        println(listTuple)
        val filteredList = listTuple.filterNot(t => t._1 == xact)
        versionedStore.update(key, filteredList)
        println(listTuple)
      case None =>
        throw new TransactionNotFound(s"Transaction: $xact not found")
    }
  }
 
  @throws(classOf[Exception])
  def commit(xact: Long) {
    //all transactions having committed between startTime and now
    val transaction = getTransaction(xact, true)
    val xactTimestamp = transaction.timestamp
    val xactsInInterval = committedTransactions.filterNot(x => x.timestamp > xactTimestamp && x.timestamp <= startAndCommitTimestampGen)
    val keysInInterval = xactsInInterval.flatMap(x => x.logRead).filter(_.isInstanceOf[Read]).map{case Read(k, _) => k}.toSet
    val keysReadInTransaction = transaction.logRead.filter(_.isInstanceOf[Read]).map{case Read(k, _) => k}.toSet

    val isValid: Boolean = (keysInInterval &~ keysReadInTransaction).isEmpty
    
    if (isValid) {
      startAndCommitTimestampGen += 1 //SHOULD BE USED
      //toutes les versions ecrites -> update le timestamp avec celui du comitted (startandcommite)
      updateTimestamp(transaction, startAndCommitTimestampGen)
      // add transaction in the comitted set
      transaction.committed = true
      transaction.finished = true
      committedTransactions.add(transaction)
      transactions.put(startAndCommitTimestampGen, transaction)
      println(s"$xact has committed")
    }else{
      rollback(xact)
    }
  }

  def updateTimestamp(transaction: Xact, newTS: Long) = {
    val writtenKeyList = transaction.logWrite.map{case Write(k, _) => k}
    writtenKeyList.foreach{ key => {
      val listTuple = versionedStore(key)
      val index = listTuple.indexWhere(p => p._1 == transaction.id)
      val tuple = listTuple(index)
      listTuple.update(index, (newTS, tuple._2))
      versionedStore.update(key, listTuple.sortBy(_._1))
    }
    }
  }
  @throws(classOf[Exception])
  def getTransaction(xact: Long, onlyUncomitted: Boolean): Xact = {
    transactions.get(xact) match {
      case Some(transaction) =>
        if ((transaction.committed || transaction.finished) && onlyUncomitted) {
          throw new TransactionAlreadyFinished(s"Transactions $xact has already finished")
        }
        transaction
      case None => 
        throw new TransactionNotFound(s"No transaction found with id: $xact")
    }

  }

  @throws(classOf[Exception])
  def rollback(xact: Long) {
    val transaction = getTransaction(xact, true)
    println(transaction.logWrite)
    val keysWriteInTransaction = transaction.logWrite.map{case Write(k, _) => k}.toSet
    keysWriteInTransaction.foreach(key => delete(xact, key)) 
    transaction.finished = true
  }
}

