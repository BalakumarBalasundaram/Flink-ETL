package cn.swordfall.hbaseOnFlink

import java.util
import java.util.ArrayList

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes


class HBaseOutputFormat extends OutputFormat[String]{

  val zkServer = "192.168.187.201"
  val port = "2181"
  var conn: Connection = null

  /**
    * @param configuration
    */
  override def configure(configuration: Configuration): Unit = {
  }


  override def open(i: Int, i1: Int): Unit = {
    val config: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create
    config.set(HConstants.ZOOKEEPER_QUORUM, zkServer)
    config.set(HConstants.ZOOKEEPER_CLIENT_PORT, port)
    config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000)
    config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000)
    conn = ConnectionFactory.createConnection(config)
  }

 
  override def writeRecord(it: String): Unit = {
    val tableName: TableName = TableName.valueOf("test")
    val cf1 = "cf1"
    val array: Array[String] = it.split(",")
    val put: Put = new Put(Bytes.toBytes(array(0)))
    put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("name"), Bytes.toBytes(array(1)))
    put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("age"), Bytes.toBytes(array(2)))
    val putList: util.ArrayList[Put] = new util.ArrayList[Put]
    putList.add(put)
    val params: BufferedMutatorParams = new BufferedMutatorParams(tableName)
    params.writeBufferSize(1024 * 1024)
    val mutator: BufferedMutator = conn.getBufferedMutator(params)
    mutator.mutate(putList)
    mutator.flush()
    putList.clear()
  }


  override def close(): Unit = {
    try {
      if (conn != null) conn.close()
    } catch {
      case e: Exception => println(e.getMessage)
    }
  }
}
