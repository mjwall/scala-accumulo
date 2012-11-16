package com.mjwall.scala

import org.apache.accumulo.core.client.Instance
import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.accumulo.core.client.Connector

class Accumulo(instance: Instance, username: String, password: String) {

  val this.instance: Instance = instance //ZooKeeperInstance
  val this.username: String = username
  val this.password: String = password
  private val connector: Connector = instance.getConnector(username, password)

  def getUsername: String = this.username
  def getPassword: String = this.password
  def getInstance: Instance = this.instance
  def getConnector: Connector = this.connector

  /**
   * Auxillary constructor that creates a ZookeeperInstance based on the
   * instanceName and zookeepers string, along with a username, password
   *
   */
  def this(instanceName: String, zookeepers: String, username: String, password: String) = {
    this(Accumulo.getZooKeeperInstance(instanceName, zookeepers, new ZKChecker()), username, password)
  }

  /**
   * Auxillary constructor that no arguments.   Assumes instance is accumulo, zookeepers
   * are running on localhost:2181 and the username/password is root/secret.
   */
  def this() = {
    this(Accumulo.getZooKeeperInstance("accumulo","localhost", new ZKChecker()),"root","secret")
  }

  /*
   * Auxillary constructor for testing that creates a ZookeeperInstance based on the
   * instanceName and zookeepers string, along with a username, password
   * and Zookeeper checker.  Most people will want to the use the
   * constructor that takes instanceName, zookeepers, username and password
   */
  def this(instanceName: String, zookeepers: String, username: String, password: String, zkChecker: ZKChecker) = {
    this(Accumulo.getZooKeeperInstance(instanceName, zookeepers, zkChecker), username, password)
  }

  /**
   * Auxillary constructor for testing that only takes a Zookeeper checker.  Assumes instance is
   * accumulo, zookeepers are running on localhost:2181 and the username/password
   * is root/secret.  Most people will want to use the empty constructor
   */
  def this(zkChecker: ZKChecker) = {
    this(Accumulo.getZooKeeperInstance("accumulo","localhost", zkChecker),"root","secret")
  }

  def createTable(tableName: String) = {
    if(!tableExists(tableName)) {
      getConnector.tableOperations().create(tableName)
    }
  }

  def tableExists(tableName: String): Boolean = getConnector.tableOperations().exists(tableName)

  override def toString: String = "ScalaAccumulo connected to " + instance.getInstanceName + " on " + instance.getZooKeepers + " as " + username

}

object Accumulo {
  /**
   * This method checks that at least one of the zookeeper connections is open before trying to
   * get Accumulo's ZooKeeperInstance.  Trying to get a ZooKeeperInstance without checking will
   * just loop if Zookeeper is not running, which is really annoying.
   */
  def getZooKeeperInstance(instance: String, zookeepers: String, zkChecker: ZKChecker): ZooKeeperInstance = {
    if (parseZookeepers(zookeepers).exists(p => zkChecker.isSocketOpen(p.host, p.port))) {
      new ZooKeeperInstance(instance, zookeepers)
    } else {
      throw new java.net.SocketException("Can't connect to zookeepers with string: " + zookeepers)
    }
  }

  /**
   * This method checks that at least one of zookeeper connections is open before tyring to
   * get Accumulo's ZooKeeperInstance Trying to get a ZooKeeperInstance without checking will
   * just loop if Zookeeper is not running, which is really annoying.
   */
  def getZooKeeperInstance(instance: String, zookeepers: String): ZooKeeperInstance = {
    getZooKeeperInstance(instance, zookeepers, new ZKChecker())
  }

  def parseZookeepers(zookeepers: String): List[ZookeeperPair] = {
    if ((null == zookeepers) || (zookeepers == "")) List()
    else {
      zookeepers.split(",").map(_.split(":").toList).toList map (_  match {
        case List(host, port) => ZookeeperPair(host, port.toInt)
        case List(host) => ZookeeperPair(host, 2181)
        case Nil => null
      })
    }
  }

  /**
   * Helper method to return a Mock Accumulo instance
   */
  def getMock() = {
    new Accumulo(new org.apache.accumulo.core.client.mock.MockInstance(),"root","secret")
  }
}

case class ZookeeperPair(host: String, port: Int)

class ZKChecker extends java.net.Socket {

  def isSocketOpen(host: String, port: Int): Boolean = {
    try {
      this.connect(new java.net.InetSocketAddress(host, port))
      true
    } catch {
      case e: Exception => false
    } finally {
      this.close
    }
  }
}







