package com.mjwall

import org.apache.accumulo.core.client.Instance
import org.apache.accumulo.core.client.ZooKeeperInstance

class ScalaAccumulo(instance: Instance, username: String, password: String) {

  val this.instance: Instance = instance
  val this.username: String = username
  val this.password: String = password

  /*
   * Auxillary constructor that creates a ZookeeperInstance based on the
   * instanceName and zookeepers string
   */
  def this(instanceName: String, zookeepers: String, username: String, password: String) = {
    this(ScalaAccumulo.getZooKeeperInstance(instanceName, zookeepers), username, password)
  }

  /*
   * Auxillary constructor that assumes instance is accumulo, zookeepers are running
   * on localhost:2181 and the username/password is root/secret
   */
  def this() = {
    this(ScalaAccumulo.getZooKeeperInstance("accumulo","localhost"),"root","secret")
  }

  def getUsername: String = this.username
  def getPassword: String = this.password
  def getInstance: Instance = this.instance

  override def toString: String = "ScalaAccumulo connected to " + instance.getInstanceName + " on " + instance.getZooKeepers + " as " + username

}

case class ZookeeperPair(host: String, port: Int)

object ScalaAccumulo {
  /*
   * This method checks that at least one of the zookeeper connections is open before trying to
   * get Accumulo's ZooKeeperInstance.  Otherwise, the code just loops trying to connect.
   */
  def getZooKeeperInstance(instance: String, zookeepers: String): ZooKeeperInstance = {
    if (parseZookeepers(zookeepers).exists(p => isSocketOpen(p.host, p.port))) {
      new ZooKeeperInstance(instance, zookeepers)
    } else {
      throw new java.net.SocketException("Can't connect to zookeepers with string: " + zookeepers)
    }
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

  private def isSocketOpen(host: String, port: Int): Boolean = {
    try {
      new java.net.Socket(host, port)
      true
    } catch {
      case e: Exception => false
    }
  }

  /*
   * Helper method to return a Mock Accumulo instance
   */
  def getMock() = {
    new ScalaAccumulo(new org.apache.accumulo.core.client.mock.MockInstance(),"root","secret")
  }
}









