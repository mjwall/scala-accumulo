package com.mjwall

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.BeforeAndAfter


@RunWith(classOf[JUnitRunner])
class ScalaAccumuloSpec extends FunSpec with ShouldMatchers with BeforeAndAfter {

  describe("ScalaAccumulo") {
    describe("parseZookeepers") {
      it("should parse one") {
        val test = ScalaAccumulo.parseZookeepers("localhost:2181")
        test.head should equal (ZookeeperPair("localhost",2181))
      }

      it("should parse two") {
        val test = ScalaAccumulo.parseZookeepers("localhost:2181,remotehost:2001")
        test.head should equal (ZookeeperPair("localhost",2181))
        test.tail.head should equal (ZookeeperPair("remotehost",2001))
      }

      it("should default the port to 2181") {
        val test = ScalaAccumulo.parseZookeepers("localhost:2000,remotehost")
        test.head should equal (ZookeeperPair("localhost",2000))
        test.tail.head should equal (ZookeeperPair("remotehost",2181))
      }

      it("should return an empty list if nothing is passed in") {
        ScalaAccumulo.parseZookeepers(null) should equal (List())
      }

      it("should return an empty list if an empty string is passed in") {
        ScalaAccumulo.parseZookeepers("") should equal (List())
      }

      it("should still work if there is no , or :") {
        ScalaAccumulo.parseZookeepers("something").head should equal (ZookeeperPair("something",2181))
      }
    }

    describe("against a live Accumulo") {
      try {
        val instanceName = "accumulo"
        val zookeepers = "localhost"
        val username = "root"
        val password = "secret"
        ScalaAccumulo.getZooKeeperInstance(instanceName, zookeepers)
        // these tests will only run if you have accumulo running locally

        describe("constructed with a ZooKeeperInstance, username and password") {
          val zookeeperInstance = new org.apache.accumulo.core.client.ZooKeeperInstance(instanceName, zookeepers)
          val scalaAccumulo = new ScalaAccumulo(zookeeperInstance, username, password)

          it("should connect to ZooKeeperInstance") {
            scalaAccumulo.getInstance should equal (zookeeperInstance)
          }

          it("should use the username") {
            scalaAccumulo.getUsername should equal (username)
          }

          it("should use the password"){
            scalaAccumulo.getPassword should equal (password)
          }
        }

        describe("constructed with empty args") {
          val scalaAccumulo = new ScalaAccumulo()

          it("should connect to localhost") {
            scalaAccumulo.getInstance match {
              case i: org.apache.accumulo.core.client.ZooKeeperInstance => {
                i.getZooKeepers.split(",").head should equal ("localhost")
              }
              case _ => fail("Instance was not a ZookeeperInstance")
            }
          }

          it("should use 'root' as username") {
            scalaAccumulo.getUsername should equal ("root")
          }

          it("should use 'secret' as password") {
            scalaAccumulo.getPassword should equal ("secret")
          }
        }

        describe("constructed with instanceName, zookeepers, username and password") {
          val scalaAccumulo = new ScalaAccumulo(instanceName, zookeepers, username, password)

          it("should connect to instanceName") {
            scalaAccumulo.getInstance.getInstanceName should equal (instanceName)
          }

          it("should connect to zookeepers") {
            scalaAccumulo.getInstance match {
              case i: org.apache.accumulo.core.client.ZooKeeperInstance => {
                i.getZooKeepers.split(",").head should equal ("localhost")
              }
              case _ => fail("Instance was not a ZookeeperInstance")
            }
          }

          it("should use the username") {
            scalaAccumulo.getUsername should equal ("root")
          }

          it("should use the password") {
            scalaAccumulo.getPassword should equal ("secret")
          }
        }
      } catch {
        case e: java.net.SocketException => {
          it("can not be run, as Accumulo doesn't appear to be running") (pending)
        }
      }
    }

    describe("when the zookeeper connection fails") {
      it("should give you a nice message") {
        val zkHost = "300.300.300.300" //hopefully this IP doesn't exist
        try {
          val scalaAccumulo = new ScalaAccumulo("inointance",zkHost,"root","secret")
        } catch {
          case e : java.net.SocketException => {
            e.getMessage should equal ("Can't connect to zookeepers with string: " + zkHost)
          }
        }
      }
    }

    describe("getMock") {
      it("should return a mock Accumulo instance") {
        val mockAccumulo = ScalaAccumulo.getMock
        mockAccumulo.getInstance.isInstanceOf[org.apache.accumulo.core.client.mock.MockInstance] should be (true)
      }
    }

    describe("getConnector") {
      it("should return an Accumulo Connector") {
        val mockAccumulo = ScalaAccumulo.getMock
        mockAccumulo.getConnector.isInstanceOf[org.apache.accumulo.core.client.Connector] should be (true)
      }
    }

    describe("tableExists") {
      it("should return true if the table exists") {
        val mockAccumulo: ScalaAccumulo = ScalaAccumulo.getMock
        val tableName = "table1"
        mockAccumulo.createTable(tableName)
        mockAccumulo.tableExists(tableName) should be (true)
      }

      it("should return false if the table doesn't exist") {
        val mockAccumulo: ScalaAccumulo = ScalaAccumulo.getMock
        mockAccumulo.tableExists("inohere") should be (false)
      }
    }

    describe("createTable with tableName") {
      it("should create the table with just a tableName") {
        val mockAccumulo: ScalaAccumulo = ScalaAccumulo.getMock
        val tableName = "table1"
        mockAccumulo.createTable(tableName)
        mockAccumulo.getConnector.tableOperations().exists(tableName) should be (true)
      }

      it("should not error if the table exists") {
        val mockAccumulo: ScalaAccumulo = ScalaAccumulo.getMock
        val tableName = "table1"
        mockAccumulo.createTable(tableName)
        mockAccumulo.createTable(tableName)
      }
    }

    describe("createTablew with tableName and limitVersion") {
      it("should create the table") {
        (pending)
      }
    }

    describe("createTablew with tableName, versioningIter, and timeType") {
      it("should create the table") {
        (pending)
      }
    }



  }
}

