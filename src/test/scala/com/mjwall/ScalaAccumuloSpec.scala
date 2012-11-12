package com.mjwall

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers

@RunWith(classOf[JUnitRunner])
class ScalaAccumuloSpec extends FunSpec with ShouldMatchers {

  describe("ScalaAccumulo") {
    describe("constructed with empty args") {

      it("should be valid") {
        ScalaAccumulo()
      }

      it("should not have a Connector") {
        (pending)
      }
    }

    describe("constructed with zookeeper, instance, username and password") {

      it("should be valid") {
        val zk = "zookeeperhere"
        ScalaAccumulo(zk,"i", "u","p").zookeepers should equal (zk)
      }

      it("should have a Connector") {
        (pending)
      }
    }
  }
}

