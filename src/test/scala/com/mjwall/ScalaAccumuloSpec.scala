package com.mjwall

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers

@RunWith(classOf[JUnitRunner])
class ScalaAccumuloSpec extends FunSpec with ShouldMatchers {

  describe("ScalaAccumulo") {
    it ("should not be empty with no constructor args") {
      ScalaAccumulo should not be (Nil)
    }

    it ("should not be empty with zookeeper, instance, username and password args") {
      ScalaAccumulo("zookeeper","instance","username","password") should not be empty
    }
  }
}

