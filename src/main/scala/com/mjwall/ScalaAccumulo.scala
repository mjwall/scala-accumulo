package com.mjwall

case class ScalaAccumulo(zookeepers: String, instance: String, username: String, password: String)

object ScalaAccumulo {
  def apply(): ScalaAccumulo = ScalaAccumulo(null,null,null,null)
}

