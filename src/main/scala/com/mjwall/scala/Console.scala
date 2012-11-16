package com.mjwall.scala

object Console {


   def main(args : Array[String]) : Unit = {
     val interpreter = new InterpreterWrapper() {
       def prompt = "accumulo> "
       def welcomeMsg = """Welcome to the Accumulo Scala Console!
    type :quit to exit"""
       def helpMsg = """This is printed *before* the help for eveyr command!"""
       //bind("josh", new MyClass("josh"))
       autoImport("com.mjwall.scala._")
     }

     interpreter.startInterpreting
   }
}
