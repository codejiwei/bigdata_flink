/**
 * Author: jiwei01
 * Date: 2022/6/21 17:40
 * Package: 
 * Description:
 */
object TestMapValues {
  var dog: Dog = _
  def main(args: Array[String]): Unit = {







  }
}
class Dog(name: String) {
  val age = 8
  run()
  def run() = {
    println(s"Dog ${name} run......")
  }


  def checkAge: Unit = {
    if (age <= 10) {
      println("is young")
      return
    }
    println("is old")
  }
}
object Dog {
  def apply(name: String): Dog = new Dog(name)
}
