package com.enzobnl.sparkscalaexpe.playground
import org.apache.http.protocol.ExecutionContext
import sun.misc.Regexp

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
/*
Case classes can be seen as plain and immutable data-holding objects that should exclusively depend on their constructor arguments.
 */
trait Iterator[A] {
  def hasNext: Boolean
  def next(a: A): A
}
class IntIterator[A](to: Int) extends Iterator[A]{
  private var current = 0
  override def hasNext: Boolean = current < to
  override def next(a: A): A =  {
    if (hasNext) {
//      val t = current
//      current += 1
      a
    } else a
  }
}

trait Pet {
  val name: String
}
trait PetGentil extends Pet {
  val doud: Boolean
}

class Cat(val name: String, val doud: Boolean) extends PetGentil
class Dog(val name: String, val doud: Boolean) extends PetGentil
trait T extends Cat{

}
object TourOfScalaStuff1 extends Runnable {
  private var _ss = Seq.apply(1,2)
  def ss_=( s: Seq[Int]): Unit = {this._ss=s}

  val e = 7
  val a = () => this.e
  abstract class Notification

  case class Email(sender: String, title: String, body: String) extends Notification

  case class SMS(caller: String, message: String) extends Notification

  case class VoiceRecording(contactName: String, link: String) extends Notification


  def methodTest(x: Int)(s: String): String = x + s
  def f1(x: Int): Int = x*8
  var f2: (Int => Int) => Int = (x: Int=>Int) => x(8)

  object CustomerID {

    def apply(name: String) = s"$name--${Math.abs(Random.nextLong)}"

    def unapply(customerID: String): Option[(String, String)] = {
      val stringArray: Array[String] = customerID.split("--")
      if (stringArray.tail.nonEmpty) Some((stringArray(0), stringArray(1))) else None
    }
  }

  val customer1ID = CustomerID("Sukyoung")  // Sukyoung--23098234908
  customer1ID match {
    case CustomerID(name, _) => println(name) // prints Sukyoung
    case _ => println("Could not extract a CustomerID")
  }
  class Stack[A](private var elements: List[A] = Nil) {
    def push(x: A) { elements = x +: elements }
    def peek: A = elements.head
    def pop(): A = {
      val currentTop = peek
      elements = elements.tail
      currentTop
    }
    def +(other: Stack[A]): Stack[A] ={
      var l: List[A] = this.elements :+ pop
      new Stack[A]()
    }
  }

    case  class User(name: String, var age: Int){
    var age_ = age
    def a = age_ = age_ + 1
  }
  class Animal(var name: String) {
    def roa: Unit = println(s"i'm $name")
  }
//  case class Cat extends Animal(name: String)
//  case class Dog extends Animal(name: String)


  override def run: Unit={
    abstract class Animal[T] {
      def name[T>:String]: T
    }
//
//    abstract class Pet[T] extends Animal[T] {}
//
//    class Cat[T] extends Pet[T] {
//      override def name[T>:String]: T = "Cat"
//    }
//    class Dog[T] extends Pet[T] {
//      override def name[T>:String]: T = "Dog"
//    }
//
//
//    class Lion[T] extends Animal[T] {
//      override def name: String = "Lion"
//    }
//
//    class PetContainer[T, P <: Animal[T]](p: P) {
//      def pet: P = p
//    }

   // val dogContainer = new PetContainer[Dog](new Dog)
//    val catContainer = new PetContainer[Animal](new Cat)

    // this would not compile
//    val lionContainer = new PetContainer[Lion](new Lion)

  }
}


//minimal example, closures et scopes :
/*
class Animal(var name: String) {
    def roa: Unit = println(s"i'm $name")
  }
    var rambo = new Animal("chacha")
    val g = (a: Int, ramboedName: String, s: Animal, serv: String) => println(s"Xx-${s.name}$ramboedName-xX[$serv]$a")
    val f = () => {
      val ramboedName = "PL"
      val a = rambo
      println(a.hashCode(), rambo.hashCode())
      g(_: Int, ramboedName, rambo, _: String)  // la référence 'rambo' reste vivante après la fermeture de la pile d'execution de f: on embarque une référence vers la référence = si l'objet change, la closure suis + si la référence rambo change de target, la closure va suivre.
//      g(_: Int, ramboedName, a, _: String)  // la référence 'a' va mourir après la fermeture de la pile d'execution de f: une nouvelle référence vers l'objet est embarquée = si l'objet change, la closure suis mais on est plus du tout lié à la ref rambo.
    }
    val h = () => rambo.name
    val rambor = f()
    rambor(1, "Menalt")
    val a = rambo
    rambo = new Animal("chienchien")
    a.name = "RaMBOOO"
    rambor(2,"Menalt")
    print(h())


  /*
  cas des référence ver des AnyVal/immutableobject:
  // la ref rambo reste vivante: si la ref change, la closure suis
  // la ref 'a' meurt, on copie une nouvelle ref vers la AnyVal/immutableobject : la closure ne suivra plus aucun changement.
  //
   */
 */