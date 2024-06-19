package refactoring.objects.methods

/*
   Плохой пример реализации
   1) Идет обращение к объектам вне класса Account
   2) При добавлении нового типа PayerType придется вносить изменения в существующий метод taxCharge()
   3) Account не имеет ни род. классов, ни потомков.
 */

//trait PayerType
//
//case object Individual extends PayerType
//case object Organization extends PayerType
//
//class Account(payerType: PayerType) {
//  private var amount = 100.0
//  private var daysOverdrawn = 0
//
//  def taxCharge(): Double = {
//
//    if (this.payerType == Individual)
//      amount * 0.13 + daysOverdrawn * 0.43
//
//    else if (this.payerType == Organization)
//      amount * 0.30 + daysOverdrawn * 1.34 + 66.5
//
//    else 0
//
//  }
//}

/*
Оптимизированный пример реализации
 */
trait PayerType {
  def taxPayer(amount: Double, daysOverdrawn: Int): Double
}

case object Individual extends PayerType {
  override def taxPayer(amount: Double, daysOverdrawn: Int): Double = {
    amount * 0.13 + daysOverdrawn * 0.43
  }
}

case object Organization extends PayerType {
  override def taxPayer(amount: Double, daysOverdrawn: Int): Double = {
    amount * 0.30 + daysOverdrawn * 1.34 + 66.5
  }
}

class Account(payerType: PayerType) {
  private var amount = 100.0
  private var daysOverdrawn = 0

  def taxPayer(): Double = {
    payerType.taxPayer(amount, daysOverdrawn)
  }
}

/**
 * Методы можно разделить на два типа:
 * - объектные (методы, которые вызываются только на экземпляре класса)
 * - статические (методы, для вызова которых не требуется отдельный экземпляр класса)
 */

class Student(name: String) {
  def displayName(): Unit = {
    println(s"Student name is $name")
  }
}

object Student {
  def toUpperCase(s: String): String = {
    s.toUpperCase
  }
}


///////////////////////////////////////////////////
object Main extends App {
  private val indAccount = new Account(Individual)
  println(indAccount.taxPayer())

  private val orgAccount = new Account(Organization)
  println(orgAccount.taxPayer())
}
