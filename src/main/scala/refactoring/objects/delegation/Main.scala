package refactoring.objects.delegation

//////////////////////EX1

/**
 * // dob - дата рождения
 * class Person(val name: String, val dob: Date) {
 * def printNameAndDetails(): Unit = {
 * println(s"Name: $name")
 * println(s"Details: Name: $name\n dob: ${dob.toString}")
 * }
 * }
 */
class Date(birthDay: String) {
  def birthdayInfo: String = {
    s"Date of birthday: $birthDay"
  }
}

class Person(name: String, dob: Date) {
  private def nameInfo: String = {
    s"Name: $name"
  }

  /*
  1) Убрал лишнее дублирование информации с именем (println(nameInfo))
  2) Переименовал метод
  3) Вынес в отдельные методы формировани строки с информацией об имени и дне рождения.
   */
  def printPersonDetails(): Unit = {
    println(s"Details: $nameInfo\n ${dob.birthdayInfo}")
  }
}

//////////////////////EX2

/**
class User(val id: Int, val name: String, val surname: String) {
  def fullName(): String = name.substring(0,1).toUpperCase() + name.substring(1,name.length())+ " " + surname.substring(0,1).toUpperCase()+surname.substring(1,surname.length())
}
 */

class User(id: Int, name: String, surname: String) {

  private def validator(value: String): Boolean = {
    value.forall(_.isLetter)
  }

  private lazy val hasNoDigits: Boolean = validator(name) && validator(surname)

  def fullName(): String = {
    if(hasNoDigits) {
      name.capitalize + " " + surname.capitalize
    } else {
      throw new Exception("User name or surname contains digits.")
    }
  }
}

//////////////////////EX3

import java.util.UUID


case class Email (
                   username: String,
                   domain: String
                 ) {

  override def toString: String = s"$username@$domain"
}

//case class Customer private(
//                             id: UUID,
//                             name: String,
//                             surname: String,
//                             email: Email,
//                             dateOfBirth: String
//                           ) {
//
//  def customerId(): UUID = id
//
//  override def toString: String = {
//    s"[$id]: $name,$surname,$email"
//  }
//}
//
//object Customer {
//  private def apply(
//                     id: UUID,
//                     name: String,
//                     surname: String,
//                     email: Email,
//                     dateOfBirth: String
//                   ): Customer = throw new IllegalAccessError
//
//  def apply(
//             name: String,
//             surname: String,
//             email: Email,
//             dateOfBirth: String
//           ): Customer = {
//    new Customer(
//      UUID.randomUUID(),
//      name,
//      surname,
//      email,
//      dateOfBirth
//    )
//  }
//}

case class Address(
                    city: String,
                    country: String
                  )

class Bank(
            name: String,
            address: Address,
            val email: Email,
            val customer: Customer
          ) {

//  val customerId: UUID = customer.customerId()

  def showEstablishedDate(): Unit = {
    println(s"$name Established in 2023.")
  }
}

//////////////////////EX4
trait OrderStatus
case object OrderPlaced extends OrderStatus
case object OutOfStock extends OrderStatus
case object InsufficientBalance extends OrderStatus
case object PaymentMethodMissing extends OrderStatus

trait PaymentMethod
case class Card(number: BigInt) extends PaymentMethod
case object NonPaymentMethod extends PaymentMethod

case class Product(id: Int, isInStock: Boolean, price: Int)
case class Customer(id: Int, balance: Int, paymentMethod: PaymentMethod)

case class Order(product: Product, customer: Customer, val orderStatus: OrderStatus) {
  private def showFailureNotify(): Unit = {
    println("Order can not be placed")
  }

  private def createOrder(product: Product, customer: Customer): Order = {
    if(!product.isInStock) {
      Order(
        product,
        customer,
        OutOfStock
      )
    } else if(customer.paymentMethod.equals(NonPaymentMethod)) {
      Order(
        product,
        customer,
        PaymentMethodMissing
      )
    } else if(customer.balance < product.price){
      Order(
        product,
        customer,
        InsufficientBalance
      )
    } else {
      Order(
        product,
        customer,
        OrderPlaced
      )
    }
  }

  private def notify(status: OrderStatus): Unit = {
    status match {
      case OrderPlaced =>
      case _ => showFailureNotify()
    }
  }

  def placeOrder(): Unit = {
    val order = createOrder(product, customer)
    notify(order.orderStatus)
  }
}

object Main extends App {

}
