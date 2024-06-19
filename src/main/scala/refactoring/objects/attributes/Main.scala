package refactoring.objects.attributes

/**
 * Запомните правило: поле должно быть размещено в том классе, который чаще других использует (или будет использовать) это поле.
 */

//non optim
//  sealed trait CustomerType
//
//  case object LoyalCustomer extends CustomerType
//
//  class Customer(id: Int, customerType: CustomerType, discount: Int)

//optim
sealed trait CustomerType {
  val discount: Int
}

case object LoyalCustomer extends CustomerType {
  override val discount: Int = 10
}

class Customer(id: Int, customerType: CustomerType)


object Main extends App {
}
