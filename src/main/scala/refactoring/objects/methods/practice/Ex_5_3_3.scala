package refactoring.objects.methods.practice

//case class User(name: String, age: Int)
//
//def giveAccessToUser(name: String, age: Int) {
//  if (age < 0 || age > 120) {
//    throw new Error("Age is Invalid");
//  }
//
//  if (age >= 18) println(s"access granted to $name")
//  else println(s"access denied for $name ")
//}

sealed trait VerifiedStatus
case object Verified extends VerifiedStatus
case object Unverified extends VerifiedStatus

object Defaults {
  val AgeLowerLimit: Int = 16
  val AgeUpperLimit: Int = 85
  val VerificationAge: Int = 18
}

case class Age(value: Int) {
  require(value >= Defaults.AgeLowerLimit && value <= Defaults.AgeUpperLimit, s"The age range should be between ${Defaults.AgeLowerLimit} and ${Defaults.AgeUpperLimit} years")

  def verifyAge: VerifiedStatus = {
    if(value > Defaults.VerificationAge) Verified
    else Unverified
  }
}

case class User(name: String, age: Age) {
  require(name.nonEmpty, "The name parameter must be specified")
}

case class Access(user: User) {
  def grantAccess(): Unit = {
    user.age.verifyAge match {
      case Verified => println(s"Access granted to ${user.name}")
      case Unverified => println(s"Access denied for ${user.name}")
    }
  }
}

object Ex_5_3_3 extends App {
  val user = User("Maksim", Age(19))
  Access(user).grantAccess()
}
