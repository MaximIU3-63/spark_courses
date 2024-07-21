package refactoring.objects.methods.practice

object Ex_5_3_1 extends App {
  def sum(a: Int, b: Int): Int = {
    val result: Long = a.toLong + b.toLong
    if (isInBorderOfIntType(result)) throw new RuntimeException("Integer Overflow")
    else result.toInt
  }

  private def isInBorderOfIntType(value: Long): Boolean = {
    if(value > Int.MaxValue || value < Int.MinValue) false
    else true
  }

  val res = sum(2147483646, 1)
}
