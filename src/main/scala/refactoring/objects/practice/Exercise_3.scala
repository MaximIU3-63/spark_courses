package refactoring.objects.practice

case class Year(value: Int)

final class Contract(
                    id: Int,
                    expirationYear: Year
                  ) {
  def increaseYear(step: Int): Contract = {
    new Contract(this.id, Year(this.expirationYear.value + step))
  }
}
/*
Буду благодарен за ответы на накопившиеся вопросы)))

1) Правильно ли я понимаю, что объекты значения немутабельны и изменения возможны только путем создать нового объекта
значения и делается это все из объекта сущности?

2) Может ли объект значение принимать в качестве параметра принимать объект класса?

3) Рассматриваемая в 4 главе концепция - это концепция Domain Driven Design?
 */

object Exercise_3 extends App {
}
