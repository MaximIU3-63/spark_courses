package refactoring.objects.types

//Entity

/**
 * 1) присутствует идентификатор
 * 2) объект может меняться со временем var isComplete: Boolean
 * 3) могут присутствовать методы изменения данных (должны возвразать тип UNIT)
 * 4) в имене указывается какое действие должен выполнять метод
 */
final case class Order(id: Int, var isComplete: Boolean) {
  def makeComplete(): Unit = {
    isComplete = true
  }
}

//Value object
final case class Credentials(userName: String, password: String)

/*
Если следовать правилам, то достаточно будет посмотреть на имя метода, чтобы определить, изменяемый это объект или неизменяемый:

в изменяемых объектах - метод, изменяющий состояние объекта, является командным
(т.е. в имени содержится указание-приказ, что необходимо сделать: markComplete, setPrice) и возвращает тип Unit
(т.е. метод не должен возвращать никакого значения)
в неизменяемых объектах - метод, направленный на изменение состояния объекта, должен описывать проводимые изменения:
withIncremented, withIncreased - как можно заметить, в названии присутствует причастие в форме прошедшего времени. Кроме того, объект неизменяемый, значит, метод возвращает новый объект
(и в этот раз никакого типа Unit)
 */

object Main