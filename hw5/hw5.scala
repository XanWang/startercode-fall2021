case class Neumaier(sum: Double, c: Double)

object HW {

  def q1_middle(x: Int, y: Int, z: Int): Int = {
    //the types of the input parameters have been declared.
    //you must do the same for the output type (see scala slides)
    //do not use return statements.
    if (((y > x) && (x > z)) || ((z > x) && (x > y))) {
      x
    } else if (((x > y) && (y > z)) || ((z > y) && (y > x))) {
      y
    } else z
  }

  def q2_interpolation(name: String, age: Int): String = {
    //the types of the input parameters have been declared.
    //you must do the same for the output type (see scala slides)
    //do not use return statements.
    (if (age >= 21) "hello" else "howdy") + ", " + name.toLowerCase()
  }

  def q3_polynomial(arr: Seq[Double]): Double = {
    //the types of the input parameters have been declared.
    //you must do the same for the output type (see scala slides)
    //do not use return statements.
    arr.foldLeft((0.0, 1)) { (x, y) => (x._1 + y * x._2, x._2 * 2) }._1
  }

  def q4_application(x: Int, y: Int, z: Int)(f: (Int, Int) => Int): Int = {
    //the types of the input parameters have been declared.
    //you must do the same for the output type (see scala slides)
    //do not use return statements.
    f(f(x, y), f(y, z))
  }

  def q5_stringy(st: Int, n: Int): Vector[String] = {
    Vector.tabulate(st + n) { x => x.toString }.drop(st)
  }

  def q6_modab(a: Int, b: Int, c: Vector[Int]): Vector[Int] = {
    c.filter { x => (x >= a) && ((x % b) != 0) }
  }

  def q7_find(a: Vector[Int])(f: Int => Boolean): Int = {
    val b = a.reverse.view.zipWithIndex.filter(x => f(x._1)).toVector
    if (b.isEmpty)
      -1
    else
      a.length - b.head._2 - 1
  }

  @annotation.tailrec
  def q8_find_tail(a: Vector[Int], i: Int = 0)(f: Int => Boolean): Int = {
    if (i == 0)
      if (a.nonEmpty)
        if (f(a.reverse.head)) a.length - i - 1 else q8_find_tail(a.reverse.tail, i + 1) { x => x % 2 == 0 }
      else
        -1
    else
      if (a.nonEmpty)
        if (f(a.head)) i else q8_find_tail(a.tail, i + 1) { x => x % 2 == 0 }
      else
        -1
  }

  def q9_neumaier(a: Seq[Double]): Double = {
    val aPos = a.filter { x => x >= 0 }
    val aNeg = a.filter { x => x < 0 }
    val cPos = aPos.foldLeft(Neumaier(0.0, 0.0)) { (x, y) => Neumaier(x.sum + (y - x.c), ((x.sum + (y - x.c)) - x.sum) - y) }.c
    val cNeg = aNeg.foldLeft(Neumaier(0.0, 0.0)) { (x, y) => Neumaier(x.sum + (y - x.c), ((x.sum + (y - x.c)) - x.sum) - y) }.c
    cPos + cNeg
  }
}


