package io.shiftleft.overflowdb.traversal.help

import scala.annotation.StaticAnnotation
import scala.reflect.runtime.universe._

/**
  * Annotation used for documentation.
  *
  * @param short a one line description for the overview table
  * @param long in-depth documentation
  * @example a short example for the overview table
  * */
case class Doc(short: String, long: String = "", example: String = "") extends StaticAnnotation

object Doc {

  def docByMethodName(tpe: Type): Map[String, Doc] = {
    def toDoc(annotation: Annotation): Doc = {
      val constants = annotation.tree.collect { case Literal(t: Constant) => t }
      constants.map(_.value.toString) match {
        case List(short) => Doc(short)
        case List(short, long) => Doc(short, long)
        case List(short, long, example) => Doc(short, long, example)
        case _ => Doc("")
      }
    }

    tpe.members
      .filter(_.isPublic)
      .map { member =>
        val docAnnotationMaybe = member.annotations.filter(_.tree.tpe =:= typeOf[Doc]).map(toDoc).headOption
        (member.name.toString, docAnnotationMaybe)
      }
      .collect { case (methodName, Some(doc)) => (methodName, doc) }
      .toMap
  }

}
