package io.shiftleft.overflowdb.traversal.help

import io.shiftleft.overflowdb.traversal.{NodeTraversal, Traversal}
import io.shiftleft.overflowdb.{NodeRef, OdbNode}
import org.reflections.Reflections

import scala.reflect.runtime.universe.runtimeMirror
import scala.jdk.CollectionConverters._

/**
 *
 * traversalExtBasePackage: The base package that we scan for @TraversalExt annotations.
 * Note that this restricts us to only find @Doc annotations in classes in that namespace and it's children.
 * If left empty, the scan takes considerable amount of time (depending on your classpath, obviously).
 */
class TraversalHelp(traversalExtBasePackage: String) {
  val ColumnNames = Array("step", "description")
  val ColumnNamesVerbose = ColumnNames :+ "traversal name"

  def renderTable(elementClass: Class[_], verbose: Boolean): String = {
    val isNode = classOf[OdbNode].isAssignableFrom(elementClass)
    val isNodeRef = classOf[NodeRef[_]].isAssignableFrom(elementClass)

    val stepDocs = {
      val base = stepDocsByElementType.get(elementClass).getOrElse(Nil)
      if (!verbose) base
      else {
        if (isNode || isNodeRef) base ++ genericStepDocs ++ genericNodeStepDocs
        else base ++ genericStepDocs
      }
    }

    val table = Table(
      columnNames = if (verbose) ColumnNamesVerbose else ColumnNames,
      rows = stepDocs.sortBy(_.methodName).map { stepDoc =>
        val baseColumns = List(s".${stepDoc.methodName}", stepDoc.doc.short)
        if (verbose) baseColumns :+ stepDoc.traversalClassName
        else baseColumns
      }
    )

    s"""Available steps for ${elementClass.getSimpleName}:
         |${table.render}
         |""".stripMargin
  }

  /**
    * Scans the entire classpath for classes annotated with @TraversalExt (using java reflection),
    * to then extract the @Doc annotations for all steps, and group them by the elementType (e.g. node.Method).
    */
  lazy val stepDocsByElementType: Map[Class[_], List[StepDoc]] = {
    for {
      traversal <- new Reflections(traversalExtBasePackage).getTypesAnnotatedWith(classOf[TraversalExt]).iterator.asScala
      elementType = traversal.getAnnotation(classOf[TraversalExt]).elementType
      stepDoc <- findStepDocs(traversal)
    } yield (elementType, stepDoc)
  }.toList.groupMap(_._1)(_._2)

  lazy val genericStepDocs: Iterable[StepDoc] =
    findStepDocs(classOf[Traversal[_]])

  lazy val genericNodeStepDocs: Iterable[StepDoc] =
    findStepDocs(classOf[NodeTraversal[_]])

  private lazy val mirror = runtimeMirror(this.getClass.getClassLoader)

  protected def findStepDocs(traversal: Class[_]): Iterable[StepDoc] = {
    val traversalTpe = mirror.classSymbol(traversal).toType
    Doc.docByMethodName(traversalTpe).map {
      case (methodName, doc) =>
        StepDoc(traversal.getName, methodName, doc)
    }
  }

  case class StepDoc(traversalClassName: String, methodName: String, doc: Doc)
}
