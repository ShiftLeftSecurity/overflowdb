package overflowdb.traversal.help

import overflowdb.traversal.{ElementTraversal, NodeTraversal, Traversal, help}
import overflowdb.{NodeRef, NodeDb}
import java.lang.annotation.{Annotation => JAnnotation}

import org.reflections8.Reflections

import scala.jdk.CollectionConverters._

/**
 *
 * traversalExtBasePackage: The base package that we scan for @TraversalExt annotations.
 * Note that this restricts us to only find @Doc annotations in classes in that namespace and it's children.
 * If left empty, the scan takes considerable amount of time (depending on your classpath, obviously).
 */
class TraversalHelp(domainBasePackage: String) {
  val ColumnNames = Array("step", "description")
  val ColumnNamesVerbose = ColumnNames :+ "traversal name"

  def forElementSpecificSteps(elementClass: Class[_], verbose: Boolean): String = {
    val isNode = classOf[NodeDb].isAssignableFrom(elementClass)
    val isNodeRef = classOf[NodeRef[_]].isAssignableFrom(elementClass)

    val stepDocs = {
      def parentTraitsRecursively(clazz: Class[_]): List[Class[_]] = {
        val parents = clazz.getInterfaces.to(List)
        parents ++ parents.flatMap(parentTraitsRecursively)
      }

      val relevantClasses = elementClass +: parentTraitsRecursively(elementClass)
      val elementSpecificDocs = relevantClasses.map(stepDocsByElementType.get).flatten.flatten

      if (!verbose) elementSpecificDocs
      else {
        if (isNode || isNodeRef) elementSpecificDocs ++ genericStepDocs ++ genericNodeStepDocs
        else elementSpecificDocs ++ genericStepDocs
      }
    }

    val table = Table(
      columnNames = if (verbose) ColumnNamesVerbose else ColumnNames,
      rows = stepDocs.sortBy(_.methodName).map { stepDoc =>
        val baseColumns = List(s".${stepDoc.methodName}", stepDoc.doc.info)
        if (verbose) baseColumns :+ stepDoc.traversalClassName
        else baseColumns
      }
    )

    s"""Available steps for ${elementClass.getSimpleName}:
         |${table.render}
         |""".stripMargin
  }

  lazy val forTraversalSources: String = {
    val stepDocs = findClassesAnnotatedWith(classOf[TraversalSource]).flatMap(findStepDocs)
    val table = Table(
      columnNames = ColumnNames,
      rows = stepDocs.toList.sortBy(_.methodName).map { stepDoc =>
        List(s".${stepDoc.methodName}", stepDoc.doc.info)
      }
    )

    s"""Available starter steps:
       |${table.render}
       |""".stripMargin
  }

  /**
    * Scans the entire classpath for classes annotated with @TraversalExt (using java reflection),
    * to then extract the @Doc annotations for all steps, and group them by the elementType (e.g. node.Method).
    */
  lazy val stepDocsByElementType: Map[Class[_], List[StepDoc]] = {
    for {
      traversal  <- findClassesAnnotatedWith(classOf[help.Traversal])
      annotation <- Option(traversal.getAnnotation(classOf[help.Traversal])).iterator
      stepDoc    <- findStepDocs(traversal)
    } yield (annotation.elementType, stepDoc)
  }.toList.groupMap(_._1)(_._2)

  private def findClassesAnnotatedWith[Annotation <: JAnnotation](annotationClass: Class[Annotation]): Iterator[Class[_]] =
    new Reflections(domainBasePackage).getTypesAnnotatedWith(annotationClass).asScala.iterator

  lazy val genericStepDocs: Iterable[StepDoc] =
    findStepDocs(classOf[Traversal[_]])

  lazy val genericNodeStepDocs: Iterable[StepDoc] =
    findStepDocs(classOf[NodeTraversal[_]]) ++ findStepDocs(classOf[ElementTraversal[_]])

  protected def findStepDocs(traversal: Class[_]): Iterable[StepDoc] = {
    traversal.getMethods.flatMap { method =>
      method.getAnnotations.find(_.isInstanceOf[Doc]).map { case docAnnotation: Doc =>
        StepDoc(traversal.getName, method.getName,
          StrippedDoc(docAnnotation.info,
            docAnnotation.longInfo.stripMargin,
            docAnnotation.example.stripMargin)
        )
      }
    }
  }

  case class StepDoc(traversalClassName: String, methodName: String, doc: StrippedDoc)
  case class StrippedDoc(info: String, longInfo: String, example: String)
}
