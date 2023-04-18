package overflowdb.traversal.help

import org.reflections8.Reflections
import overflowdb.traversal.help.DocFinder.StepDoc
import overflowdb.traversal.{ElementTraversal, NodeTraversal, help}
import overflowdb.traversal
import overflowdb.{NodeDb, NodeRef}

import java.lang.annotation.{Annotation => JAnnotation}
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/** Searches classpath for @Traversal|@TraversalSource and @Doc annotations (via reflection). Used for `.help` step.
  * There are two use cases for this, which require slightly different implementations 1) `myDomain.help` - for the node
  * starter steps 2) `myDomain.someNodeType.help` - for steps that are available a specific node type
  *
  * For use case 2, we also take into account all parent traits of a node type, recursively.
  * I.e. if `SomeNodeType` has a base type `SomeBaseType`, and there are steps defined for `Traversal[SomeBaseType]`, we
  * will include those in the results.
  *
  * @param searchPackages:
  *   The base packages that we scan for - we're not scanning the entire classpath
  */
class TraversalHelp(searchPackages: DocSearchPackages) {
  import TraversalHelp._

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
    val stepDocs = for {
      packageName <- packageNamesToSearch
      traversal <- findClassesAnnotatedWith(packageName, classOf[help.TraversalSource])
      stepDoc <- findStepDocs(traversal)
    } yield stepDoc

    val table = Table(
      columnNames = ColumnNames,
      rows = stepDocs.distinct.sortBy(_.methodName).map { stepDoc =>
        List(s".${stepDoc.methodName}", stepDoc.doc.info)
      }
    )

    s"""Available starter steps:
       |${table.render}
       |""".stripMargin
  }

  /** Scans the entire classpath for classes annotated with @TraversalExt (using java reflection), to then extract the
    * \@Doc annotations for all steps, and group them by the elementType (e.g. node.Method).
    */
  lazy val stepDocsByElementType: Map[Class[_], List[StepDoc]] = {
    for {
      packageName <- packageNamesToSearch
      traversal <- findClassesAnnotatedWith(packageName, classOf[help.Traversal])
      annotation <- Option(traversal.getAnnotation(classOf[help.Traversal])).iterator
      stepDoc <- findStepDocs(traversal)
    } yield (annotation.elementType, stepDoc)
  }.toList.distinct.groupMap(_._1)(_._2)

  private def findClassesAnnotatedWith[Annotation <: JAnnotation](
      packageName: String,
      annotationClass: Class[Annotation]
  ): Iterator[Class[_]] =
    new Reflections(packageName).getTypesAnnotatedWith(annotationClass).asScala.iterator

  lazy val genericStepDocs: Iterable[StepDoc] =
    findStepDocs(classOf[traversal.TraversalSugarExt[_]]) ++ findStepDocs(
      classOf[traversal.TraversalFilterExt[_]]
    ) ++ findStepDocs(classOf[traversal.TraversalLogicExt[_]]) ++ findStepDocs(
      classOf[traversal.TraversalTrackingExt[_]]
    ) ++ findStepDocs(classOf[traversal.TraversalRepeatExt[_]])

  lazy val genericNodeStepDocs: Iterable[StepDoc] =
    findStepDocs(classOf[NodeTraversal[_]]) ++ findStepDocs(classOf[ElementTraversal[_]])

  protected def findStepDocs(traversal: Class[_]): Iterable[StepDoc] = {
    DocFinder
      .findDocumentedMethodsOf(traversal)
      // scala generates additional `fooBar$extension` methods, but those don't matter in the context of .help/@Doc
      .filterNot(_.methodName.endsWith("$extension"))
  }

  private def packageNamesToSearch: Seq[String] =
    searchPackages() :+ "overflowdb"
}

object TraversalHelp {
  private val ColumnNames = Array("step", "description")
  private val ColumnNamesVerbose = ColumnNames :+ "traversal name"
}
