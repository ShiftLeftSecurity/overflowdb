package overflowdb.traversal.help

object DocFinder {
  def findDocumentedMethodsOf(clazz: Class[_]): Iterable[StepDoc] = {
    clazz.getMethods.flatMap { method =>
      method.getAnnotations.find(_.isInstanceOf[Doc]).map { case docAnnotation: Doc =>
        StepDoc(
          clazz.getName,
          method.getName,
          StrippedDoc(docAnnotation.info, docAnnotation.longInfo.stripMargin, docAnnotation.example.stripMargin)
        )
      }
    }
  }

  case class StepDoc(traversalClassName: String, methodName: String, doc: StrippedDoc)
  case class StrippedDoc(info: String, longInfo: String, example: String)
}
