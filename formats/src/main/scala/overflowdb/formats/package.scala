package overflowdb

import java.nio.charset.Charset
import java.nio.file.{Files, Path}
import scala.collection.immutable.ArraySeq
import scala.jdk.CollectionConverters.{IterableHasAsScala, MapHasAsScala}

package object formats {
  object Format extends Enumeration {
    val Neo4jCsv, GraphML, GraphSON, Dot = Value

    lazy val byNameLowercase: Map[String, Format.Value] =
      values.map(format => (format.toString.toLowerCase, format)).toMap

    lazy val valuesAsStringLowercase: Seq[String] =
      byNameLowercase.values.toSeq.map(_.toString.toLowerCase).sorted
  }

  /**
   * @return true if the given class is either array or a (subclass of) Java Iterable or Scala IterableOnce
   */
  def isList(clazz: Class[_]): Boolean = {
    clazz.isArray ||
      classOf[java.lang.Iterable[_]].isAssignableFrom(clazz) ||
      classOf[IterableOnce[_]].isAssignableFrom(clazz)
  }

  val iterableForList: PartialFunction[Any, Iterable[_]] = {
    case it: Iterable[_]           => it
    case it: IterableOnce[_]       => it.iterator.toSeq
    case it: java.lang.Iterable[_] => it.asScala
    case arr: Array[_]             => ArraySeq.unsafeWrapArray(arr)
  }

  /** If given outputFile is a directory: export into a new file in that directory
   * Otherwise: use the given outputFile as is, and create all parent directories (if not there already)
   */
  def resolveOutputFileSingle(outputFile: Path, defaultName: String): Path = {
    if (Files.exists(outputFile) && Files.isDirectory(outputFile)) {
      outputFile.resolve(defaultName)
    } else {
      Files.createDirectories(outputFile.getParent)
      outputFile
    }
  }

  def writeFile(file: Path, content: String): Unit =
    Files.write(file, content.getBytes(Charset.forName("UTF-8")))
}
