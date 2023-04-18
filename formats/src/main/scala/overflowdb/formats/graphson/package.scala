package overflowdb.formats

import scala.language.implicitConversions

package object graphson {

  object Type extends Enumeration {
    // Boolean and String do not require type specification
    // strings are simply in quotes and booleans are not
    // We use the following as placeholders
    val Boolean = GraphSONVal(1, "g:Boolean")
    val String = GraphSONVal(2, "g:String")
    val Int = GraphSONVal(3, "g:Int32")
    val Long = GraphSONVal(4, "g:Int64")
    val Float = GraphSONVal(5, "g:Float")
    val Double = GraphSONVal(6, "g:Double")
    val List = GraphSONVal(7, "g:List")

    def fromRuntimeClass(clazz: Class[_]): Type.Value = {
      if (clazz.isAssignableFrom(classOf[Boolean]) || clazz.isAssignableFrom(classOf[java.lang.Boolean]))
        Type.Boolean
      else if (clazz.isAssignableFrom(classOf[Int]) || clazz.isAssignableFrom(classOf[Integer]))
        Type.Int
      else if (clazz.isAssignableFrom(classOf[Long]) || clazz.isAssignableFrom(classOf[java.lang.Long]))
        Type.Long
      else if (clazz.isAssignableFrom(classOf[Float]) || clazz.isAssignableFrom(classOf[java.lang.Float]))
        Type.Float
      else if (clazz.isAssignableFrom(classOf[Double]) || clazz.isAssignableFrom(classOf[java.lang.Double]))
        Type.Double
      else if (clazz.isAssignableFrom(classOf[String]))
        Type.String
      else if (clazz.isAssignableFrom(classOf[List[_]]))
        Type.List
      else
        throw new AssertionError(
          s"unsupported runtime class `$clazz` - only ${Type.values.mkString("|")} are supported...}"
        )
    }

    protected case class GraphSONVal(num: Int, typ: String) extends super.Val
    implicit def name(x: GraphSONVal): String = x.typ
  }

  case class GraphSON(`@value`: GraphSONElements, `@type`: String = "tinker:graph")

  case class GraphSONElements(vertices: Seq[Vertex], edges: Seq[Edge])

  case class Vertex(id: LongValue, label: String, properties: Map[String, Property], `@type`: String = "g:Vertex")

  case class Edge(
      id: LongValue,
      label: String,
      inVLabel: String,
      outVLabel: String,
      inV: LongValue,
      outV: LongValue,
      properties: Map[String, Property],
      `@type`: String = "g:Edge"
  )

  trait PropertyValue {
    def `@value`: Any
    def `@type`: String
  }

  case class StringValue(override val `@value`: String, `@type`: String = Type.String.typ) extends PropertyValue

  case class BooleanValue(override val `@value`: Boolean, `@type`: String = Type.Boolean.typ) extends PropertyValue

  case class LongValue(override val `@value`: Long, `@type`: String = Type.Long.typ) extends PropertyValue

  case class IntValue(override val `@value`: Int, `@type`: String = Type.Int.typ) extends PropertyValue

  case class DoubleValue(override val `@value`: Double, `@type`: String = Type.Double.typ) extends PropertyValue

  case class FloatValue(override val `@value`: Float, `@type`: String = Type.Float.typ) extends PropertyValue

  case class ListValue(override val `@value`: Array[PropertyValue], `@type`: String = Type.List.typ)
      extends PropertyValue

  case class Property(id: LongValue, `@value`: PropertyValue, `@type`: String = "g:Property")

}
