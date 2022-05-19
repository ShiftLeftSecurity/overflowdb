package overflowdb.formats

package object graphml {
  // we could/should make these configurable...
  val KeyForNodeLabel = "labelV"
  val KeyForEdgeLabel = "labelE"

  object Type extends Enumeration {
    val Boolean = Value("boolean")
    val Int = Value("int")
    val Long = Value("long")
    val Float = Value("float")
    val Double = Value("double")
    val String = Value("string")

    /** Warning: list properties are not natively supported by graphml...
     *  For our purposes we fake it by encoding it as a `;` separated string - if you import this into a different database, you'll need to parse that separately.
     *  In comparison, Tinkerpop just bails out if you try to export a list property to graphml. */
    val List = Value("list")

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
      else if (clazz.isArray || classOf[java.lang.Iterable[_]].isAssignableFrom(clazz) || classOf[IterableOnce[_]].isAssignableFrom(clazz))
        Type.List
      else
        throw new AssertionError(s"unsupported runtime class `$clazz` - only ${Type.values.mkString("|")} are supported...}")
    }
  }

  private[graphml] case class PropertyContext(name: String, tpe: Type.Value)

}
