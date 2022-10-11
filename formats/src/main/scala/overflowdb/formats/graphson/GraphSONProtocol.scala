package overflowdb.formats.graphson
import spray.json._

object GraphSONProtocol extends DefaultJsonProtocol {

  implicit object PropertyValueJsonFormat extends RootJsonFormat[PropertyValue] {
    def write(c: PropertyValue): JsValue = {
      c match {
        case x: StringValue  => JsString(x.`@value`)
        case x: BooleanValue => JsBoolean(x.`@value`)
        case x: ListValue =>
          JsObject(
            "@value" -> JsArray(x.`@value`.map(write).toVector),
            "@type" -> JsString(x.`@type`)
          )
        case x: LongValue =>
          JsObject(
            "@value" -> JsNumber(x.`@value`),
            "@type" -> JsString(x.`@type`)
          )
        case x: IntValue =>
          JsObject(
            "@value" -> JsNumber(x.`@value`),
            "@type" -> JsString(x.`@type`)
          )
        case x: FloatValue =>
          JsObject(
            "@value" -> JsNumber(x.`@value`),
            "@type" -> JsString(x.`@type`)
          )
        case x: DoubleValue =>
          JsObject(
            "@value" -> JsNumber(x.`@value`),
            "@type" -> JsString(x.`@type`)
          )
        case _ => serializationError("PropertyValue expected")
      }
    }

    def read(value: JsValue): PropertyValue with Product = {
      value match {
        case JsString(v)  => return StringValue(v)
        case JsBoolean(v) => return BooleanValue(v)
        case _            =>
      }
      value.asJsObject.getFields("@value", "@type") match {
        case Seq(JsArray(v), JsString(_)) => ListValue(v.map(read).toArray)
        case x: Seq[_]                    => readNonList(x)
        case null                         => deserializationError("PropertyValue expected")
      }
    }

    def readNonList(value: Seq[_]): PropertyValue with Product = value match {
      case Seq(JsNumber(v), JsString(typ)) =>
        if (typ.equals(Type.Long.typ)) LongValue(v.toLongExact)
        else if (typ.equals(Type.Int.typ)) IntValue(v.toIntExact)
        else if (typ.equals(Type.Float.typ)) FloatValue(v.toFloat)
        else if (typ.equals(Type.Double.typ)) DoubleValue(v.toDouble)
        else deserializationError("Valid number type or list expected")
      case _ => deserializationError("PropertyValue expected")
    }
  }

  implicit object LongValueFormat extends RootJsonFormat[LongValue] {
    def write(c: LongValue): JsValue = PropertyValueJsonFormat.write(c)

    def read(value: JsValue): LongValue with Product =
      value.asJsObject.getFields("@value", "@type") match {
        case Seq(JsNumber(v), JsString(typ)) if typ.equals(Type.Long.typ) =>
          LongValue(v.toLongExact)
        case _ => deserializationError("LongValue expected")
      }
  }

  implicit val propertyFormat: RootJsonFormat[Property] =
    jsonFormat3(Property.apply)

  implicit val vertexFormat: RootJsonFormat[Vertex] =
    jsonFormat4(Vertex.apply)

  implicit val edgeFormat: RootJsonFormat[Edge] =
    jsonFormat8(Edge.apply)

  implicit val graphSONElementsFormat: RootJsonFormat[GraphSONElements] =
    jsonFormat2(GraphSONElements.apply)

  implicit val graphSONFormat: RootJsonFormat[GraphSON] =
    jsonFormat2(GraphSON.apply)

}
