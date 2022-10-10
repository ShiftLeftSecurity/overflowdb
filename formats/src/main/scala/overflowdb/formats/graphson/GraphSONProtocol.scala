package overflowdb.formats.graphson
import spray.json._

object GraphSONProtocol extends DefaultJsonProtocol {

  implicit object PropertyValueJsonFormat extends RootJsonFormat[PropertyValue] {
    def write(c: PropertyValue): JsValue = {
      c match {
        case x: StringValue  => JsString(x.`@value`)
        case x: BooleanValue => JsBoolean(x.`@value`)
        case x: ListValue    => JsArray(x.`@value`.map(write).toVector)
        case x: LongValue    => JsNumber(x.`@value`)
        case x: IntValue     => JsNumber(x.`@value`)
        case x: FloatValue   => JsNumber(x.`@value`)
        case x: DoubleValue  => JsNumber(x.`@value`)
        case _               => serializationError("PropertyValue expected")
      }
    }

    def read(value: JsValue): PropertyValue with Product = value match {
      case JsArray(Vector(JsArray(v), JsString(_))) =>
        ListValue(v.map({
          case lx: JsValue => read(lx)
          case _           => deserializationError("PropertyValue within list expected")
        }))
      case x: JsArray => readNonList(x)
      case _          => deserializationError("PropertyValue expected")
    }

    def readNonList(value: JsValue): PropertyValue with Product = value match {
      case JsString(v)  => StringValue(v)
      case JsBoolean(v) => BooleanValue(v)
      case JsArray(Vector(JsNumber(v), JsString(typ))) =>
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

    def read(value: JsValue): LongValue with Product = value match {
      case JsArray(Vector(JsNumber(v), JsString(typ))) if typ.equals(Type.Long.typ) =>
        LongValue(v.toLongExact)
      case _ => deserializationError("LongValue expected")
    }
  }

  implicit val propertyFormat: RootJsonFormat[Property] = jsonFormat3(Property)

  implicit val vertexPropertyFormat: RootJsonFormat[VertexProperty] = jsonFormat3(VertexProperty)

  implicit val vertexFormat: RootJsonFormat[Vertex] = jsonFormat4(Vertex)

  implicit val edgeFormat: RootJsonFormat[Edge] = jsonFormat8(Edge)

  implicit val graphSONElementsFormat: RootJsonFormat[GraphSONElements] = jsonFormat2(GraphSONElements)

  implicit val graphSONFormat: RootJsonFormat[GraphSON] = jsonFormat2(GraphSON)

}
