package overflowdb.traversal.help;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
  * Annotation used for documentation.
  *
  * @param info a one line description for the overview table
  * @param longInfo in-depth documentation
  * @param example a short example for the overview table
  * note: both longInfo and example are processed by Scala's `.stripMargin` to
  *       allow for convenient multiline string writing with "|".
  *       see https://docs.scala-lang.org/scala3/book/first-look-at-types.html#multiline-strings
  * */
@Retention(RetentionPolicy.RUNTIME)
public @interface Doc {
  String info();
  String longInfo() default "";
  String example() default "";
}
