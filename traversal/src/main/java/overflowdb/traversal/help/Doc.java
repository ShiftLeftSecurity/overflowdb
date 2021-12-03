package overflowdb.traversal.help;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
  * Annotation used for documentation.
  *
  * @param info a one line description for the overview table
  * @param longInfo in-depth documentation processed by Scalas .stripMargin to
  *                 allow for convienied multiline string writing with "|".
  * @param example a short example for the overview table processed by Scalas
  *                  .stripMargin to allow for convienied multiline string
  *                  writing with "|".
  * */
@Retention(RetentionPolicy.RUNTIME)
public @interface Doc {
  String info();
  String longInfo() default "";
  String example() default "";
}
