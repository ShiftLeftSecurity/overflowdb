package overflowdb.traversal.help;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface Doc {
  String info();
  String longInfo() default "";
  String example() default "";
}
