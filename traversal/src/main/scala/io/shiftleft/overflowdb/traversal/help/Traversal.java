package io.shiftleft.overflowdb.traversal.help;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Traversal {
  Class elementType();
}
