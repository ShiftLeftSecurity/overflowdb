package overflowdb.util;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PackedIntArrayTest {
  @Test
  public void shouldHaveSingleEmptyInstance() {
    final PackedIntArray a = PackedIntArray.create(0);
    final PackedIntArray b = PackedIntArray.create(0);
    Assert.assertSame(a, b);
  }

  @Test
  public void shouldProvideGetSetOperations() {
    final PackedIntArray a = PackedIntArray.of(1, 2, 3);
    assertEquals(1, a.get(0));
    assertEquals(2, a.get(1));
    assertEquals(3, a.get(2));
    assertEquals(3, a.length());
    a.set(0, 255); // should grow to short[]
    assertEquals(255, a.get(0));
    a.set(1, Short.MAX_VALUE + 1); // should grow to int[]
    assertEquals(Short.MAX_VALUE + 1, a.get(1));
  }
}
