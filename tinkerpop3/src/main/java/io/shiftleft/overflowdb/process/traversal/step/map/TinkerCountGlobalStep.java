package io.shiftleft.overflowdb.process.traversal.step.map;

import io.shiftleft.overflowdb.structure.OdbGraph;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TinkerCountGlobalStep<S extends Element> extends AbstractStep<S, Long> {

  private final Class<S> elementClass;
  private boolean done = false;

  public TinkerCountGlobalStep(final Traversal.Admin traversal, final Class<S> elementClass) {
    super(traversal);
    this.elementClass = elementClass;
  }

  @Override
  protected Traverser.Admin<Long> processNextStart() throws NoSuchElementException {
    if (!this.done) {
      this.done = true;
      final OdbGraph graph = (OdbGraph) this.getTraversal().getGraph().get();
      final long size;
      if (Vertex.class.isAssignableFrom(this.elementClass)) {
        size = graph.vertexCount();
      } else throw new NotImplementedException("edges only exist virtually. run e.g. `g.V().outE().count()` instead");
      return this.getTraversal().getTraverserGenerator().generate(size, (Step) this, 1L);
    } else
      throw FastNoSuchElementException.instance();
  }

  @Override
  public String toString() {
    return StringFactory.stepString(this, this.elementClass.getSimpleName().toLowerCase());
  }

  @Override
  public int hashCode() {
    return super.hashCode() ^ this.elementClass.hashCode();
  }

  @Override
  public void reset() {
    this.done = false;
  }
}
