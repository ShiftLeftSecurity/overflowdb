package overflowdb.tinkerpop;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphVariableHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GraphVariables implements Graph.Variables {

  private final Map<String, Object> variables = new ConcurrentHashMap<>();

  public GraphVariables() {

  }

  @Override
  public Set<String> keys() {
    return this.variables.keySet();
  }

  @Override
  public <R> Optional<R> get(final String key) {
    return Optional.ofNullable((R) this.variables.get(key));
  }

  @Override
  public void remove(final String key) {
    this.variables.remove(key);
  }

  @Override
  public void set(final String key, final Object value) {
    GraphVariableHelper.validateVariable(key, value);
    this.variables.put(key, value);
  }

  public String toString() {
    return StringFactory.graphVariablesString(this);
  }
}
