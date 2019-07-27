package io.shiftleft.overflowdb.process.traversal.step.sideEffect;

import io.shiftleft.overflowdb.structure.OverflowDb;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;

public final class OverflowDbGraphStep<S, E extends Element> extends GraphStep<S, E> implements HasContainerHolder {

    private final List<HasContainer> hasContainers = new ArrayList<>();

    public OverflowDbGraphStep(final GraphStep<S, E> originalGraphStep) {
        super(originalGraphStep.getTraversal(), originalGraphStep.getReturnClass(), originalGraphStep.isStartStep(), originalGraphStep.getIds());
        originalGraphStep.getLabels().forEach(this::addLabel);

        // we used to only setIteratorSupplier() if there were no ids OR the first id was instanceof Element,
        // but that allowed the filter in g.V(v).has('k','v') to be ignored.  this created problems for
        // PartitionStrategy which wants to prevent someone from passing "v" from one TraversalSource to
        // another TraversalSource using a different partition
        this.setIteratorSupplier(() -> (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
    }

    private Iterator<? extends Edge> edges() {
        final OverflowDb graph = (OverflowDb) this.getTraversal().getGraph().get();
        final Optional<HasContainer> hasLabelContainer = findHasLabelStep();
        // ids are present, filter on them first
        if (null == this.ids)
            return Collections.emptyIterator();
        else if (this.ids.length > 0)
            return this.iteratorList(graph.edges(this.ids));
        else
            return this.iteratorList(graph.edges());
    }

    private Iterator<? extends Vertex> vertices() {
        final OverflowDb graph = (OverflowDb) this.getTraversal().getGraph().get();
        final HasContainer indexedContainer = getIndexKey(Vertex.class);
        final Optional<HasContainer> hasLabelContainer = findHasLabelStep();
        // ids are present, filter on them first
        if (null == this.ids)
            return Collections.emptyIterator();
        else if (this.ids.length > 0)
            return this.iteratorList(graph.vertices(this.ids));
        else if (hasLabelContainer.isPresent())
            return graph.verticesByLabel((P<String>) hasLabelContainer.get().getPredicate());
        else {
            if (indexedContainer == null) return this.iteratorList(graph.vertices());
            else throw new NotImplementedException("");
        }
//            return null == indexedContainer ?
//                    this.iteratorList(graph.vertices()) :
//                      throw new NotImplementedException("");
//                    IteratorUtils.filter(TinkerHelper.queryVertexIndex(graph, indexedContainer.getKey(), indexedContainer.getPredicate().getValue()).iterator(),
//                            vertex -> HasContainer.testAll(vertex, this.hasContainers));
    }

    // only optimize if hasLabel is the _only_ hasContainer, since that's the simplest case
    // TODO implement other cases as well, e.g. for `g.V.hasLabel(lbl).has(k,v)`
    private Optional<HasContainer> findHasLabelStep() {
        if (hasContainers.size() == 1) {
            if (T.label.getAccessor().equals(hasContainers.get(0).getKey())) {
                return Optional.of(hasContainers.get(0));
            }
        }
        return Optional.empty();
    }

    private HasContainer getIndexKey(final Class<? extends Element> indexedClass) {
        final Set<String> indexedKeys = ((OverflowDb) this.getTraversal().getGraph().get()).getIndexedKeys(indexedClass);

        final Iterator<HasContainer> itty = IteratorUtils.filter(hasContainers.iterator(),
                c -> c.getPredicate().getBiPredicate() == Compare.eq && indexedKeys.contains(c.getKey()));
        return itty.hasNext() ? itty.next() : null;
    }

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty())
            return super.toString();
        else
            return 0 == this.ids.length ?
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), this.hasContainers) :
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), Arrays.toString(this.ids), this.hasContainers);
    }

    private <E extends Element> Iterator<E> iteratorList(final Iterator<E> iterator) {
        final List<E> list = new ArrayList<>();
        while (iterator.hasNext()) {
            final E e = iterator.next();
            if (HasContainer.testAll(e, this.hasContainers))
                list.add(e);
        }
        return list.iterator();
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public void addHasContainer(final HasContainer hasContainer) {
        if (hasContainer.getPredicate() instanceof AndP) {
            for (final P<?> predicate : ((AndP<?>) hasContainer.getPredicate()).getPredicates()) {
                this.addHasContainer(new HasContainer(hasContainer.getKey(), predicate));
            }
        } else
            this.hasContainers.add(hasContainer);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.hasContainers.hashCode();
    }
}
