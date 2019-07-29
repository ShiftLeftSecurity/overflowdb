package io.shiftleft.overflowdb.process.traversal.strategy.optimization;

import io.shiftleft.overflowdb.process.traversal.step.sideEffect.OverflowDbGraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

public final class OverflowDbGraphStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

  private static final OverflowDbGraphStepStrategy INSTANCE = new OverflowDbGraphStepStrategy();

  private OverflowDbGraphStepStrategy() {
  }

  @Override
  public void apply(final Traversal.Admin<?, ?> traversal) {
    if (TraversalHelper.onGraphComputer(traversal))
      return;

    for (final GraphStep originalGraphStep : TraversalHelper.getStepsOfClass(GraphStep.class, traversal)) {
      final OverflowDbGraphStep<?, ?> overflowDbGraphStep = new OverflowDbGraphStep<>(originalGraphStep);
      TraversalHelper.replaceStep(originalGraphStep, overflowDbGraphStep, traversal);
      Step<?, ?> currentStep = overflowDbGraphStep.getNextStep();
      while (currentStep instanceof HasStep || currentStep instanceof NoOpBarrierStep) {
        if (currentStep instanceof HasStep) {
          for (final HasContainer hasContainer : ((HasContainerHolder) currentStep).getHasContainers()) {
            if (!GraphStep.processHasContainerIds(overflowDbGraphStep, hasContainer))
              overflowDbGraphStep.addHasContainer(hasContainer);
          }
          TraversalHelper.copyLabels(currentStep, currentStep.getPreviousStep(), false);
          traversal.removeStep(currentStep);
        }
        currentStep = currentStep.getNextStep();
      }
    }
  }

  public static OverflowDbGraphStepStrategy instance() {
    return INSTANCE;
  }
}
