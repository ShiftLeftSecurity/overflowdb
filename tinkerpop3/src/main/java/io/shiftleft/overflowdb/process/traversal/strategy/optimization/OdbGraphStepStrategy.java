package io.shiftleft.overflowdb.process.traversal.strategy.optimization;

import io.shiftleft.overflowdb.process.traversal.step.sideEffect.OdbGraphStep;
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

public final class OdbGraphStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

  private static final OdbGraphStepStrategy INSTANCE = new OdbGraphStepStrategy();

  private OdbGraphStepStrategy() {
  }

  @Override
  public void apply(final Traversal.Admin<?, ?> traversal) {
    if (TraversalHelper.onGraphComputer(traversal))
      return;

    for (final GraphStep originalGraphStep : TraversalHelper.getStepsOfClass(GraphStep.class, traversal)) {
      final OdbGraphStep<?, ?> odbGraphStep = new OdbGraphStep<>(originalGraphStep);
      TraversalHelper.replaceStep(originalGraphStep, odbGraphStep, traversal);
      Step<?, ?> currentStep = odbGraphStep.getNextStep();
      while (currentStep instanceof HasStep || currentStep instanceof NoOpBarrierStep) {
        if (currentStep instanceof HasStep) {
          for (final HasContainer hasContainer : ((HasContainerHolder) currentStep).getHasContainers()) {
            if (!GraphStep.processHasContainerIds(odbGraphStep, hasContainer))
              odbGraphStep.addHasContainer(hasContainer);
          }
          TraversalHelper.copyLabels(currentStep, currentStep.getPreviousStep(), false);
          traversal.removeStep(currentStep);
        }
        currentStep = currentStep.getNextStep();
      }
    }
  }

  public static OdbGraphStepStrategy instance() {
    return INSTANCE;
  }
}
