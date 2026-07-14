package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.NioStep;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The mirroring tax, collected by the build instead of by a user.
 *
 * <p>Every method of NioFlow / NioStep / Lane must be re-declared covariantly on
 * its reactive counterpart. Miss one and nothing breaks loudly: the chain simply
 * falls back to the base return type mid-way and silently loses its reactive
 * steps. So the rule is checked here — add a step to core, forget the mirror,
 * and the build goes red naming the method.
 */
class ReactiveMirrorTest {

    @Test
    void reactiveFlowOverridesEveryNioFlowMethod() {
        assertMirrors(NioFlow.class, ReactiveFlow.class);
    }

    @Test
    void reactiveStepOverridesEveryNioStepMethod() {
        assertMirrors(NioStep.class, ReactiveStep.class);
    }

    @Test
    void reactiveLaneOverridesEveryLaneMethod() {
        assertMirrors(Lane.class, ReactiveLane.class);
    }

    /**
     * The other half of the tax, and the one the covariance check has nothing to
     * say about: the REACTIVE-ONLY steps must exist on the lane too.
     *
     * <p>They went out of sync exactly once already — ReactiveStep grew
     * {@code adaptMono(call, budget)} and ReactiveLane did not, so a remote call
     * inside a when()/match() branch or a fork body could not take a budget at
     * all. A pipeline's steps and a lane's steps are the same steps; the only
     * ones that legitimately live on the step alone are the terminal and the
     * builders that re-type OUT of the lane's world.
     */
    @Test
    void everyReactiveStepAlsoExistsOnTheLane() {
        List<String> missing = new ArrayList<>();
        for (Method method : ReactiveStep.class.getDeclaredMethods()) {
            if (method.isSynthetic() || !isReactiveOnly(method) || STEP_ONLY.contains(method.getName())) {
                continue;
            }
            try {
                ReactiveLane.class.getDeclaredMethod(method.getName(), method.getParameterTypes());
            } catch (NoSuchMethodException absent) {
                missing.add(signature(method) + " — on ReactiveStep but not on ReactiveLane, so the step is"
                        + " unreachable inside a when()/match() branch and inside a fork body");
            }
        }
        assertTrue(missing.isEmpty(), "ReactiveStep and ReactiveLane have drifted apart:\n  "
                + String.join("\n  ", missing));
    }

    // executeMono is the terminal (a lane has no terminal — it is not an execution).
    private static final Set<String> STEP_ONLY = Set.of("executeMono");

    /** A step core does not have: the ones this facade adds, and must add everywhere. */
    private static boolean isReactiveOnly(Method method) {
        try {
            NioStep.class.getDeclaredMethod(method.getName(), method.getParameterTypes());
            return false;
        } catch (NoSuchMethodException reactiveOnly) {
            return true;
        }
    }

    /**
     * A covariant override is one the mirror re-declares with the same name and
     * parameter types, whose return type is a STRICT subtype of the base one.
     * Terminals that must not change (execute, executeAsync, executeResult)
     * return types outside the facade hierarchy, so they are exempt.
     */
    private static void assertMirrors(Class<?> base, Class<?> mirror) {
        List<String> missing = new ArrayList<>();
        for (Method method : base.getDeclaredMethods()) {
            if (method.isSynthetic()) {
                continue;
            }
            if (!isBuilder(method.getReturnType())) {
                continue;   // a terminal (execute, executeAsync, executeResult): nothing to narrow
            }
            Method override;
            try {
                override = mirror.getDeclaredMethod(method.getName(), method.getParameterTypes());
            } catch (NoSuchMethodException absent) {
                missing.add(signature(method) + " — not re-declared on " + mirror.getSimpleName()
                        + ", so the chain falls back to " + method.getReturnType().getSimpleName()
                        + " and silently loses its reactive steps");
                continue;
            }
            boolean narrowed = method.getReturnType().isAssignableFrom(override.getReturnType())
                    && !override.getReturnType().equals(method.getReturnType());
            if (!narrowed) {
                missing.add(signature(method) + " — re-declared but still returns "
                        + method.getReturnType().getSimpleName() + " (the chain would lose its reactive type)");
            }
        }
        assertTrue(missing.isEmpty(),
                mirror.getSimpleName() + " does not fully mirror " + base.getSimpleName() + ":\n  "
                        + String.join("\n  ", missing));
    }

    // The builders — the types a chain flows through, and therefore the ones the
    // mirror must narrow. A terminal returning T, a CompletableFuture or a
    // FlowResult has nothing to narrow.
    private static final Set<String> BUILDERS = Set.of(
            "NioFlow", "NioStep", "Lane",
            "Condition", "Branch", "Cases",
            "StepCondition", "StepBranch", "StepCases",
            "LaneCondition", "LaneBranch", "LaneCases");

    private static boolean isBuilder(Class<?> type) {
        return BUILDERS.contains(type.getSimpleName());
    }

    private static String signature(Method method) {
        return method.getName() + Arrays.toString(method.getParameterTypes());
    }
}
