/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableMap;
import static com.google.common.base.Preconditions.checkState;

/**
 * Merge together the functions in WindowNodes that have identical WindowNode.Specifications.
 * For example:
 *
 * OutputNode
 * `--...
 *    `--WindowNode(Specification: A, Functions: [sum(something)])
 *       `--WindowNode(Specification: B, Functions: [sum(something)])
 *          `--WindowNode(Specification: A, Functions: [avg(something)])
 *             `--...
 *
 * Will be transformed into
 *
 * OutputNode
 * `--...
 *    `--WindowNode(Specification: A, Functions: [sum(something), avg(something)])
 *       `--WindowNode(Specification: B, Functions: [sum(something)])
 *          `--...
 *
 * This will NOT merge the functions in WindowNodes that have identical WindowNode.Specifications,
 * but have a node between them that is not a WindowNode.
 * In the following example, the functions in the WindowNodes with specification `A' will not be
 * merged into a single WindowNode.
 *
 * OutputNode
 * `--...
 *    `--WindowNode(Specification: A, Functions: [sum(something)])
 *       `--WindowNode(Specification: B, Functions: [sum(something)])
 *          `-- ProjectNode(...)
 *             `--WindowNode(Specification: A, Functions: [avg(something)])
 *                `--...
 */
public class MergeIdenticalWindows
        implements PlanOptimizer
{
    public PlanNode optimize(PlanNode plan,
            Session session,
            Map<Symbol, Type> types,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator)
    {
        return SimplePlanRewriter.rewriteWith(new Rewriter(), plan, new Context());
    }

    private static class Rewriter
            extends SimplePlanRewriter<MergeIdenticalWindows.Context>
    {
        @Override
        public PlanNode visitWindow(
                WindowNode node,
                RewriteContext<MergeIdenticalWindows.Context> context)
        {
            checkState(!node.getHashSymbol().isPresent(),
                    "MergeIdenticalWindows should be run before HashGenerationOptimizer");
            checkState(node.getPrePartitionedInputs().isEmpty() && node.getPreSortedOrderPrefix() == 0,
                    "MergeIdenticalWindows should be run before AddExchanges");
            checkState(node.getFrames().values().size() == 1, "More than one frame per WindowNode is not yet supported");

            Context mergeContext = context.get();
            boolean retainNode = mergeContext.collectFunctions(node);

            // Don't merge window functions across nodes that aren't WindowNodes
            if (node.getSource() instanceof WindowNode) {
                node = (WindowNode) context.defaultRewrite(node, mergeContext);
            }
            else {
                node = (WindowNode) context.defaultRewrite(node, new Context());
            }

            // WindowFunctions contained in this node will be merged in some node further up the tree.
            if (!retainNode) {
                return node.getSource();
            }

            Map<Symbol, WindowNode.Function> collectedFunctions = mergeContext.getCollectedFunctions(node).build();

            WindowNode.Frame frame = node.getFrames().values().iterator().next();

            return new WindowNode(
                    node.getId(),
                    node.getSource(),
                    node.getSpecification(),
                    collectedFunctions,
                    collectedFunctions.values().stream().collect(toImmutableMap(Function.identity(), ignore -> frame)),
                    node.getHashSymbol(),
                    node.getPrePartitionedInputs(),
                    node.getPreSortedOrderPrefix());
        }
    }

    private static final class Context
    {
        private static final class SpecificationAndFrame
        {
            private final WindowNode.Specification specification;
            private final WindowNode.Frame frame;

            SpecificationAndFrame(WindowNode.Specification specification, WindowNode.Frame frame)
            {
                this.specification = specification;
                this.frame = frame;
            }

            SpecificationAndFrame(WindowNode node)
            {
                this(node.getSpecification(),
                        node.getFrames().values().iterator().next()); // asserted in #visitWindow already
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(specification, frame);
            }

            @Override
            public boolean equals(Object obj)
            {
                if (this == obj) {
                    return true;
                }
                if (obj == null || getClass() != obj.getClass()) {
                    return false;
                }
                SpecificationAndFrame other = (SpecificationAndFrame) obj;
                return Objects.equals(this.specification, other.specification) &&
                        Objects.equals(this.frame, other.frame);
            }
        }

        private Map<SpecificationAndFrame, ImmutableMap.Builder<Symbol, WindowNode.Function>> functions = new HashMap<>();

        private boolean collectFunctions(WindowNode window)
        {
            SpecificationAndFrame specificationAndFrame = new SpecificationAndFrame(window);
            ImmutableMap.Builder<Symbol, WindowNode.Function> builder = functions.get(specificationAndFrame);

            if (builder == null) {
                builder = ImmutableMap.builder();
            }

            builder.putAll(window.getWindowFunctions());
            return functions.put(specificationAndFrame, builder) == null;
        }

        private ImmutableMap.Builder<Symbol, WindowNode.Function> getCollectedFunctions(WindowNode window)
        {
            SpecificationAndFrame key = new SpecificationAndFrame(window);
            checkState(
                    functions.containsKey(key),
                    "No pair for window specification. Tried to merge the same window twice?");
            return functions.get(key);
        }
    }
}
