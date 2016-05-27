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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class WindowCombineSamePartitionAndOrder
    implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return SimplePlanRewriter.rewriteWith(new Rewriter(), plan, new Context());
    }

    private static class Rewriter
            extends SimplePlanRewriter<Context>
    {
        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<Context> context)
        {
            PartitionAndOrder partitionAndOrder = new PartitionAndOrder(node.getPartitionBy(), node.getOrderBy(), node.getOrderings());
            Context ctx = context.get();
            boolean elideCurrentNode = ctx.isPartitionAndOrderRegistered(partitionAndOrder);
            ctx.addWindowSpecification(partitionAndOrder, new WindowSpecification(node.getFrames()));

            PlanNode rewrittenSource = context.rewrite(node.getSource());

            if (elideCurrentNode) {
                return rewrittenSource;
            }

            WindowSpecification specification = ctx.getWindowSpecification(partitionAndOrder);
            checkState(specification != null, "WindowSpecification cannot be null while processing WindowNode");

            WindowNode rewrittenNode = new WindowNode(node.getId(),
                    rewrittenSource,
                    node.getPartitionBy(),
                    node.getOrderBy(),
                    node.getOrderings(),
                    specification.framesForFunctions,
                    node.getWindowFunctions(),
                    node.getSignatures(),
                    node.getHashSymbol(),
                    node.getPrePartitionedInputs(),
                    node.getPreSortedOrderPrefix()
                    );

            return replaceChildren(rewrittenNode, ImmutableList.of(rewrittenSource));
        }
    }

    private static class Context
    {
        private final Map<PartitionAndOrder, WindowSpecification> windowsToMerge = new HashMap<>();

        public void addWindowSpecification(PartitionAndOrder partitionAndOrder, WindowSpecification sourceSpecification)
        {
            WindowSpecification windowSpecification = windowsToMerge.get(partitionAndOrder);
            if (windowSpecification == null) {
                windowSpecification = new WindowSpecification();
            }
            appendFunctionsWithFrames(sourceSpecification, windowSpecification);
        }

        private void appendFunctionsWithFrames(WindowSpecification sourceSpecification, WindowSpecification targetSpecification)
        {
            for (Map.Entry<Symbol, List<WindowNode.Frame>> entry : sourceSpecification.framesForFunctions.entrySet()) {
                List<WindowNode.Frame> frames = targetSpecification.framesForFunctions.get(entry.getKey());
                if (frames == null) {
                    frames = new ArrayList<>();
                    targetSpecification.framesForFunctions.put(entry.getKey(), frames);
                }
                frames.addAll(entry.getValue());
            }
        }

        public WindowSpecification getWindowSpecification(PartitionAndOrder partitionAndOrder)
        {
            return windowsToMerge.get(partitionAndOrder);
        }

        public boolean isPartitionAndOrderRegistered(PartitionAndOrder partitionAndOrder)
        {
            return windowsToMerge.containsKey(partitionAndOrder);
        }
    }

    private static class PartitionAndOrder
    {
        private final List<Symbol> partitionBy;
        private final List<Symbol> orderBy;
        private final Map<Symbol, SortOrder> orderings;

        public PartitionAndOrder(List<Symbol> partitionBy, List<Symbol> orderBy, Map<Symbol, SortOrder> orderings)
        {
            this.partitionBy = requireNonNull(partitionBy, "partitionBy is null");
            this.orderBy = requireNonNull(orderBy, "orderBy is null");
            this.orderings = requireNonNull(orderings, "orderings is null");
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

            PartitionAndOrder other = (PartitionAndOrder) obj;
            return Objects.equals(partitionBy, other.partitionBy)
                    && Objects.equals(orderBy, other.orderBy)
                    && Objects.equals(orderings, other.orderings);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(partitionBy, orderBy, orderings);
        }
    }

    private static class WindowSpecification
    {
        public final Map<Symbol, List<WindowNode.Frame>> framesForFunctions;

        public WindowSpecification()
        {
            framesForFunctions = new HashMap<>();
        }

        public WindowSpecification(Map<Symbol, List<WindowNode.Frame>> framesForFunctions)
        {
            this.framesForFunctions = requireNonNull(framesForFunctions, "framesForFunctions null");
        }
    }
}
