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

import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.WindowFrame;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class WindowCombineSamePartitionAndOrderTest
{
    private static final TableHandle DUAL_TABLE_HANDLE = new TableHandle("test", new TestingTableHandle());
    private static final Symbol A = new Symbol("a");
    private static final Symbol B = new Symbol("b");

    private PlanOptimizer optimizer;
    private Map<Symbol, ColumnHandle> scanAssignments;
    private TableScanNode baseTableScan;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        optimizer = new WindowCombineSamePartitionAndOrder();
        scanAssignments = ImmutableMap.of(
                A, new TestingColumnHandle("a"),
                B, new TestingColumnHandle("b"));

        baseTableScan = new TableScanNode(
                newId(),
                DUAL_TABLE_HANDLE,
                ImmutableList.copyOf(scanAssignments.keySet()),
                scanAssignments,
                Optional.empty(),
                TupleDomain.all(),
                null
        );
    }

    private static PlanNodeId newId()
    {
        return new PlanNodeId(UUID.randomUUID().toString());
    }

    @Test
    public void planWithoutWindowOperatorsIsLeftUntouched()
            throws Exception
    {
        PlanNode planNode = createPlanWithoutWindowOperators();

        PlanNode actualNode = optimizer.optimize(planNode, testSessionBuilder().build(), ImmutableMap.of(), null, null);
        assertPlansEqual(actualNode, planNode);
    }

    private static void assertPlansEqual(PlanNode actual, PlanNode expected)
    {
        PlanComparator planComparator = new PlanComparator();
        if (expected.accept(planComparator, actual) == false) {
            throw new RuntimeException("Plans don't match");
        }
    }

    private PlanNode createPlanWithoutWindowOperators()
    {
        return new OutputNode(newId(),
                baseTableScan,
                ImmutableList.of(A.getName()),
                ImmutableList.of(A));
    }

    @Test
    public void planWithSingleWindowOperatorIsLeftUntouched()
            throws Exception
    {
        PlanNode planNode = createPlanWithSingleWindowOperator(1);

        PlanNode actualNode = optimizer.optimize(planNode, testSessionBuilder().build(), ImmutableMap.of(), null, null);
        assertPlansEqual(actualNode, planNode);
    }

    private PlanNode createPlanWithSingleWindowOperator(int startingId)
    {
        PlanNode windowNode = new WindowNode(id(startingId),
                baseTableScan,
                ImmutableList.of(A),
                ImmutableList.of(A),
                ImmutableMap.of(A, SortOrder.ASC_NULLS_FIRST),
                new WindowNode.Frame(WindowFrame.Type.RANGE,
                        FrameBound.Type.UNBOUNDED_PRECEDING, Optional.empty(),
                        FrameBound.Type.CURRENT_ROW, Optional.empty()),
                ImmutableMap.of(),
                ImmutableMap.of(),
                Optional.empty(),
                ImmutableSet.of(),
                0);
        return new OutputNode(id(startingId + 1),
                windowNode,
                ImmutableList.of(A.getName()),
                ImmutableList.of(A));
    }

    @Test
    public void twoAdjacentIdenticalWindowOperatorsAreMerged()
            throws Exception
    {
        PlanNode planNode = createPlanWithAdjacentIdenticalWindowOperators();

        PlanNode expectedPlan = createPlanWithSingleWindowOperator(2);

        PlanNode actualNode = optimizer.optimize(planNode, testSessionBuilder().build(), ImmutableMap.of(), null, null);
        assertPlansEqual(actualNode, expectedPlan);
    }

    private PlanNode createPlanWithAdjacentIdenticalWindowOperators()
    {
        PlanNode windowNode = new WindowNode(id(1),
                baseTableScan,
                ImmutableList.of(A),
                ImmutableList.of(A),
                ImmutableMap.of(A, SortOrder.ASC_NULLS_FIRST),
                new WindowNode.Frame(WindowFrame.Type.RANGE,
                        FrameBound.Type.UNBOUNDED_PRECEDING, Optional.empty(),
                        FrameBound.Type.CURRENT_ROW, Optional.empty()),
                ImmutableMap.of(),
                ImmutableMap.of(),
                Optional.empty(),
                ImmutableSet.of(),
                0);
        PlanNode secondWindowNode = new WindowNode(id(2),
                windowNode,
                ImmutableList.of(A),
                ImmutableList.of(A),
                ImmutableMap.of(A, SortOrder.ASC_NULLS_FIRST),
                new WindowNode.Frame(WindowFrame.Type.RANGE,
                        FrameBound.Type.UNBOUNDED_PRECEDING, Optional.empty(),
                        FrameBound.Type.CURRENT_ROW, Optional.empty()),
                ImmutableMap.of(),
                ImmutableMap.of(),
                Optional.empty(),
                ImmutableSet.of(),
                0);
        return new OutputNode(id(3),
                secondWindowNode,
                ImmutableList.of(A.getName()),
                ImmutableList.of(A));
    }

    @Test
    public void planWithTwoAdjacentWindowOperatorsWithDifferentPartitionIsLeftUntouched()
            throws Exception
    {
        PlanNode planNode = createPlanWithAdjacentWindowOperatorsWithDifferentPartition();

        PlanNode actualNode = optimizer.optimize(planNode, testSessionBuilder().build(), ImmutableMap.of(), null, null);
        assertPlansEqual(actualNode, planNode);
    }

    private PlanNode createPlanWithAdjacentWindowOperatorsWithDifferentPartition()
    {
        PlanNode windowNode = new WindowNode(id(1),
                baseTableScan,
                ImmutableList.of(A),
                ImmutableList.of(A),
                ImmutableMap.of(A, SortOrder.ASC_NULLS_FIRST),
                new WindowNode.Frame(WindowFrame.Type.RANGE,
                        FrameBound.Type.UNBOUNDED_PRECEDING, Optional.empty(),
                        FrameBound.Type.CURRENT_ROW, Optional.empty()),
                ImmutableMap.of(),
                ImmutableMap.of(),
                Optional.empty(),
                ImmutableSet.of(),
                0);
        PlanNode secondWindowNode = new WindowNode(id(2),
                windowNode,
                ImmutableList.of(B),
                ImmutableList.of(A),
                ImmutableMap.of(A, SortOrder.ASC_NULLS_FIRST),
                new WindowNode.Frame(WindowFrame.Type.RANGE,
                        FrameBound.Type.UNBOUNDED_PRECEDING, Optional.empty(),
                        FrameBound.Type.CURRENT_ROW, Optional.empty()),
                ImmutableMap.of(),
                ImmutableMap.of(),
                Optional.empty(),
                ImmutableSet.of(),
                0);
        return new OutputNode(id(3),
                secondWindowNode,
                ImmutableList.of(A.getName()),
                ImmutableList.of(A));
    }

    private static PlanNodeId id(int i)
    {
        return new PlanNodeId(new Integer(i).toString());
    }

//    @Test
//    public void twoAdjacentWindowsWithTheSamePartitionAndOrderAreMerged()
//            throws Exception
//    {
//        PlanNode planNode = createPlanWithAdjacentWindowOperatorsOfTheSamePartitionAndOrder();
//
//        PlanNode expectedPlan = createPlanWithSingleWindowOperator();
//
//        PlanNode actualNode = optimizer.optimize(planNode, testSessionBuilder().build(), ImmutableMap.of(), null, null);
//        assertPlansEqual(actualNode, expectedPlan);
//    }
//
//    private PlanNode createPlanWithAdjacentWindowOperatorsOfTheSamePartitionAndOrder()
//    {
//        PlanNode windowNode = new WindowNode(newId(),
//                baseTableScan,
//                ImmutableList.of(A),
//                ImmutableList.of(A),
//                ImmutableMap.of(A, SortOrder.ASC_NULLS_FIRST),
//                new WindowNode.Frame(WindowFrame.Type.RANGE,
//                        FrameBound.Type.UNBOUNDED_PRECEDING, Optional.empty(),
//                        FrameBound.Type.CURRENT_ROW, Optional.empty()),
//                ImmutableMap.of(),
//                ImmutableMap.of(),
//                Optional.empty(),
//                ImmutableSet.of(),
//                0);
//        PlanNode secondWindowNode = new WindowNode(newId(),
//                windowNode,
//                ImmutableList.of(A),
//                ImmutableList.of(A),
//                ImmutableMap.of(A, SortOrder.ASC_NULLS_FIRST),
//                new WindowNode.Frame(WindowFrame.Type.RANGE,
//                        FrameBound.Type.CURRENT_ROW, Optional.empty(),
//                        FrameBound.Type.FOLLOWING, Optional.empty()),
//                ImmutableMap.of(),
//                ImmutableMap.of(),
//                Optional.empty(),
//                ImmutableSet.of(),
//                0);
//        return new OutputNode(newId(),
//                secondWindowNode,
//                ImmutableList.of(A.getName()),
//                ImmutableList.of(A));
//    }
}
