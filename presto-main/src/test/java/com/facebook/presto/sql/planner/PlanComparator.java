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

import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.WindowNode;

import java.util.Iterator;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

public class PlanComparator
        extends PlanVisitor<PlanNode, Boolean>
{
    public Boolean visitTableScan(TableScanNode node, PlanNode context)
    {
        checkArgument(context instanceof TableScanNode, "Expected node of type %s, %s found", node.getClass().getName(), context.getClass().getName());
        TableScanNode other = (TableScanNode) context;

        boolean fieldsEqual = Objects.equals(node.getId(), other.getId())
                && Objects.equals(node.getTable(), other.getTable())
                && Objects.equals(node.getOutputSymbols(), other.getOutputSymbols())
                && Objects.equals(node.getAssignments(), other.getAssignments())
                && Objects.equals(node.getLayout(), other.getLayout())
                && Objects.equals(node.getCurrentConstraint(), other.getCurrentConstraint())
                && Objects.equals(node.getOriginalConstraint(), other.getOriginalConstraint());
        if (!fieldsEqual) {
            return false;
        }

        Iterator<PlanNode> firstSources = node.getSources().iterator();
        Iterator<PlanNode> secondSources = other.getSources().iterator();
        if (node.getSources().size() != other.getSources().size()) {
            return false;
        }

        while (firstSources.hasNext() && secondSources.hasNext()) {
            if (firstSources.next().accept(this, secondSources.next()) == false) {
                return false;
            }
        }
        return true;
    }

    public Boolean visitWindow(WindowNode node, PlanNode context)
    {
        checkArgument(context instanceof WindowNode, "Expected node of type %s, %s found", node.getClass().getName(), context.getClass().getName());
        WindowNode other = (WindowNode) context;

        return Objects.equals(node.getId(), other.getId())
                && Objects.equals(node.getPartitionBy(), other.getPartitionBy())
                && Objects.equals(node.getOrderBy(), other.getOrderBy())
                && Objects.equals(node.getOrderings(), other.getOrderings())
                && Objects.equals(node.getFrame(), other.getFrame())
                && Objects.equals(node.getWindowFunctions(), other.getWindowFunctions())
                && Objects.equals(node.getHashSymbol(), other.getHashSymbol())
                && Objects.equals(node.getPrePartitionedInputs(), other.getPrePartitionedInputs())
                && Objects.equals(node.getPreSortedOrderPrefix(), other.getPreSortedOrderPrefix())
                && node.getSource().accept(this, other.getSource());
    }

    public Boolean visitOutput(OutputNode node, PlanNode context)
    {
        checkArgument(context instanceof OutputNode, "Expected node of type %s, %s found", node.getClass().getName(), context.getClass().getName());
        OutputNode other = (OutputNode) context;

        return Objects.equals(node.getId(), other.getId())
                && Objects.equals(node.getColumnNames(), other.getColumnNames())
                && Objects.equals(node.getOutputSymbols(), other.getOutputSymbols())
                && node.getSource().accept(this, other.getSource());
    }
}
