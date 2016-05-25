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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.sql.planner.ExpressionNodeInliner.replaceExpression;
import static com.facebook.presto.sql.util.AstUtils.nodeContains;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableMap;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

class SubqueryPlanner
{
    private final Analysis analysis;
    private final SymbolAllocator symbolAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Metadata metadata;
    private final Session session;

    SubqueryPlanner(Analysis analysis, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, Metadata metadata, Session session)
    {
        requireNonNull(analysis, "analysis is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(session, "session is null");

        this.analysis = analysis;
        this.symbolAllocator = symbolAllocator;
        this.idAllocator = idAllocator;
        this.metadata = metadata;
        this.session = session;
    }

    public PlanBuilder handleSubqueries(PlanBuilder builder, Collection<Expression> expressions, Node node)
    {
        for (Expression expression : expressions) {
            builder = handleSubqueries(builder, expression, node);
        }
        return builder;
    }

    public PlanBuilder handleSubqueries(PlanBuilder builder, Expression expression, Node node)
    {
        builder = appendInPredicateApplyNodes(
                builder,
                analysis.getInPredicateSubqueries(node)
                        .stream()
                        .filter(inPredicate -> nodeContains(expression, inPredicate.getValueList()))
                        .collect(toImmutableSet()));
        builder = appendScalarSubqueryApplyNodes(
                builder,
                analysis.getScalarSubqueries(node)
                        .stream()
                        .filter(subquery -> nodeContains(expression, subquery))
                        .collect(toImmutableSet()));
        return builder;
    }

    private PlanBuilder appendInPredicateApplyNodes(PlanBuilder subPlan, Set<InPredicate> inPredicates)
    {
        for (InPredicate inPredicate : inPredicates) {
            subPlan = appendInPredicateApplyNode(subPlan, inPredicate);
        }
        return subPlan;
    }

    private PlanBuilder appendInPredicateApplyNode(PlanBuilder subPlan, InPredicate inPredicate)
    {
        subPlan = subPlan.appendProjections(ImmutableList.of(inPredicate.getValue()), symbolAllocator, idAllocator);

        checkState(inPredicate.getValueList() instanceof SubqueryExpression);
        PlanNode subquery = createRelationPlan((SubqueryExpression) inPredicate.getValueList()).getRoot();
        Map<Expression, Symbol> correlation = extractCorrelation(subPlan, subquery);
        subPlan = subPlan.appendProjections(correlation.keySet(), symbolAllocator, idAllocator);
        subquery = replaceExpressionsWithSymbols(subquery, correlation);

        TranslationMap translationMap = subPlan.copyTranslations();
        QualifiedNameReference valueList = getOnlyElement(subquery.getOutputSymbols()).toQualifiedNameReference();
        translationMap.setExpressionAsAlreadyTranslated(valueList);
        translationMap.put(inPredicate, new InPredicate(inPredicate.getValue(), valueList));

        return new PlanBuilder(translationMap,
                new ApplyNode(idAllocator.getNextId(),
                        subPlan.getRoot(),
                        subquery,
                        ImmutableList.copyOf(correlation.values())),
                subPlan.getSampleWeight());
    }

    private PlanBuilder appendScalarSubqueryApplyNodes(PlanBuilder builder, Set<SubqueryExpression> scalarSubqueries)
    {
        for (SubqueryExpression scalarSubquery : scalarSubqueries) {
            builder = appendScalarSubqueryApplyNode(builder, scalarSubquery);
        }
        return builder;
    }

    private PlanBuilder appendScalarSubqueryApplyNode(PlanBuilder subPlan, SubqueryExpression scalarSubquery)
    {
        if (subPlan.canTranslate(scalarSubquery)) {
            // given subquery is already appended
            return subPlan;
        }

        PlanNode subquery = new EnforceSingleRowNode(idAllocator.getNextId(), createRelationPlan(scalarSubquery).getRoot());

        Map<Expression, Symbol> correlation = extractCorrelation(subPlan, subquery);
        subPlan = subPlan.appendProjections(correlation.keySet(), symbolAllocator, idAllocator);
        subquery = replaceExpressionsWithSymbols(subquery, correlation);

        TranslationMap translations = subPlan.copyTranslations();
        translations.put(scalarSubquery, getOnlyElement(subquery.getOutputSymbols()));

        PlanNode root = subPlan.getRoot();
        if (root.getOutputSymbols().isEmpty()) {
            // there is nothing to join with - e.g. SELECT (SELECT 1)
            return new PlanBuilder(translations, subquery, subPlan.getSampleWeight());
        }
        else {
            return new PlanBuilder(translations,
                    new ApplyNode(idAllocator.getNextId(),
                            root,
                            subquery,
                            ImmutableList.copyOf(correlation.values())),
                    subPlan.getSampleWeight());
        }
    }

    private Map<Expression, Symbol> extractCorrelation(PlanBuilder subPlan, PlanNode subquery)
    {
        Set<Expression> missingReferences = extractMissingExpressions(subquery);
        ImmutableMap.Builder<Expression, Symbol> correlation = ImmutableMap.builder();
        if (!missingReferences.isEmpty()) {
            for (Expression missingReference : missingReferences) {
                Expression rewritten = subPlan.rewrite(missingReference);
                if (rewritten != missingReference) {
                    correlation.put(missingReference, asSymbol(rewritten));
                }
            }
        }
        return correlation.build();
    }

    private RelationPlan createRelationPlan(SubqueryExpression subqueryExpression)
    {
        return new RelationPlanner(analysis, symbolAllocator, idAllocator, metadata, session)
                .process(subqueryExpression.getQuery(), null);
    }

    private Symbol asSymbol(Expression expression)
    {
        checkState(expression instanceof QualifiedNameReference);
        return Symbol.fromQualifiedName(((QualifiedNameReference) expression).getName());
    }

    private Set<Expression> extractMissingExpressions(PlanNode planNode)
    {
        MissingReferencesExtractor visitor = new MissingReferencesExtractor(analysis);
        planNode.accept(visitor, null);
        return visitor.getMissing();
    }

    private static class MissingReferencesExtractor
            extends PlanVisitor<Void, Void>
    {
        private final Set<Expression> missing = new HashSet<>();
        private final Set<Expression> columnReferences;

        private MissingReferencesExtractor(Analysis analysis)
        {
            columnReferences = analysis.getColumnReferences();
        }

        @Override
        protected Void visitPlan(PlanNode node, Void context)
        {
            node.getSources().forEach(source -> source.accept(this, null));

            for (Symbol symbol : node.getOutputSymbols()) {
                missing.remove(symbol.toQualifiedNameReference());
            }
            return null;
        }

        @Override
        public Void visitProject(ProjectNode node, Void context)
        {
            ImmutableSet<Expression> columnReferences = node.getAssignments().values().stream()
                    .flatMap(expression -> extractColumnReferences(expression, this.columnReferences).stream())
                    .collect(toImmutableSet());
            missing.addAll(columnReferences);
            visitPlan(node, context);
            return null;
        }

        public Set<Expression> getMissing()
        {
            return ImmutableSet.copyOf(missing);
        }
    }

    private static Set<Expression> extractColumnReferences(Expression expression, Set<Expression> columnReferences)
    {
        ImmutableSet.Builder<Expression> expressionColumnReferences = ImmutableSet.builder();
        new ColumnReferencesExtractor(columnReferences).process(expression, expressionColumnReferences);
        return expressionColumnReferences.build();
    }

    private static class ColumnReferencesExtractor
            extends DefaultExpressionTraversalVisitor<Void, ImmutableSet.Builder<Expression>>
    {
        private final Set<Expression> columnReferences;

        private ColumnReferencesExtractor(Set<Expression> columnReferences)
        {
            this.columnReferences = requireNonNull(columnReferences, "columnReferences is null");
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, ImmutableSet.Builder<Expression> builder)
        {
            if (columnReferences.contains(node)) {
                builder.add(node);
            }
            else {
                process(node.getBase(), builder);
            }
            return null;
        }

        @Override
        protected Void visitQualifiedNameReference(QualifiedNameReference node, ImmutableSet.Builder<Expression> builder)
        {
            builder.add(node);
            return null;
        }
    }

    private PlanNode replaceExpressionsWithSymbols(PlanNode planNode, Map<Expression, Symbol> mapping)
    {
        if (mapping.isEmpty()) {
            return planNode;
        }

        Map<Expression, Expression> expressionMapping = mapping.entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, e -> e.getValue().toQualifiedNameReference()));
        return SimplePlanRewriter.rewriteWith(new ExpressionReplacer(idAllocator, expressionMapping), planNode, null);
    }

    private static class ExpressionReplacer
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Map<Expression, Expression> mapping;

        public ExpressionReplacer(PlanNodeIdAllocator idAllocator, Map<Expression, Expression> mapping)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.mapping = requireNonNull(mapping, "mapping is null");
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            ProjectNode rewrittenNode = (ProjectNode) context.defaultRewrite(node);

            ImmutableMap.Builder<Symbol, Expression> assignments = ImmutableMap.builder();
            for (Map.Entry<Symbol, Expression> assignment : rewrittenNode.getAssignments().entrySet()) {
                Expression expression = assignment.getValue();
                Expression rewritten = replaceExpression(expression, mapping);
                assignments.put(assignment.getKey(), rewritten);
            }

            return new ProjectNode(idAllocator.getNextId(), rewrittenNode.getSource(), assignments.build());
        }
    }
}
