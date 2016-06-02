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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.security.ViewAccessControl;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.NoOpSymbolResolver;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.optimizations.CanonicalizeExpressions;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Delete;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Except;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.ExplainType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FieldReference;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Intersect;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.JoinUsing;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NaturalJoin;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SetOperation;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.Unnest;
import com.facebook.presto.sql.tree.Use;
import com.facebook.presto.sql.tree.Values;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.sql.tree.WindowFrame;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.metadata.FunctionKind.AGGREGATE;
import static com.facebook.presto.metadata.FunctionKind.APPROXIMATE_AGGREGATE;
import static com.facebook.presto.metadata.FunctionKind.WINDOW;
import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.createConstantAnalyzer;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.COLUMN_NAME_NOT_SPECIFIED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.COLUMN_TYPE_UNKNOWN;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_COLUMN_NAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_RELATION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_ORDINAL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_WINDOW_FRAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISMATCHED_COLUMN_ALIASES;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISMATCHED_SET_COLUMN_TYPES;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_CATALOG;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_COLUMN;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_SCHEMA;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MUST_BE_WINDOW_FUNCTION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NESTED_WINDOW;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NON_NUMERIC_SAMPLE_PERCENTAGE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.ORDER_BY_MUST_BE_IN_SELECT;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_ALREADY_EXISTS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.VIEW_ANALYSIS_ERROR;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.VIEW_IS_STALE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.VIEW_PARSE_ERROR;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.WILDCARD_WITHOUT_FROM;
import static com.facebook.presto.sql.planner.ExpressionInterpreter.expressionOptimizer;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.EQUAL;
import static com.facebook.presto.sql.tree.ExplainType.Type.DISTRIBUTED;
import static com.facebook.presto.sql.tree.FrameBound.Type.CURRENT_ROW;
import static com.facebook.presto.sql.tree.FrameBound.Type.FOLLOWING;
import static com.facebook.presto.sql.tree.FrameBound.Type.PRECEDING;
import static com.facebook.presto.sql.tree.FrameBound.Type.UNBOUNDED_FOLLOWING;
import static com.facebook.presto.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static com.facebook.presto.sql.tree.WindowFrame.Type.RANGE;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Iterables.transform;
import static java.util.Objects.requireNonNull;

class StatementAnalyzer
        extends DefaultTraversalVisitor<Scope, Scope>
{
    private final Analysis analysis;
    private final Metadata metadata;
    private final Session session;
    private final boolean experimentalSyntaxEnabled;
    private final SqlParser sqlParser;
    private final AccessControl accessControl;

    public StatementAnalyzer(
            Analysis analysis,
            Metadata metadata,
            SqlParser sqlParser,
            AccessControl accessControl, Session session,
            boolean experimentalSyntaxEnabled)
    {
        this.analysis = requireNonNull(analysis, "analysis is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.session = requireNonNull(session, "session is null");
        this.experimentalSyntaxEnabled = experimentalSyntaxEnabled;
    }

    @Override
    protected Scope visitUse(Use node, Scope scope)
    {
        throw new SemanticException(NOT_SUPPORTED, node, "USE statement is not supported");
    }

    @Override
    protected Scope visitInsert(Insert insert, Scope scope)
    {
        QualifiedObjectName targetTable = createQualifiedObjectName(session, insert, insert.getTarget());
        if (metadata.getView(session, targetTable).isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, insert, "Inserting into views is not supported");
        }

        // analyze the query that creates the data
        Scope queryScope = process(insert.getQuery(), scope);

        analysis.setUpdateType("INSERT");

        // verify the insert destination columns match the query
        Optional<TableHandle> targetTableHandle = metadata.getTableHandle(session, targetTable);
        if (!targetTableHandle.isPresent()) {
            throw new SemanticException(MISSING_TABLE, insert, "Table '%s' does not exist", targetTable);
        }
        accessControl.checkCanInsertIntoTable(session.getRequiredTransactionId(), session.getIdentity(), targetTable);

        TableMetadata tableMetadata = metadata.getTableMetadata(session, targetTableHandle.get());
        List<String> tableColumns = tableMetadata.getVisibleColumnNames();

        List<String> insertColumns;
        if (insert.getColumns().isPresent()) {
            insertColumns = insert.getColumns().get().stream()
                    .map(String::toLowerCase)
                    .collect(toImmutableList());

            Set<String> columnNames = new HashSet<>();
            for (String insertColumn : insertColumns) {
                if (!tableColumns.contains(insertColumn)) {
                    throw new SemanticException(MISSING_COLUMN, insert, "Insert column name does not exist in target table: %s", insertColumn);
                }
                if (!columnNames.add(insertColumn)) {
                    throw new SemanticException(DUPLICATE_COLUMN_NAME, insert, "Insert column name is specified more than once: %s", insertColumn);
                }
            }
        }
        else {
            insertColumns = tableColumns;
        }

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, targetTableHandle.get());
        analysis.setInsert(new Analysis.Insert(
                targetTableHandle.get(),
                insertColumns.stream().map(columnHandles::get).collect(toImmutableList())));

        Iterable<Type> tableTypes = insertColumns.stream()
                .map(insertColumn -> tableMetadata.getColumn(insertColumn).getType())
                .collect(toImmutableList());

        Iterable<Type> queryTypes = transform(queryScope.getRelationType().getVisibleFields(), Field::getType);

        if (!typesMatchForInsert(tableTypes, queryTypes)) {
            throw new SemanticException(MISMATCHED_SET_COLUMN_TYPES, insert, "Insert query has mismatched column types: " +
                    "Table: [" + Joiner.on(", ").join(tableTypes) + "], " +
                    "Query: [" + Joiner.on(", ").join(queryTypes) + "]");
        }

        return createScope(insert, scope, Field.newUnqualified("rows", BIGINT));
    }

    private boolean typesMatchForInsert(Iterable<Type> tableTypes, Iterable<Type> queryTypes)
    {
        if (Iterables.size(tableTypes) != Iterables.size(queryTypes)) {
            return false;
        }

        Iterator<Type> tableTypesIterator = tableTypes.iterator();
        Iterator<Type> queryTypesIterator = queryTypes.iterator();
        while (tableTypesIterator.hasNext()) {
            Type tableType = tableTypesIterator.next();
            Type queryType = queryTypesIterator.next();

            if (!metadata.getTypeManager().canCoerce(queryType, tableType)) {
                return false;
            }
        }

        return true;
    }

    @Override
    protected Scope visitDelete(Delete node, Scope scope)
    {
        Table table = node.getTable();
        QualifiedObjectName tableName = createQualifiedObjectName(session, table, table.getName());
        if (metadata.getView(session, tableName).isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, node, "Deleting from views is not supported");
        }

        // Analyzer checks for select permissions but DELETE has a separate permission, so disable access checks
        // TODO: we shouldn't need to create a new analyzer. The access control should be carried in the context object
        StatementAnalyzer analyzer = new StatementAnalyzer(
                analysis,
                metadata,
                sqlParser,
                new AllowAllAccessControl(),
                session,
                experimentalSyntaxEnabled
        );

        Scope tableScope = analyzer.process(table, scope);
        node.getWhere().ifPresent(where -> analyzer.analyzeWhere(node, tableScope, where));

        analysis.setUpdateType("DELETE");

        accessControl.checkCanDeleteFromTable(session.getRequiredTransactionId(), session.getIdentity(), tableName);

        return createScope(node, scope, Field.newUnqualified("rows", BIGINT));
    }

    @Override
    protected Scope visitCreateTableAsSelect(CreateTableAsSelect node, Scope scope)
    {
        analysis.setUpdateType("CREATE TABLE");

        // turn this into a query that has a new table writer node on top.
        QualifiedObjectName targetTable = createQualifiedObjectName(session, node, node.getName());
        analysis.setCreateTableDestination(targetTable);

        Optional<TableHandle> targetTableHandle = metadata.getTableHandle(session, targetTable);
        if (targetTableHandle.isPresent()) {
            if (node.isNotExists()) {
                analysis.setCreateTableAsSelectNoOp(true);
                return createScope(node, scope, Field.newUnqualified("rows", BIGINT));
            }
            throw new SemanticException(TABLE_ALREADY_EXISTS, node, "Destination table '%s' already exists", targetTable);
        }

        for (Expression expression : node.getProperties().values()) {
            // analyze table property value expressions which must be constant
            createConstantAnalyzer(metadata, session)
                    .analyze(expression, scope);
        }
        analysis.setCreateTableProperties(node.getProperties());

        accessControl.checkCanCreateTable(session.getRequiredTransactionId(), session.getIdentity(), targetTable);

        analysis.setCreateTableAsSelectWithData(node.isWithData());

        // analyze the query that creates the table
        Scope queryScope = process(node.getQuery(), scope);

        validateColumns(node, queryScope.getRelationType());

        return createScope(node, scope, Field.newUnqualified("rows", BIGINT));
    }

    @Override
    protected Scope visitCreateView(CreateView node, Scope scope)
    {
        analysis.setUpdateType("CREATE VIEW");

        // analyze the query that creates the view
        StatementAnalyzer analyzer = new StatementAnalyzer(
                analysis,
                metadata,
                sqlParser,
                new ViewAccessControl(accessControl),
                session,
                experimentalSyntaxEnabled
        );

        Scope queryScope = analyzer.process(node.getQuery(), scope);

        QualifiedObjectName viewName = createQualifiedObjectName(session, node, node.getName());
        accessControl.checkCanCreateView(session.getRequiredTransactionId(), session.getIdentity(), viewName);

        validateColumns(node, queryScope.getRelationType());

        return createScope(node, scope, queryScope.getRelationType());
    }

    private static void validateColumns(Statement node, RelationType descriptor)
    {
        // verify that all column names are specified and unique
        // TODO: collect errors and return them all at once
        Set<String> names = new HashSet<>();
        for (Field field : descriptor.getVisibleFields()) {
            Optional<String> fieldName = field.getName();
            if (!fieldName.isPresent()) {
                throw new SemanticException(COLUMN_NAME_NOT_SPECIFIED, node, "Column name not specified at position %s", descriptor.indexOf(field) + 1);
            }
            if (!names.add(fieldName.get())) {
                throw new SemanticException(DUPLICATE_COLUMN_NAME, node, "Column name '%s' specified more than once", fieldName.get());
            }
            if (field.getType().equals(UNKNOWN)) {
                throw new SemanticException(COLUMN_TYPE_UNKNOWN, node, "Column type is unknown: %s", fieldName.get());
            }
        }
    }

    @Override
    protected Scope visitExplain(Explain node, Scope scope)
            throws SemanticException
    {
        checkState(node.isAnalyze(), "Non analyze explain should be rewritten to Query");
        if (node.getOptions().stream().anyMatch(option -> !option.equals(new ExplainType(DISTRIBUTED)))) {
            throw new SemanticException(NOT_SUPPORTED, node, "EXPLAIN ANALYZE only supports TYPE DISTRIBUTED option");
        }
        process(node.getStatement(), scope);
        analysis.setUpdateType(null);
        return createScope(node, scope, Field.newUnqualified("Query Plan", VARCHAR));
    }

    @Override
    protected Scope visitQuery(Query node, Scope scope)
    {
        Scope withScope = analyzeWith(node, scope);
        boolean approximate = isApproximate(node);
        Scope queryScope = Scope.builder().withParent(withScope).withApproximate(approximate).build();
        Scope queryBodyScope = process(node.getQueryBody(), queryScope);
        analyzeOrderBy(node, queryBodyScope);

        // Input fields == Output fields
        analysis.setOutputExpressions(node, descriptorToFields(queryBodyScope));

        queryScope = Scope.builder().withParent(withScope).withRelationType(queryBodyScope.getRelationType()).withApproximate(approximate).build();
        analysis.setScope(node, queryScope);
        return queryScope;
    }

    private boolean isApproximate(Query node)
    {
        boolean approximate = false;
        if (node.getApproximate().isPresent()) {
            if (!experimentalSyntaxEnabled) {
                throw new SemanticException(NOT_SUPPORTED, node, "approximate queries are not enabled");
            }
            approximate = true;
        }
        return approximate;
    }

    @Override
    protected Scope visitUnnest(Unnest node, Scope scope)
    {
        ImmutableList.Builder<Field> outputFields = ImmutableList.builder();
        for (Expression expression : node.getExpressions()) {
            ExpressionAnalysis expressionAnalysis = analyzeExpression(expression, scope);
            Type expressionType = expressionAnalysis.getType(expression);
            if (expressionType instanceof ArrayType) {
                outputFields.add(Field.newUnqualified(Optional.empty(), ((ArrayType) expressionType).getElementType()));
            }
            else if (expressionType instanceof MapType) {
                outputFields.add(Field.newUnqualified(Optional.empty(), ((MapType) expressionType).getKeyType()));
                outputFields.add(Field.newUnqualified(Optional.empty(), ((MapType) expressionType).getValueType()));
            }
            else {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Cannot unnest type: " + expressionType);
            }
        }
        if (node.isWithOrdinality()) {
            outputFields.add(Field.newUnqualified(Optional.empty(), BIGINT));
        }
        return createScope(node, scope, outputFields.build());
    }

    @Override
    protected Scope visitTable(Table table, Scope scope)
    {
        if (!table.getName().getPrefix().isPresent()) {
            // is this a reference to a WITH query?
            String name = table.getName().getSuffix();

            Optional<WithQuery> withQuery = scope.getNamedQuery(name);
            if (withQuery.isPresent()) {
                Query query = withQuery.get().getQuery();
                analysis.registerNamedQuery(table, query);

                // re-alias the fields with the name assigned to the query in the WITH declaration
                RelationType queryDescriptor = analysis.getOutputDescriptor(query);

                List<Field> fields;
                Optional<List<String>> columnNames = withQuery.get().getColumnNames();
                if (columnNames.isPresent()) {
                    // if columns are explicitly aliased -> WITH cte(alias1, alias2 ...)
                    ImmutableList.Builder<Field> fieldBuilder = ImmutableList.builder();

                    int field = 0;
                    for (String columnName : columnNames.get()) {
                        Field inputField = queryDescriptor.getFieldByIndex(field);
                        fieldBuilder.add(Field.newQualified(
                                QualifiedName.of(name),
                                Optional.of(columnName),
                                inputField.getType(),
                                Field.Kind.VISIBLE));

                        field++;
                    }

                    fields = fieldBuilder.build();
                }
                else {
                    fields = queryDescriptor.getAllFields().stream()
                            .map(field -> Field.newQualified(
                                    QualifiedName.of(name),
                                    field.getName(),
                                    field.getType(),
                                    field.getKind()))
                            .collect(toImmutableList());
                }

                return createScope(table, scope, fields);
            }
        }

        QualifiedObjectName name = createQualifiedObjectName(session, table, table.getName());

        Optional<ViewDefinition> optionalView = metadata.getView(session, name);
        if (optionalView.isPresent()) {
            ViewDefinition view = optionalView.get();

            Query query = parseView(view.getOriginalSql(), name, table);

            analysis.registerNamedQuery(table, query);

            accessControl.checkCanSelectFromView(session.getRequiredTransactionId(), session.getIdentity(), name);
            RelationType descriptor = analyzeView(query, name, view.getCatalog(), view.getSchema(), view.getOwner(), table);

            if (isViewStale(view.getColumns(), descriptor.getVisibleFields())) {
                throw new SemanticException(VIEW_IS_STALE, table, "View '%s' is stale; it must be re-created", name);
            }

            // Derive the type of the view from the stored definition, not from the analysis of the underlying query.
            // This is needed in case the underlying table(s) changed and the query in the view now produces types that
            // are implicitly coercible to the declared view types.
            List<Field> outputFields = view.getColumns().stream()
                    .map(column -> Field.newQualified(
                            QualifiedName.of(name.getObjectName()),
                            Optional.of(column.getName()),
                            column.getType(),
                            Field.Kind.VISIBLE))
                    .collect(toImmutableList());

            analysis.addRelationCoercion(table, outputFields.stream().map(Field::getType).toArray(Type[]::new));

            return createScope(table, scope, outputFields);
        }

        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, name);
        if (!tableHandle.isPresent()) {
            if (!metadata.getCatalogNames().containsKey(name.getCatalogName())) {
                throw new SemanticException(MISSING_CATALOG, table, "Catalog %s does not exist", name.getCatalogName());
            }
            if (!metadata.listSchemaNames(session, name.getCatalogName()).contains(name.getSchemaName())) {
                throw new SemanticException(MISSING_SCHEMA, table, "Schema %s does not exist", name.getSchemaName());
            }
            throw new SemanticException(MISSING_TABLE, table, "Table %s does not exist", name);
        }
        accessControl.checkCanSelectFromTable(session.getRequiredTransactionId(), session.getIdentity(), name);
        TableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle.get());
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle.get());

        // TODO: discover columns lazily based on where they are needed (to support datasources that can't enumerate all tables)
        ImmutableList.Builder<Field> fields = ImmutableList.builder();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            Field.Kind kind = column.isHidden() ? Field.Kind.INTERNAL : Field.Kind.VISIBLE;
            Field field = Field.newQualified(table.getName(), Optional.of(column.getName()), column.getType(), kind);
            fields.add(field);
            ColumnHandle columnHandle = columnHandles.get(column.getName());
            checkArgument(columnHandle != null, "Unknown field %s", field);
            analysis.setColumn(field, columnHandle);
        }

        analysis.registerTable(table, tableHandle.get());

        return createScope(table, scope, fields.build());
    }

    @Override
    protected Scope visitAliasedRelation(AliasedRelation relation, Scope scope)
    {
        Scope relationScope = process(relation.getRelation(), scope);

        // todo this check should be inside of TupleDescriptor.withAlias, but the exception needs the node object
        if (relation.getColumnNames() != null) {
            int totalColumns = relationScope.getRelationType().getVisibleFieldCount();
            if (totalColumns != relation.getColumnNames().size()) {
                throw new SemanticException(MISMATCHED_COLUMN_ALIASES, relation, "Column alias list has %s entries but '%s' has %s columns available", relation.getColumnNames().size(), relation.getAlias(), totalColumns);
            }
        }

        relationScope = relationScope.copyWithPrunedHiddenFields();

        RelationType descriptor = relationScope.getRelationType().withAlias(relation.getAlias(), relation.getColumnNames());
        return createScope(relation, scope, descriptor);
    }

    @Override
    protected Scope visitSampledRelation(SampledRelation relation, Scope scope)
    {
        if (relation.getColumnsToStratifyOn().isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, relation, "STRATIFY ON is not yet implemented");
        }

        if (!DependencyExtractor.extractNames(relation.getSamplePercentage(), analysis.getColumnReferences()).isEmpty()) {
            throw new SemanticException(NON_NUMERIC_SAMPLE_PERCENTAGE, relation.getSamplePercentage(), "Sample percentage cannot contain column references");
        }

        IdentityHashMap<Expression, Type> expressionTypes = getExpressionTypes(session, metadata, sqlParser, ImmutableMap.<Symbol, Type>of(), relation.getSamplePercentage());
        ExpressionInterpreter samplePercentageEval = expressionOptimizer(relation.getSamplePercentage(), metadata, session, expressionTypes);

        Object samplePercentageObject = samplePercentageEval.optimize(symbol -> {
            throw new SemanticException(NON_NUMERIC_SAMPLE_PERCENTAGE, relation.getSamplePercentage(), "Sample percentage cannot contain column references");
        });

        if (!(samplePercentageObject instanceof Number)) {
            throw new SemanticException(NON_NUMERIC_SAMPLE_PERCENTAGE, relation.getSamplePercentage(), "Sample percentage should evaluate to a numeric expression");
        }

        double samplePercentageValue = ((Number) samplePercentageObject).doubleValue();

        if (samplePercentageValue < 0.0) {
            throw new SemanticException(SemanticErrorCode.SAMPLE_PERCENTAGE_OUT_OF_RANGE, relation.getSamplePercentage(), "Sample percentage must be greater than or equal to 0");
        }
        if ((samplePercentageValue > 100.0) && ((relation.getType() != SampledRelation.Type.POISSONIZED) || relation.isRescaled())) {
            throw new SemanticException(SemanticErrorCode.SAMPLE_PERCENTAGE_OUT_OF_RANGE, relation.getSamplePercentage(), "Sample percentage must be less than or equal to 100");
        }

        if (relation.isRescaled() && !experimentalSyntaxEnabled) {
            throw new SemanticException(NOT_SUPPORTED, relation, "Rescaling is not enabled");
        }

        analysis.setSampleRatio(relation, samplePercentageValue / 100);
        Scope relationScope = process(relation.getRelation(), scope);
        return createScope(relation, scope, relationScope.getRelationType());
    }

    @Override
    protected Scope visitTableSubquery(TableSubquery node, Scope scope)
    {
        StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, sqlParser, accessControl, session, experimentalSyntaxEnabled);
        Scope queryScope = analyzer.process(node.getQuery(), scope);
        return createScope(node, scope, queryScope.copyWithPrunedHiddenFields().getRelationType());
    }

    @Override
    protected Scope visitQuerySpecification(QuerySpecification node, Scope scope)
    {
        // TODO: extract candidate names from SELECT, WHERE, HAVING, GROUP BY and ORDER BY expressions
        // to pass down to analyzeFrom

        Scope sourceScope = analyzeFrom(node, scope);

        node.getWhere().ifPresent(where -> analyzeWhere(node, sourceScope, where));

        List<AnalyzedColumn> analyzedColumns = analyzeSelect(node, sourceScope);

        List<List<Expression>> groupByExpressions = analyzeGroupBy(node, sourceScope, analyzedColumns);

        Scope outputScope = computeOutput(node, sourceScope, analyzedColumns, groupByExpressions);

        analyzeOrderBy(node, outputScope, analysis.getOutputExpressions(node));
        analyzeHaving(node, sourceScope);

        analyzeAggregations(node, sourceScope, groupByExpressions, analysis.getOutputExpressions(node), analysis.getColumnReferences());
        analyzeWindowFunctions(node, analysis.getOutputExpressions(node));

        return outputScope;
    }

    private Scope computeOutput(QuerySpecification node, Scope scope, List<AnalyzedColumn> analyzedColumns, List<List<Expression>> groupByExpressions)
    {
        List<Expression> outputExpressions = new ArrayList<>();
        List<Field> outputFields = new ArrayList<>();
        for (AnalyzedColumn analyzedColumn : analyzedColumns) {
            Expression expression = analyzedColumn.getExpression();
            outputExpressions.add(expression);

            Optional<ResolvedField> resolvedField = scope.tryResolveField(expression);
            Optional<String> alias = analyzedColumn.getAlias();
            if (resolvedField.isPresent() && !alias.isPresent()) {
                Field field = resolvedField.get().getField();
                outputFields.add(new Field(field.getRelationAlias(), field.getName(), field.getType(), Field.Kind.VISIBLE));
            }
            else {
                if (!alias.isPresent()) {
                    QualifiedName name = null;
                    if (expression instanceof QualifiedNameReference) {
                        name = ((QualifiedNameReference) expression).getName();
                    }
                    else if (expression instanceof DereferenceExpression) {
                        name = DereferenceExpression.getQualifiedName((DereferenceExpression) expression);
                    }
                    if (name != null) {
                        alias = Optional.of(getLast(name.getOriginalParts()));
                    }
                }

                outputFields.add(Field.newUnqualified(alias, analyzedColumn.getType()));
            }
        }

        List<Expression> accessibleExpressions;
        if (!node.getSelect().isDistinct() && groupByExpressions.isEmpty()) {
            accessibleExpressions = scope.getRelationType().getVisibleFields().stream()
                    .map(Field::getQualifiedName)
                    .filter(Optional::isPresent)
                    .map(name -> new QualifiedNameReference(name.get()))
                    .distinct()
                    .collect(toImmutableList());
        }
        else {
            accessibleExpressions = groupByExpressions.stream()
                    .flatMap(List::stream)
                    .distinct()
                    .collect(toImmutableList());
        }

        for (Expression expression : accessibleExpressions) {
            Optional<ResolvedField> resolvedField = scope.tryResolveField(expression);
            if (resolvedField.isPresent()) {
                boolean isInOutputFields = outputFields.stream()
                        .filter(field -> field.getName().equals(resolvedField.get().getField().getName()))
                        .filter(field -> field.getRelationAlias().equals(resolvedField.get().getField().getRelationAlias()))
                        .findAny()
                        .isPresent();
                if (!outputExpressions.contains(expression) && !isInOutputFields) {
                    outputExpressions.add(expression);
                    Field field = resolvedField.get().getField();
                    outputFields.add(new Field(field.getRelationAlias(), field.getName(), field.getType(), Field.Kind.HIDDEN));
                }
            }
            else if (!outputExpressions.contains(expression)) {
                outputExpressions.add(expression);
                outputFields.add(Field.newUnqualified(Optional.empty(), analysis.getType(expression), Field.Kind.HIDDEN));
            }
        }

        analysis.setOutputExpressions(node, ImmutableList.copyOf(outputExpressions));

        return createScope(node, scope, ImmutableList.copyOf(outputFields));
    }

    @Override
    protected Scope visitSetOperation(SetOperation node, Scope scope)
    {
        checkState(node.getRelations().size() >= 2);

        List<Scope> relationScopes = node.getRelations().stream()
                .map(relation -> process(relation, scope))
                .collect(toImmutableList());

        Type[] outputFieldTypes = relationScopes.get(0).getRelationType().getVisibleFields().stream()
                .map(Field::getType)
                .toArray(Type[]::new);
        for (Scope relationScope : relationScopes) {
            int outputFieldSize = outputFieldTypes.length;
            int descFieldSize = relationScope.getRelationType().getVisibleFields().size();
            String setOperationName = node.getClass().getSimpleName();
            if (outputFieldSize != descFieldSize) {
                throw new SemanticException(MISMATCHED_SET_COLUMN_TYPES,
                        node,
                        "%s query has different number of fields: %d, %d",
                        setOperationName, outputFieldSize, descFieldSize);
            }
            for (int i = 0; i < descFieldSize; i++) {
                Type descFieldType = relationScope.getRelationType().getFieldByIndex(i).getType();
                Optional<Type> commonSuperType = metadata.getTypeManager().getCommonSuperType(outputFieldTypes[i], descFieldType);
                if (!commonSuperType.isPresent()) {
                    throw new SemanticException(TYPE_MISMATCH,
                            node,
                            "column %d in %s query has incompatible types: %s, %s",
                            i, outputFieldTypes[i].getDisplayName(), setOperationName, descFieldType.getDisplayName());
                }
                outputFieldTypes[i] = commonSuperType.get();
            }
        }

        Field[] outputDescriptorFields = new Field[outputFieldTypes.length];
        RelationType firstDescriptor = relationScopes.get(0).getRelationType().withOnlyVisibleFields();
        for (int i = 0; i < outputFieldTypes.length; i++) {
            Field oldField = firstDescriptor.getFieldByIndex(i);
            outputDescriptorFields[i] = new Field(oldField.getRelationAlias(), oldField.getName(), outputFieldTypes[i], oldField.getKind());
        }

        for (int i = 0; i < node.getRelations().size(); i++) {
            Relation relation = node.getRelations().get(i);
            Scope relationScope = relationScopes.get(i);
            int visibleFieldIndex = 0;
            int fieldIndex = 0;
            boolean typeChanged = false;
            RelationType relationType = relationScope.getRelationType();
            Type[] relationFieldTypes = new Type[relationType.getAllFieldCount()];
            for (Field field : relationType.getAllFields()) {
                relationFieldTypes[fieldIndex] = field.getType();
                if (field.isVisible()) {
                    if (!field.getType().equals(outputFieldTypes[visibleFieldIndex])) {
                        typeChanged = true;
                        relationFieldTypes[fieldIndex] = outputFieldTypes[visibleFieldIndex];
                    }
                }
                visibleFieldIndex++;
                fieldIndex++;
            }
            if (typeChanged) {
                analysis.addRelationCoercion(relation, relationFieldTypes);
            }
        }
        return createScope(node, scope, outputDescriptorFields);
    }

    @Override
    protected Scope visitIntersect(Intersect node, Scope scope)
    {
        if (!node.isDistinct()) {
            throw new SemanticException(NOT_SUPPORTED, node, "INTERSECT ALL not yet implemented");
        }

        return visitSetOperation(node, scope);
    }

    @Override
    protected Scope visitExcept(Except node, Scope scope)
    {
        throw new SemanticException(NOT_SUPPORTED, node, "EXCEPT not yet implemented");
    }

    @Override
    protected Scope visitJoin(Join node, Scope scope)
    {
        JoinCriteria criteria = node.getCriteria().orElse(null);
        if (criteria instanceof NaturalJoin) {
            throw new SemanticException(NOT_SUPPORTED, node, "Natural join not supported");
        }

        Scope left = process(node.getLeft(), scope);
        Scope right = process(node.getRight(), left);

        Scope output = createScope(node, scope, left.getRelationType().joinWith(right.getRelationType()));

        if (node.getType() == Join.Type.CROSS || node.getType() == Join.Type.IMPLICIT) {
            return output;
        }

        if (criteria instanceof JoinUsing) {
            // TODO: implement proper "using" semantics with respect to output columns
            List<String> columns = ((JoinUsing) criteria).getColumns();

            List<Expression> expressions = new ArrayList<>();
            for (String column : columns) {
                Expression leftExpression = new QualifiedNameReference(QualifiedName.of(column));
                Expression rightExpression = new QualifiedNameReference(QualifiedName.of(column));

                ExpressionAnalysis leftExpressionAnalysis = analyzeExpression(leftExpression, left);
                ExpressionAnalysis rightExpressionAnalysis = analyzeExpression(rightExpression, right);
                checkState(leftExpressionAnalysis.getSubqueryInPredicates().isEmpty(), "INVARIANT");
                checkState(rightExpressionAnalysis.getSubqueryInPredicates().isEmpty(), "INVARIANT");
                checkState(leftExpressionAnalysis.getScalarSubqueries().isEmpty(), "INVARIANT");
                checkState(rightExpressionAnalysis.getScalarSubqueries().isEmpty(), "INVARIANT");

                addCoercionForJoinCriteria(node, leftExpression, rightExpression);
                expressions.add(new ComparisonExpression(EQUAL, leftExpression, rightExpression));
            }

            analysis.setJoinCriteria(node, ExpressionUtils.and(expressions));
        }
        else if (criteria instanceof JoinOn) {
            Expression expression = ((JoinOn) criteria).getExpression();

            // ensure all names can be resolved, types match, etc (we don't need to record resolved names, subexpression types, etc. because
            // we do it further down when after we determine which subexpressions apply to left vs right tuple)
            ExpressionAnalyzer analyzer = ExpressionAnalyzer.create(analysis, session, metadata, sqlParser, accessControl, experimentalSyntaxEnabled);
            analyzer.analyze(expression, output);

            Analyzer.verifyNoAggregatesOrWindowFunctions(metadata, expression, "JOIN");

            // expressionInterpreter/optimizer only understands a subset of expression types
            // TODO: remove this when the new expression tree is implemented
            Expression canonicalized = CanonicalizeExpressions.canonicalizeExpression(expression);
            analyzer.analyze(canonicalized, output);

            Object optimizedExpression = expressionOptimizer(canonicalized, metadata, session, analyzer.getExpressionTypes()).optimize(NoOpSymbolResolver.INSTANCE);

            if (!(optimizedExpression instanceof Expression) && optimizedExpression instanceof Boolean) {
                // If the JoinOn clause evaluates to a boolean expression, simulate a cross join by adding the relevant redundant expression
                if (optimizedExpression.equals(Boolean.TRUE)) {
                    optimizedExpression = new ComparisonExpression(EQUAL, new LongLiteral("0"), new LongLiteral("0"));
                }
                else {
                    optimizedExpression = new ComparisonExpression(EQUAL, new LongLiteral("0"), new LongLiteral("1"));
                }
            }

            if (!(optimizedExpression instanceof Expression)) {
                throw new SemanticException(TYPE_MISMATCH, node, "Join clause must be a boolean expression");
            }
            // The optimization above may have rewritten the expression tree which breaks all the identity maps, so redo the analysis
            // to re-analyze coercions that might be necessary
            analyzer = ExpressionAnalyzer.create(analysis, session, metadata, sqlParser, accessControl, experimentalSyntaxEnabled);
            analyzer.analyze((Expression) optimizedExpression, output);
            analysis.addCoercions(analyzer.getExpressionCoercions(), analyzer.getTypeOnlyCoercions());

            Set<Expression> postJoinConjuncts = new HashSet<>();
            final Set<Expression> leftExpressions = new HashSet<>();
            final Set<Expression> rightExpressions = new HashSet<>();

            for (Expression conjunct : ExpressionUtils.extractConjuncts((Expression) optimizedExpression)) {
                conjunct = ExpressionUtils.normalize(conjunct);

                // in case of outer join we look for case when conjunct can be resolved using only inner side symbols.
                // In such case it will be pushed to filter node on inner side of join by RelationPlanner
                if (node.getType() == Join.Type.RIGHT) {
                    Set<QualifiedName> names = DependencyExtractor.extractNames(conjunct, analyzer.getColumnReferences());
                    if (names.stream().allMatch(left.getRelationType().canResolvePredicate())) {
                        analyzeExpression(conjunct, left);
                        continue;
                    }
                }
                if (node.getType() == Join.Type.LEFT) {
                    Set<QualifiedName> names = DependencyExtractor.extractNames(conjunct, analyzer.getColumnReferences());
                    if (names.stream().allMatch(right.getRelationType().canResolvePredicate())) {
                        analyzeExpression(conjunct, right);
                        continue;
                    }
                }

                if (conjunct instanceof ComparisonExpression
                        && (((ComparisonExpression) conjunct).getType() == EQUAL || node.getType() == Join.Type.INNER)) {
                    Expression conjunctFirst = ((ComparisonExpression) conjunct).getLeft();
                    Expression conjunctSecond = ((ComparisonExpression) conjunct).getRight();
                    Set<QualifiedName> firstDependencies = DependencyExtractor.extractNames(conjunctFirst, analyzer.getColumnReferences());
                    Set<QualifiedName> secondDependencies = DependencyExtractor.extractNames(conjunctSecond, analyzer.getColumnReferences());

                    Expression leftExpression = null;
                    Expression rightExpression = null;
                    if (firstDependencies.stream().allMatch(left.getRelationType().canResolvePredicate()) && secondDependencies.stream().allMatch(right.getRelationType().canResolvePredicate())) {
                        leftExpression = conjunctFirst;
                        rightExpression = conjunctSecond;
                    }
                    else if (firstDependencies.stream().allMatch(right.getRelationType().canResolvePredicate()) && secondDependencies.stream().allMatch(left.getRelationType().canResolvePredicate())) {
                        leftExpression = conjunctSecond;
                        rightExpression = conjunctFirst;
                    }

                    // expression on each side of comparison operator references only symbols from one side of join.
                    // analyze the clauses to record the types of all subexpressions and resolve names against the left/right underlying tuples
                    if (rightExpression != null) {
                        ExpressionAnalysis leftExpressionAnalysis = analyzeExpression(leftExpression, left);
                        ExpressionAnalysis rightExpressionAnalysis = analyzeExpression(rightExpression, right);
                        leftExpressions.add(leftExpression);
                        rightExpressions.add(rightExpression);
                        analysis.recordSubqueries(node, leftExpressionAnalysis);
                        analysis.recordSubqueries(node, rightExpressionAnalysis);
                    }
                    else {
                        // mixed references to both left and right join relation on one side of comparison operator.
                        // expression will be put in post-join condition; analyze in context of output table.
                        postJoinConjuncts.add(conjunct);
                    }
                }
                else {
                    // non-comparison expression.
                    // expression will be put in post-join condition; analyze in context of output table.
                    postJoinConjuncts.add(conjunct);
                }
            }
            Expression postJoinPredicate = ExpressionUtils.combineConjuncts(postJoinConjuncts);
            analysis.recordSubqueries(node, analyzeExpression(postJoinPredicate, output));
            analysis.setJoinCriteria(node, (Expression) optimizedExpression);
        }
        else {
            throw new UnsupportedOperationException("unsupported join criteria: " + criteria.getClass().getName());
        }

        return output;
    }

    private void addCoercionForJoinCriteria(Join node, Expression leftExpression, Expression rightExpression)
    {
        Type leftType = analysis.getTypeWithCoercions(leftExpression);
        Type rightType = analysis.getTypeWithCoercions(rightExpression);
        Optional<Type> superType = metadata.getTypeManager().getCommonSuperType(leftType, rightType);
        if (!superType.isPresent()) {
            throw new SemanticException(TYPE_MISMATCH, node, "Join criteria has incompatible types: %s, %s", leftType.getDisplayName(), rightType.getDisplayName());
        }
        if (!leftType.equals(superType.get())) {
            analysis.addCoercion(leftExpression, superType.get(), metadata.getTypeManager().isTypeOnlyCoercion(leftType, rightType));
        }
        if (!rightType.equals(superType.get())) {
            analysis.addCoercion(rightExpression, superType.get(), metadata.getTypeManager().isTypeOnlyCoercion(rightType, leftType));
        }
    }

    @Override
    protected Scope visitValues(Values node, Scope scope)
    {
        checkState(node.getRows().size() >= 1);

        // get unique row types
        Set<List<Type>> rowTypes = node.getRows().stream()
                .map(row -> analyzeExpression(row, scope).getType(row))
                .map(type -> {
                    if (type instanceof RowType) {
                        return type.getTypeParameters();
                    }
                    return ImmutableList.of(type);
                })
                .collect(toImmutableSet());

        // determine common super type of the rows
        List<Type> fieldTypes = new ArrayList<>(rowTypes.iterator().next());
        for (List<Type> rowType : rowTypes) {
            for (int i = 0; i < rowType.size(); i++) {
                Type fieldType = rowType.get(i);
                Type superType = fieldTypes.get(i);

                Optional<Type> commonSuperType = metadata.getTypeManager().getCommonSuperType(fieldType, superType);
                if (!commonSuperType.isPresent()) {
                    throw new SemanticException(MISMATCHED_SET_COLUMN_TYPES,
                            node,
                            "Values rows have mismatched types: %s vs %s",
                            Iterables.get(rowTypes, 0),
                            Iterables.get(rowTypes, 1));
                }
                fieldTypes.set(i, commonSuperType.get());
            }
        }

        // add coercions for the rows
        for (Expression row : node.getRows()) {
            if (row instanceof Row) {
                List<Expression> items = ((Row) row).getItems();
                for (int i = 0; i < items.size(); i++) {
                    Type expectedType = fieldTypes.get(i);
                    Expression item = items.get(i);
                    Type actualType = analysis.getType(item);
                    if (!actualType.equals(expectedType)) {
                        analysis.addCoercion(item, expectedType, metadata.getTypeManager().isTypeOnlyCoercion(actualType, expectedType));
                    }
                }
            }
            else {
                Type actualType = analysis.getType(row);
                Type expectedType = fieldTypes.get(0);
                if (!actualType.equals(expectedType)) {
                    analysis.addCoercion(row, expectedType, metadata.getTypeManager().isTypeOnlyCoercion(actualType, expectedType));
                }
            }
        }

        List<Field> fields = fieldTypes.stream()
                .map(valueType -> Field.newUnqualified(Optional.empty(), valueType))
                .collect(toImmutableList());

        return createScope(node, scope, fields);
    }

    private void analyzeWindowFunctions(QuerySpecification node, List<Expression> outputExpressions)
    {
        WindowFunctionExtractor extractor = new WindowFunctionExtractor();

        for (Expression expression : Iterables.concat(outputExpressions)) {
            extractor.process(expression, null);
            new WindowFunctionValidator().process(expression, analysis);
        }

        List<FunctionCall> windowFunctions = extractor.getWindowFunctions();

        for (FunctionCall windowFunction : windowFunctions) {
            Window window = windowFunction.getWindow().get();

            WindowFunctionExtractor nestedExtractor = new WindowFunctionExtractor();
            for (Expression argument : windowFunction.getArguments()) {
                nestedExtractor.process(argument, null);
            }

            for (Expression expression : window.getPartitionBy()) {
                nestedExtractor.process(expression, null);
            }

            for (SortItem sortItem : window.getOrderBy()) {
                nestedExtractor.process(sortItem.getSortKey(), null);
            }

            if (window.getFrame().isPresent()) {
                nestedExtractor.process(window.getFrame().get(), null);
            }

            if (!nestedExtractor.getWindowFunctions().isEmpty()) {
                throw new SemanticException(NESTED_WINDOW, node, "Cannot nest window functions inside window function '%s': %s",
                        windowFunction,
                        extractor.getWindowFunctions());
            }

            if (windowFunction.isDistinct()) {
                throw new SemanticException(NOT_SUPPORTED, node, "DISTINCT in window function parameters not yet supported: %s", windowFunction);
            }

            if (window.getFrame().isPresent()) {
                analyzeWindowFrame(window.getFrame().get());
            }

            List<TypeSignature> argumentTypes = Lists.transform(windowFunction.getArguments(), expression -> analysis.getType(expression).getTypeSignature());

            FunctionKind kind = metadata.getFunctionRegistry().resolveFunction(windowFunction.getName(), argumentTypes, false).getKind();
            if (kind != AGGREGATE && kind != APPROXIMATE_AGGREGATE && kind != WINDOW) {
                throw new SemanticException(MUST_BE_WINDOW_FUNCTION, node, "Not a window function: %s", windowFunction.getName());
            }
        }

        analysis.setWindowFunctions(node, windowFunctions);
    }

    private static void analyzeWindowFrame(WindowFrame frame)
    {
        FrameBound.Type startType = frame.getStart().getType();
        FrameBound.Type endType = frame.getEnd().orElse(new FrameBound(CURRENT_ROW)).getType();

        if (startType == UNBOUNDED_FOLLOWING) {
            throw new SemanticException(INVALID_WINDOW_FRAME, frame, "Window frame start cannot be UNBOUNDED FOLLOWING");
        }
        if (endType == UNBOUNDED_PRECEDING) {
            throw new SemanticException(INVALID_WINDOW_FRAME, frame, "Window frame end cannot be UNBOUNDED PRECEDING");
        }
        if ((startType == CURRENT_ROW) && (endType == PRECEDING)) {
            throw new SemanticException(INVALID_WINDOW_FRAME, frame, "Window frame starting from CURRENT ROW cannot end with PRECEDING");
        }
        if ((startType == FOLLOWING) && (endType == PRECEDING)) {
            throw new SemanticException(INVALID_WINDOW_FRAME, frame, "Window frame starting from FOLLOWING cannot end with PRECEDING");
        }
        if ((startType == FOLLOWING) && (endType == CURRENT_ROW)) {
            throw new SemanticException(INVALID_WINDOW_FRAME, frame, "Window frame starting from FOLLOWING cannot end with CURRENT ROW");
        }
        if ((frame.getType() == RANGE) && ((startType == PRECEDING) || (endType == PRECEDING))) {
            throw new SemanticException(INVALID_WINDOW_FRAME, frame, "Window frame RANGE PRECEDING is only supported with UNBOUNDED");
        }
        if ((frame.getType() == RANGE) && ((startType == FOLLOWING) || (endType == FOLLOWING))) {
            throw new SemanticException(INVALID_WINDOW_FRAME, frame, "Window frame RANGE FOLLOWING is only supported with UNBOUNDED");
        }
    }

    private void analyzeHaving(QuerySpecification node, Scope scope)
    {
        if (node.getHaving().isPresent()) {
            Expression predicate = node.getHaving().get();

            ExpressionAnalysis expressionAnalysis = analyzeExpression(predicate, scope);
            analysis.recordSubqueries(node, expressionAnalysis);

            Type predicateType = expressionAnalysis.getType(predicate);
            if (!predicateType.equals(BOOLEAN) && !predicateType.equals(UNKNOWN)) {
                throw new SemanticException(TYPE_MISMATCH, predicate, "HAVING clause must evaluate to a boolean: actual type %s", predicateType);
            }

            analysis.setHaving(node, predicate);
        }
    }

    private void analyzeOrderBy(QuerySpecification node, Scope scope, List<Expression> outputExpressions)
    {
        List<SortItem> sortItems = node.getOrderBy();

        ImmutableList.Builder<Expression> orderByExpressions = ImmutableList.builder();

        if (!sortItems.isEmpty()) {
            for (SortItem sortItem : sortItems) {
                Expression expression = sortItem.getSortKey();

                final Expression orderByExpression;
                if (expression instanceof LongLiteral) {
                    // this is an ordinal in the output tuple
                    long ordinal = ((LongLiteral) expression).getValue();
                    if (ordinal < 1 || ordinal > scope.getRelationType().getVisibleFieldCount()) {
                        throw new SemanticException(INVALID_ORDINAL, expression, "ORDER BY position %s is not in select list", ordinal);
                    }
                    int field = Ints.checkedCast(ordinal - 1);
                    orderByExpression = new FieldReference(field);
                    analysis.copyExpressionAnalysis(orderByExpression, outputExpressions.get(field));
                }
                else {
                    if (node.getSelect().isDistinct() && !outputExpressions.contains(expression)) {
                        throw new SemanticException(ORDER_BY_MUST_BE_IN_SELECT, node.getSelect(), "For SELECT DISTINCT, ORDER BY expressions must appear in select list");
                    }
                    ExpressionAnalysis expressionAnalysis = analyzeExpression(expression, scope);
                    analysis.recordSubqueries(node, expressionAnalysis);
                    orderByExpression = expression;
                }

                Type type = analysis.getType(orderByExpression);
                if (!type.isOrderable()) {
                    throw new SemanticException(TYPE_MISMATCH, node, "Type %s is not orderable, and therefore cannot be used in ORDER BY: %s", type, expression);
                }
                orderByExpressions.add(orderByExpression);
            }
        }

        analysis.setOrderByExpressions(node, orderByExpressions.build());
    }

    private List<List<Expression>> analyzeGroupBy(QuerySpecification node, Scope scope, List<AnalyzedColumn> outputExpressions)
    {
        List<Set<Expression>> computedGroupingSets = ImmutableList.of(); // empty list = no aggregations

        if (node.getGroupBy().isPresent()) {
            List<List<Set<Expression>>> enumeratedGroupingSets = node.getGroupBy().get().getGroupingElements().stream()
                    .map(GroupingElement::enumerateGroupingSets)
                    .collect(toImmutableList());

            // compute cross product of enumerated grouping sets, if there are any
            computedGroupingSets = computeGroupingSetsCrossProduct(enumeratedGroupingSets, node.getGroupBy().get().isDistinct());
            checkState(!computedGroupingSets.isEmpty(), "computed grouping sets cannot be empty");
        }
        else if (!extractAggregates(node).isEmpty()) {
            // if there are aggregates, but no group by, create a grand total grouping set (global aggregation)
            computedGroupingSets = ImmutableList.of(ImmutableSet.of());
        }

        List<List<Expression>> analyzedGroupingSets = computedGroupingSets.stream()
                .map(groupingSet -> analyzeGroupingColumns(groupingSet, node, scope, outputExpressions))
                .collect(toImmutableList());

        analysis.setGroupingSets(node, analyzedGroupingSets);
        return analyzedGroupingSets;
    }

    private List<Set<Expression>> computeGroupingSetsCrossProduct(List<List<Set<Expression>>> enumeratedGroupingSets, boolean isDistinct)
    {
        checkState(!enumeratedGroupingSets.isEmpty(), "enumeratedGroupingSets cannot be empty");

        List<Set<Expression>> groupingSetsCrossProduct = new ArrayList<>();
        enumeratedGroupingSets.get(0)
                .stream()
                .map(ImmutableSet::copyOf)
                .forEach(groupingSetsCrossProduct::add);

        for (int i = 1; i < enumeratedGroupingSets.size(); i++) {
            List<Set<Expression>> groupingSets = enumeratedGroupingSets.get(i);
            List<Set<Expression>> oldGroupingSetsCrossProduct = ImmutableList.copyOf(groupingSetsCrossProduct);
            groupingSetsCrossProduct.clear();
            for (Set<Expression> existingSet : oldGroupingSetsCrossProduct) {
                for (Set<Expression> groupingSet : groupingSets) {
                    Set<Expression> concatenatedSet = ImmutableSet.<Expression>builder()
                            .addAll(existingSet)
                            .addAll(groupingSet)
                            .build();
                    groupingSetsCrossProduct.add(concatenatedSet);
                }
            }
        }

        if (isDistinct) {
            return ImmutableList.copyOf(ImmutableSet.copyOf(groupingSetsCrossProduct));
        }

        return groupingSetsCrossProduct;
    }

    private List<Expression> analyzeGroupingColumns(Set<Expression> groupingColumns, QuerySpecification node, Scope scope, List<AnalyzedColumn> outputExpressions)
    {
        ImmutableList.Builder<Expression> groupingColumnsBuilder = ImmutableList.builder();
        for (Expression groupingColumn : groupingColumns) {
            // first, see if this is an ordinal
            Expression groupByExpression;

            if (groupingColumn instanceof LongLiteral) {
                long ordinal = ((LongLiteral) groupingColumn).getValue();
                if (ordinal < 1 || ordinal > outputExpressions.size()) {
                    throw new SemanticException(INVALID_ORDINAL, groupingColumn, "GROUP BY position %s is not in select list", ordinal);
                }

                groupByExpression = outputExpressions.get(Ints.checkedCast(ordinal - 1)).getExpression();
            }
            else {
                ExpressionAnalysis expressionAnalysis = analyzeExpression(groupingColumn, scope);
                analysis.recordSubqueries(node, expressionAnalysis);
                groupByExpression = groupingColumn;
            }

            Analyzer.verifyNoAggregatesOrWindowFunctions(metadata, groupByExpression, "GROUP BY");
            Type type = analysis.getType(groupByExpression);
            if (!type.isComparable()) {
                throw new SemanticException(TYPE_MISMATCH, node, "%s is not comparable, and therefore cannot be used in GROUP BY", type);
            }

            groupingColumnsBuilder.add(groupByExpression);
        }
        return groupingColumnsBuilder.build();
    }

    private List<AnalyzedColumn> analyzeSelect(QuerySpecification node, Scope scope)
    {
        boolean distinct = node.getSelect().isDistinct();
        List<AnalyzedColumn> columns = new ArrayList<>();
        for (SelectItem item : node.getSelect().getSelectItems()) {
            if (item instanceof AllColumns) {
                // expand * and T.*
                Optional<QualifiedName> starPrefix = ((AllColumns) item).getPrefix();

                List<Field> fields = scope.getRelationType().resolveFieldsWithPrefix(starPrefix);
                if (fields.isEmpty()) {
                    if (starPrefix.isPresent()) {
                        throw new SemanticException(MISSING_TABLE, item, "Table '%s' not found", starPrefix.get());
                    }
                    throw new SemanticException(WILDCARD_WITHOUT_FROM, item, "SELECT * not allowed in queries without FROM clause");
                }

                for (Field field : fields) {
                    int fieldIndex = scope.getRelationType().indexOf(field);
                    Expression expression = new FieldReference(fieldIndex);
                    if (field.getQualifiedName().isPresent()) {
                        expression = new QualifiedNameReference(field.getQualifiedName().get());
                    }
                    columns.add(new AnalyzedColumn(expression, Optional.empty(), field.getType()));
                    Type type = field.getType();
                    analysis.addType(expression, type);
                    if (distinct && !type.isComparable()) {
                        throw new SemanticException(TYPE_MISMATCH, node.getSelect(), "DISTINCT can only be applied to comparable types (actual: %s)", type);
                    }
                }
            }
            else if (item instanceof SingleColumn) {
                SingleColumn column = (SingleColumn) item;
                Expression expression = column.getExpression();
                Type type;
                Optional<ResolvedField> resolvedField = scope.tryResolveField(expression);
                if (resolvedField.isPresent()) {
                    Field field = resolvedField.get().getField();
                    type = field.getType();
                    analysis.addType(expression, type);
                }
                else {
                    ExpressionAnalysis expressionAnalysis = analyzeExpression(expression, scope);
                    analysis.recordSubqueries(node, expressionAnalysis);
                    type = expressionAnalysis.getType(expression);
                }

                if (distinct && !type.isComparable()) {
                    throw new SemanticException(TYPE_MISMATCH, node.getSelect(), "DISTINCT can only be applied to comparable types (actual: %s): %s", type, expression);
                }

                columns.add(new AnalyzedColumn(expression, column.getAlias(), type));
            }
            else {
                throw new IllegalArgumentException("Unsupported SelectItem type: " + item.getClass().getName());
            }
        }

        return columns;
    }

    public void analyzeWhere(Node node, Scope scope, Expression predicate)
    {
        Analyzer.verifyNoAggregatesOrWindowFunctions(metadata, predicate, "WHERE");

        ExpressionAnalysis expressionAnalysis = analyzeExpression(predicate, scope);
        analysis.recordSubqueries(node, expressionAnalysis);

        Type predicateType = expressionAnalysis.getType(predicate);
        if (!predicateType.equals(BOOLEAN)) {
            if (!predicateType.equals(UNKNOWN)) {
                throw new SemanticException(TYPE_MISMATCH, predicate, "WHERE clause must evaluate to a boolean: actual type %s", predicateType);
            }
            // coerce null to boolean
            analysis.addCoercion(predicate, BOOLEAN, false);
        }

        analysis.setWhere(node, predicate);
    }

    private Scope analyzeFrom(QuerySpecification node, Scope scope)
    {
        if (node.getFrom().isPresent()) {
            return process(node.getFrom().get(), scope);
        }

        return scope;
    }

    private void analyzeAggregations(
            QuerySpecification node,
            Scope scope,
            List<List<Expression>> groupingSets,
            List<Expression> outputExpressions,
            Set<Expression> columnReferences)
    {
        List<FunctionCall> aggregates = extractAggregates(node);

        if (scope.isApproximate()) {
            if (aggregates.stream().anyMatch(FunctionCall::isDistinct)) {
                throw new SemanticException(NOT_SUPPORTED, node, "DISTINCT aggregations not supported for approximate queries");
            }
        }

        // is this an aggregation query?
        if (!groupingSets.isEmpty()) {
            // ensure SELECT, ORDER BY and HAVING are constant with respect to group
            // e.g, these are all valid expressions:
            //     SELECT f(a) GROUP BY a
            //     SELECT f(a + 1) GROUP BY a + 1
            //     SELECT a + sum(b) GROUP BY a
            ImmutableList<Expression> distinctGroupingColumns = groupingSets.stream()
                    .flatMap(Collection::stream)
                    .distinct()
                    .collect(toImmutableList());

            for (Expression expression : Iterables.concat(outputExpressions)) {
                verifyAggregations(distinctGroupingColumns, scope, expression, columnReferences);
            }

            if (node.getHaving().isPresent()) {
                verifyAggregations(distinctGroupingColumns, scope, node.getHaving().get(), columnReferences);
            }
        }
    }

    private List<FunctionCall> extractAggregates(QuerySpecification node)
    {
        AggregateExtractor extractor = new AggregateExtractor(metadata);
        for (SelectItem item : node.getSelect().getSelectItems()) {
            if (item instanceof SingleColumn) {
                extractor.process(((SingleColumn) item).getExpression(), null);
            }
        }

        for (SortItem item : node.getOrderBy()) {
            extractor.process(item.getSortKey(), null);
        }

        if (node.getHaving().isPresent()) {
            extractor.process(node.getHaving().get(), null);
        }

        List<FunctionCall> aggregates = extractor.getAggregates();
        analysis.setAggregates(node, aggregates);

        return aggregates;
    }

    private void verifyAggregations(
            List<Expression> groupByExpressions,
            Scope scope,
            Expression expression,
            Set<Expression> columnReferences)
    {
        AggregationAnalyzer analyzer = new AggregationAnalyzer(groupByExpressions, metadata, scope, columnReferences);
        analyzer.analyze(expression);
    }

    private RelationType analyzeView(Query query, QualifiedObjectName name, Optional<String> catalog, Optional<String> schema, Optional<String> owner, Table node)
    {
        try {
            // run view as view owner if set; otherwise, run as session user
            Identity identity;
            AccessControl viewAccessControl;
            if (owner.isPresent()) {
                identity = new Identity(owner.get(), Optional.empty());
                viewAccessControl = new ViewAccessControl(accessControl);
            }
            else {
                identity = session.getIdentity();
                viewAccessControl = accessControl;
            }

            Session viewSession = Session.builder(metadata.getSessionPropertyManager())
                    .setQueryId(session.getQueryId())
                    .setTransactionId(session.getTransactionId().orElse(null))
                    .setIdentity(identity)
                    .setSource(session.getSource().orElse(null))
                    .setCatalog(catalog.orElse(null))
                    .setSchema(schema.orElse(null))
                    .setTimeZoneKey(session.getTimeZoneKey())
                    .setLocale(session.getLocale())
                    .setRemoteUserAddress(session.getRemoteUserAddress().orElse(null))
                    .setUserAgent(session.getUserAgent().orElse(null))
                    .setStartTime(session.getStartTime())
                    .build();

            StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, sqlParser, viewAccessControl, viewSession, experimentalSyntaxEnabled);
            Scope queryScope = analyzer.process(query, Scope.builder().build());
            return queryScope.getRelationType().withAlias(name.getObjectName(), null);
        }
        catch (RuntimeException e) {
            throw new SemanticException(VIEW_ANALYSIS_ERROR, node, "Failed analyzing stored view '%s': %s", name, e.getMessage());
        }
    }

    private Query parseView(String view, QualifiedObjectName name, Node node)
    {
        try {
            Statement statement = sqlParser.createStatement(view);
            return checkType(statement, Query.class, "parsed view");
        }
        catch (ParsingException e) {
            throw new SemanticException(VIEW_PARSE_ERROR, node, "Failed parsing stored view '%s': %s", name, e.getMessage());
        }
    }

    private boolean isViewStale(List<ViewDefinition.ViewColumn> columns, Collection<Field> fields)
    {
        if (columns.size() != fields.size()) {
            return true;
        }

        List<Field> fieldList = ImmutableList.copyOf(fields);
        for (int i = 0; i < columns.size(); i++) {
            ViewDefinition.ViewColumn column = columns.get(i);
            Field field = fieldList.get(i);
            if (!column.getName().equals(field.getName().orElse(null)) ||
                    !metadata.getTypeManager().canCoerce(field.getType(), column.getType())) {
                return true;
            }
        }

        return false;
    }

    private ExpressionAnalysis analyzeExpression(Expression expression, Scope scope)
    {
        return ExpressionAnalyzer.analyzeExpression(
                session,
                metadata,
                accessControl,
                sqlParser,
                scope,
                analysis,
                experimentalSyntaxEnabled,
                expression);
    }

    private List<Expression> descriptorToFields(Scope scope)
    {
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();
        for (int fieldIndex = 0; fieldIndex < scope.getRelationType().getAllFieldCount(); fieldIndex++) {
            FieldReference expression = new FieldReference(fieldIndex);
            builder.add(expression);
            analyzeExpression(expression, scope);
        }
        return builder.build();
    }

    private Scope analyzeWith(Query node, Scope scope)
    {
        // analyze WITH clause
        if (!node.getWith().isPresent()) {
            return scope;
        }

        With with = node.getWith().get();
        if (with.isRecursive()) {
            throw new SemanticException(NOT_SUPPORTED, with, "Recursive WITH queries are not supported");
        }

        Scope.Builder withScopeBuilder = Scope.builder().withParent(scope);
        for (WithQuery withQuery : with.getQueries()) {
            Query query = withQuery.getQuery();
            process(query, withScopeBuilder.build());

            String name = withQuery.getName();

            // check if all or none of the columns are explicitly alias
            if (withQuery.getColumnNames().isPresent()) {
                List<String> columnNames = withQuery.getColumnNames().get();
                RelationType queryDescriptor = analysis.getOutputDescriptor(query);
                if (columnNames.size() != queryDescriptor.getVisibleFieldCount()) {
                    throw new SemanticException(MISMATCHED_COLUMN_ALIASES, withQuery, "WITH column alias list has %s entries but WITH query(%s) has %s columns", columnNames.size(), name, queryDescriptor.getVisibleFieldCount());
                }
            }
            if (withScopeBuilder.containsNamedQuery(name)) {
                throw new SemanticException(DUPLICATE_RELATION, withQuery, "WITH query name '%s' specified more than once", name);
            }

            withScopeBuilder.withNamedQuery(name, withQuery);
        }

        Scope withScope = withScopeBuilder.build();
        analysis.setScope(with, withScope);
        return withScope;
    }

    private void analyzeOrderBy(Query node, Scope scope)
    {
        List<SortItem> items = node.getOrderBy();

        ImmutableList.Builder<Expression> orderByFieldsBuilder = ImmutableList.builder();

        for (SortItem item : items) {
            Expression expression = item.getSortKey();

            if (expression instanceof LongLiteral) {
                // this is an ordinal in the output tuple

                long ordinal = ((LongLiteral) expression).getValue();
                if (ordinal < 1 || ordinal > scope.getRelationType().getVisibleFieldCount()) {
                    throw new SemanticException(INVALID_ORDINAL, expression, "ORDER BY position %s is not in select list", ordinal);
                }

                expression = new FieldReference(Ints.checkedCast(ordinal - 1));
            }

            ExpressionAnalysis expressionAnalysis = ExpressionAnalyzer.analyzeExpression(session,
                    metadata,
                    accessControl, sqlParser,
                    scope,
                    analysis,
                    experimentalSyntaxEnabled,
                    expression);
            analysis.recordSubqueries(node, expressionAnalysis);

            orderByFieldsBuilder.add(expression);
        }

        analysis.setOrderByExpressions(node, orderByFieldsBuilder.build());
    }

    public Scope createScope(Node node, Scope parent, Field... fields)
    {
        return createScope(node, parent, new RelationType(fields));
    }

    public Scope createScope(Node node, Scope parent, List<Field> fields)
    {
        return createScope(node, parent, new RelationType(fields));
    }

    public Scope createScope(Node node, Scope parent, RelationType relationType)
    {
        Scope scope = Scope.builder().withParent(parent).withRelationType(relationType).build();
        analysis.setScope(node, scope);
        return scope;
    }

    public Analysis getAnalysis()
    {
        return analysis;
    }

    private class AnalyzedColumn
    {
        private final Expression expression;
        private final Optional<String> alias;
        private final Type type;

        public AnalyzedColumn(Expression expression, Optional<String> alias, Type type)
        {
            this.expression = requireNonNull(expression, "expression is null");
            this.alias = requireNonNull(alias, "alias is null");
            this.type = requireNonNull(type, "type is null");
        }

        public Expression getExpression()
        {
            return expression;
        }

        public Optional<String> getAlias()
        {
            return alias;
        }

        public Type getType()
        {
            return type;
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("alias", alias)
                    .add("expression", expression)
                    .add("type", type)
                    .toString();
        }
    }
}
