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

import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.WithQuery;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.Immutable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.analyzer.SemanticExceptions.createAmbiguousAttributeException;
import static com.facebook.presto.sql.analyzer.SemanticExceptions.createMissingAttributeException;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class Scope
{
    private final Optional<Scope> parent;
    private final RelationType relation;
    private final boolean approximate;
    private final boolean queryBoundary;
    private final Map<String, WithQuery> namedQueries;

    public static Builder builder()
    {
        return new Builder();
    }

    private Scope(Optional<Scope> parent, RelationType relation, Map<String, WithQuery> namedQueries, boolean approximate, boolean queryBoundary)
    {
        this.parent = requireNonNull(parent, "parent is null");
        this.relation = requireNonNull(relation, "relation is null");
        this.namedQueries = ImmutableMap.copyOf(requireNonNull(namedQueries, "namedQueries is null"));
        this.approximate = approximate;
        this.queryBoundary = queryBoundary;
    }

    public RelationType getRelationType()
    {
        return relation;
    }

    public ResolvedField resolveField(Expression expression)
    {
        return resolveField(expression, asQualifiedName(expression));
    }

    public ResolvedField resolveField(Expression expression, QualifiedName name)
    {
        List<ResolvedField> resolvedFields = resolveField(name, true);
        if (resolvedFields.size() == 0) {
            throw createMissingAttributeException(expression, name);
        }
        else if (resolvedFields.size() == 1) {
            return resolvedFields.get(0);
        }
        else {
            resolvedFields = filterVisible(resolvedFields);
            if (resolvedFields.size() == 1) {
                return resolvedFields.get(0);
            }
            throw createAmbiguousAttributeException(expression, name);
        }
    }

    private List<ResolvedField> filterVisible(List<ResolvedField> resolvedFields)
    {
        return resolvedFields.stream()
                .filter(resolvedField -> resolvedField.getField().isVisible())
                .collect(toImmutableList());
    }

    public Optional<ResolvedField> tryResolveField(Expression expression)
    {
        QualifiedName qualifiedName = asQualifiedName(expression);
        if (qualifiedName != null) {
            return tryResolveField(qualifiedName);
        }
        return Optional.empty();
    }

    private QualifiedName asQualifiedName(Expression expression)
    {
        QualifiedName name = null;
        if (expression instanceof QualifiedNameReference) {
            name = ((QualifiedNameReference) expression).getName();
        }
        else if (expression instanceof DereferenceExpression) {
            name = DereferenceExpression.getQualifiedName((DereferenceExpression) expression);
        }
        return name;
    }

    public Optional<ResolvedField> tryResolveField(QualifiedName name)
    {
        List<ResolvedField> resolvedFields = resolveField(name, true);
        if (resolvedFields.size() > 1) {
            resolvedFields = filterVisible(resolvedFields);
        }
        if (resolvedFields.size() == 1) {
            return Optional.of(resolvedFields.get(0));
        }
        return Optional.empty();
    }

    private List<ResolvedField> resolveField(QualifiedName name, boolean local)
    {
        List<Field> matches = relation.resolveFields(name);
        if (matches.isEmpty()) {
            if (isColumnReference(name, relation)) {
                return ImmutableList.of();
            }
            Scope boundary = this;
            while (!boundary.queryBoundary) {
                if (boundary.parent.isPresent()) {
                    boundary = boundary.parent.get();
                }
                else {
                    return ImmutableList.of();
                }
            }
            if (boundary.parent.isPresent()) {
                // jump over the query boundary
                return boundary.parent.get().resolveField(name, false);
            }
            return ImmutableList.of();
        }

        List<Optional<QualifiedName>> names = new ArrayList<>();
        return matches.stream()
                .filter(field -> {
                    if (!names.contains(field.getQualifiedName())) {
                        names.add(field.getQualifiedName());
                        return true;
                    }
                    return false;
                })
                .map(field -> asResolvedField(field, local))
                .collect(toImmutableList());
    }

    private ResolvedField asResolvedField(Field field, boolean local)
    {
        int fieldIndex = relation.indexOf(field);
        return new ResolvedField(this, field, fieldIndex, local);
    }

    public boolean isColumnReference(QualifiedName name)
    {
        Scope current = this;
        while (current != null) {
            if (isColumnReference(name, current.relation)) {
                return true;
            }

            current = current.parent.orElse(null);
        }

        return false;
    }

    private boolean isColumnReference(QualifiedName name, RelationType relation)
    {
        while (name.getPrefix().isPresent()) {
            name = name.getPrefix().get();
            if (!relation.resolveFields(name).isEmpty()) {
                return true;
            }
        }
        return false;
    }

    public Optional<WithQuery> getNamedQuery(String name)
    {
        if (namedQueries.containsKey(name)) {
            return Optional.of(namedQueries.get(name));
        }

        if (parent.isPresent()) {
            return parent.get().getNamedQuery(name);
        }

        return Optional.empty();
    }

    public boolean isApproximate()
    {
        return approximate;
    }

    public Scope copyWithPrunedHiddenFields()
    {
        ImmutableList<Field> nonHiddenFields = relation.getAllFields().stream()
                .filter(field -> field.getKind() != Field.Kind.HIDDEN)
                .collect(toImmutableList());
        return new Scope(parent, new RelationType(nonHiddenFields), namedQueries, approximate, queryBoundary);
    }

    public static final class Builder
    {
        private RelationType relationType = new RelationType();
        private Optional<Boolean> approximate = Optional.empty();
        private Optional<Boolean> queryBoundary = Optional.empty();
        private final Map<String, WithQuery> namedQueries = new HashMap<>();
        private Optional<Scope> parent = Optional.empty();

        public Builder withRelationType(RelationType relationType)
        {
            this.relationType = requireNonNull(relationType, "relationType is null");
            return this;
        }

        public Builder withParent(Scope parent)
        {
            this.parent = Optional.of(parent);
            return this;
        }

        public Builder withQueryBoundary(boolean queryBoundary)
        {
            this.queryBoundary = Optional.of(queryBoundary);
            return this;
        }

        public Builder withApproximate(boolean approximate)
        {
            this.approximate = Optional.of(approximate);
            return this;
        }

        public Builder withNamedQuery(String name, WithQuery withQuery)
        {
            checkArgument(!containsNamedQuery(name), "Query '%s' is already added", name);
            namedQueries.put(name, withQuery);
            return this;
        }

        public boolean containsNamedQuery(String name)
        {
            return namedQueries.containsKey(name);
        }

        public Scope build()
        {
            boolean approximate = this.approximate.orElse(parent.map(Scope::isApproximate).orElse(false));
            return new Scope(parent, relationType, namedQueries, approximate, queryBoundary.orElse(false));
        }
    }
}
