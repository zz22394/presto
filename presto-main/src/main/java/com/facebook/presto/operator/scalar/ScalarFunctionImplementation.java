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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.ConnectorSession;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class ScalarFunctionImplementation
{
    private final boolean nullable;
    private final List<Boolean> nullableArguments;
    private final MethodHandle methodHandle;
    private final Optional<MethodHandle> instanceFactory;
    private final boolean deterministic;
    private final boolean returnValueAsParameter;
    private final Optional<Integer> returnValueSliceLength;

    public ScalarFunctionImplementation(boolean nullable, List<Boolean> nullableArguments, MethodHandle methodHandle, boolean deterministic)
    {
        this(nullable, nullableArguments, methodHandle, Optional.empty(), deterministic);
    }

    public ScalarFunctionImplementation(boolean nullable, List<Boolean> nullableArguments, MethodHandle methodHandle, Optional<MethodHandle> instanceFactory, boolean deterministic)
    {
        this(nullable, nullableArguments, methodHandle, instanceFactory, deterministic, false, Optional.empty());
    }

    public ScalarFunctionImplementation(
            boolean nullable,
            List<Boolean> nullableArguments,
            MethodHandle methodHandle,
            Optional<MethodHandle> instanceFactory,
            boolean deterministic,
            boolean returnValueAsParameter,
            Optional<Integer> returnValueSliceLength)
    {
        this.nullable = nullable;
        this.nullableArguments = ImmutableList.copyOf(requireNonNull(nullableArguments, "nullableArguments is null"));
        this.methodHandle = requireNonNull(methodHandle, "methodHandle is null");
        this.instanceFactory = requireNonNull(instanceFactory, "instanceFactory is null");
        this.deterministic = deterministic;
        this.returnValueAsParameter = returnValueAsParameter;
        this.returnValueSliceLength = returnValueSliceLength;

        int expectedParametersCount = methodHandle.type().parameterCount();
        if (returnValueAsParameter) {
            expectedParametersCount--;
        }
        if (instanceFactory.isPresent()) {
            expectedParametersCount--;
        }
        if (methodHandle.type().parameterCount() >= 1 && methodHandle.type().parameterType(0) == ConnectorSession.class) {
            expectedParametersCount--;
        }
        checkArgument(nullableArguments.size() == expectedParametersCount, "method handle parameters count mismatch");

        if (returnValueAsParameter) {
            checkArgument(!nullable, "nullable not supported for functions returning value as parameter");
            checkArgument(getMethodHandle().type().returnType() == void.class, "methodHandle returns non-void for function returning value as parameter");
            int returnParameterPos = instanceFactory.isPresent() ? 1 : 0;
            checkArgument(getMethodHandle().type().parameterType(returnParameterPos) == Slice.class, "only Slice parameters supported as return parameter");
            checkArgument(returnValueSliceLength.isPresent(), "returnValuesSliceLength not set");
        }
        else {
            checkArgument(!returnValueSliceLength.isPresent(), "returnValueSliceLength set for non Slice return parameter");
        }

        if (instanceFactory.isPresent()) {
            Class<?> instanceType = instanceFactory.get().type().returnType();
            checkArgument(instanceType.equals(methodHandle.type().parameterType(0)), "methodHandle is not an instance method");
        }
    }

    public boolean isNullable()
    {
        return nullable;
    }

    public List<Boolean> getNullableArguments()
    {
        return nullableArguments;
    }

    public MethodHandle getMethodHandle()
    {
        return methodHandle;
    }

    public Optional<MethodHandle> getInstanceFactory()
    {
        return instanceFactory;
    }

    public boolean isDeterministic()
    {
        return deterministic;
    }

    public boolean isReturnValueAsParameter()
    {
        return returnValueAsParameter;
    }

    public Optional<Integer> getReturnValueSliceLength()
    {
        return returnValueSliceLength;
    }
}
