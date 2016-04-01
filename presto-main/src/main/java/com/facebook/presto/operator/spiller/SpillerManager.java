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
package com.facebook.presto.operator.spiller;

import com.facebook.presto.spi.spiller.SpillerFactory;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

public class SpillerManager
{
    private Optional<SpillerFactory> spillerFactory = Optional.empty();

    public void addSpillerFactor(SpillerFactory spillerFactory)
    {
        checkState(
                !this.spillerFactory.isPresent(),
                "Can not add spillerFactory [%s] because another is already registered [%s]",
                spillerFactory.getName(),
                this.spillerFactory.isPresent() ? this.spillerFactory.get().getName() : "SHOULDN'T HAPPEN");
        this.spillerFactory = Optional.of(spillerFactory);
    }

    public Optional<SpillerFactory> getSpillerFactory()
    {
        return spillerFactory;
    }
}
