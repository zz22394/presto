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
package com.facebook.presto.plugin.jdbc;

//import com.google.common.collect.ImmutableSet;

//import java.util.Set;

//import static java.util.Locale.ENGLISH;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

public enum PostgreSqlPrivilege
{
    SELECT, INSERT, UPDATE, DELETE, OWNERSHIP, USAGE, SELECT_WITH_GRANT;

    public static Set<PostgreSqlPrivilege> parsePrivilege(String name)
    {
        switch (name) {
            case "ALL":
                return ImmutableSet.copyOf(values());
            case "SELECT":
                return ImmutableSet.of(SELECT);
            case "INSERT":
                return ImmutableSet.of(INSERT);
            case "UPDATE":
                return ImmutableSet.of(UPDATE);
            case "DELETE":
                return ImmutableSet.of(DELETE);
            case "OWNERSHIP":
                return ImmutableSet.of(OWNERSHIP);
            case "USAGE":
                return ImmutableSet.of(USAGE);
        }
        return ImmutableSet.of();
    }
}
