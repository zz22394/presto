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
package com.facebook.presto.tests;

import com.teradata.tempto.Requirement;
import com.teradata.tempto.RequirementsProvider;
import com.teradata.tempto.configuration.Configuration;
import com.teradata.tempto.fulfillment.ldap.LdapObjectRequirement;

import java.util.Arrays;

import static com.facebook.presto.tests.ImmutableLdapObjectDefinitions.AMERICA_ORG;
import static com.facebook.presto.tests.ImmutableLdapObjectDefinitions.ASIA_ORG;
import static com.facebook.presto.tests.ImmutableLdapObjectDefinitions.CHILD_GROUP;
import static com.facebook.presto.tests.ImmutableLdapObjectDefinitions.CHILD_GROUP_USER;
import static com.facebook.presto.tests.ImmutableLdapObjectDefinitions.DEFAULT_GROUP;
import static com.facebook.presto.tests.ImmutableLdapObjectDefinitions.DEFAULT_GROUP_USER;
import static com.facebook.presto.tests.ImmutableLdapObjectDefinitions.ORPHAN_USER;
import static com.facebook.presto.tests.ImmutableLdapObjectDefinitions.PARENT_GROUP;
import static com.facebook.presto.tests.ImmutableLdapObjectDefinitions.PARENT_GROUP_USER;
import static com.teradata.tempto.Requirements.compose;

public class ImmutableLdapObjectRequirements
        implements RequirementsProvider
{
    public static final Requirement DEFAULT_OBJECTS = new LdapObjectRequirement(Arrays.asList(AMERICA_ORG, ASIA_ORG, DEFAULT_GROUP, DEFAULT_GROUP_USER));
    public static final Requirement PARENT_OBJECTS = new LdapObjectRequirement(Arrays.asList(AMERICA_ORG, ASIA_ORG, PARENT_GROUP, PARENT_GROUP_USER));
    public static final Requirement CHILD_OBJECTS = new LdapObjectRequirement(Arrays.asList(AMERICA_ORG, ASIA_ORG, CHILD_GROUP, CHILD_GROUP_USER));
 //   public static final Requirement MULTIPLE_GROUPS_OBJECTS = new LdapObjectRequirement(Arrays.asList(AMERICA_ORG, ASIA_ORG, DEFAULT_GROUP, ANOTHER_GROUP, USER_IN_MULTIPLE_GROUPS));
    public static final Requirement ORPHAN_OBJECTS = new LdapObjectRequirement(Arrays.asList(AMERICA_ORG, ASIA_ORG, ORPHAN_USER));

    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return compose(
                DEFAULT_OBJECTS,
                PARENT_OBJECTS,
                CHILD_OBJECTS,
//                MULTIPLE_GROUPS_OBJECTS,
                ORPHAN_OBJECTS
        );
    }

    public static class ImmutableDefaultUserObject
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return DEFAULT_OBJECTS;
        }
    }

    public static class ImmutableParentUserObject
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return PARENT_OBJECTS;
        }
    }

    public static class ImmutableChildUserObject
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return CHILD_OBJECTS;
        }
    }
/*
    public static class ImmutableUserInMultipleGroupsObject
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return MULTIPLE_GROUPS_OBJECTS;
        }
    }
*/
    public static class ImmutableOrphanUserObject
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return ORPHAN_OBJECTS;
        }
    }
}
