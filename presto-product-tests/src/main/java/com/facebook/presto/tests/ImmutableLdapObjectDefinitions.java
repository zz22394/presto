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

import com.google.common.collect.ImmutableMap;
import com.teradata.tempto.fulfillment.ldap.LdapObjectDefinition;

import java.util.Arrays;

public final class ImmutableLdapObjectDefinitions
{
    private ImmutableLdapObjectDefinitions() {}

    public static final LdapObjectDefinition AMERICA_ORG =
            LdapObjectDefinition.builder("AmericaOrg")
                    .setDistinguishedName("ou=America,dc=10.25.150.158")
                    .setAttributes(ImmutableMap.of(
                            "ou", "America",
                            "name", "America"))
                    .addObjectClasses(Arrays.asList("top", "organizationalUnit"))
                    .build();

    public static final LdapObjectDefinition ASIA_ORG =
            LdapObjectDefinition.builder("AsiaOrg")
                    .setDistinguishedName("ou=Asia,dc=10.25.150.158")
                    .setAttributes(ImmutableMap.of(
                            "ou", "Asia",
                            "name", "Asia"))
                    .addObjectClasses(Arrays.asList("top", "organizationalUnit"))
                    .build();

    public static final LdapObjectDefinition DEFAULT_GROUP =
            LdapObjectDefinition.builder("DefaultGroup")
                    .setDistinguishedName("cn=DefaultGroup,ou=America,dc=10.25.150.158")
                    .setAttributes(ImmutableMap.of(
                            "cn", "DefaultGroup",
                            "member", "uid=DefaultGroupUser,ou=Asia,dc=10.25.150.158"))
                    .addObjectClasses(Arrays.asList("groupOfNames"))
                    .build();

    public static final LdapObjectDefinition PARENT_GROUP =
            LdapObjectDefinition.builder("ParentGroup")
                    .setDistinguishedName("cn=ParentGroup,ou=America,dc=10.25.150.158")
                    .setAttributes(ImmutableMap.of(
                            "cn", "ParentGroup",
                            "member", "uid=ParentGroupUser,ou=Asia,dc=10.25.150.158",
                            "member", "uid=UserInMultipleGroups,ou=Asia,dc=10.25.150.158"))
                    .addObjectClasses(Arrays.asList("groupOfNames"))
                    .build();

    public static final LdapObjectDefinition CHILD_GROUP =
            LdapObjectDefinition.builder("ChildGroup")
                    .setDistinguishedName("cn=ChildGroup,ou=America,dc=10.25.150.158")
                    .setAttributes(ImmutableMap.of(
                            "cn", "ChildGroup",
                            "member", "uid=ChildGroupUser,ou=Asia,dc=10.25.150.158"))
                    .addObjectClasses(Arrays.asList("groupOfNames"))
                    .build();

    public static final LdapObjectDefinition ANOTHER_GROUP =
            LdapObjectDefinition.builder("AnotherGroup")
                    .setDistinguishedName("cn=AnotherGroup,ou=America,dc=10.25.150.158")
                    .setAttributes(ImmutableMap.of(
                            "cn", "AnotherGroup",
                            "member", "uid=UserInMultipleGroups,ou=Asia,dc=10.25.150.158"))
                    .addObjectClasses(Arrays.asList("groupOfNames"))
                    .build();

    public static final LdapObjectDefinition DEFAULT_GROUP_USER =
            LdapObjectDefinition.builder("DefaultGroupUser")
                    .setDistinguishedName("uid=defaultgroupuser,ou=Asia,dc=10.25.150.158")
                    .setAttributes(ImmutableMap.of(
                            "cn", "DefaultGroupUser",
                            "sn", "DefaultGroupUser",
                            "password", "LDAPPass123",
                            "memberOf", "DefaultGroup"
                    ))
                    .addObjectClasses(Arrays.asList("person", "inetOrgPerson"))
                    .build();

    public static final LdapObjectDefinition PARENT_GROUP_USER =
            LdapObjectDefinition.builder("ParentGroupUser")
                    .setDistinguishedName("uid=parentgroupuser,ou=Asia,dc=10.25.150.158")
                    .setAttributes(ImmutableMap.of(
                            "cn", "ParentGroupUser",
                            "sn", "ParentGroupUser",
                            "password", "LDAPPass123",
                            "memberOf", "ParentGroup"
                    ))
                    .addObjectClasses(Arrays.asList("person", "inetOrgPerson"))
                    .build();

    public static final LdapObjectDefinition CHILD_GROUP_USER =
            LdapObjectDefinition.builder("ChildGroupUser")
                    .setDistinguishedName("uid=childgroupuser,ou=Asia,dc=10.25.150.158")
                    .setAttributes(ImmutableMap.of(
                            "cn", "ChildGroupUser",
                            "sn", "ChildGroupUser",
                            "password", "LDAPPass123",
                            "memberOf", "ChildGroup"
                    ))
                    .addObjectClasses(Arrays.asList("person", "inetOrgPerson"))
                    .build();

    public static final LdapObjectDefinition USER_IN_MULTIPLE_GROUPS =
            LdapObjectDefinition.builder("UserInMultipleGroups")
                    .setDistinguishedName("uid=userinmultiplegroups,ou=Asia,dc=10.25.150.158")
                    .setAttributes(ImmutableMap.of(
                            "cn", "UserInMultipleGroups",
                            "sn", "UserInMultipleGroups",
                            "password", "LDAPPass123",
                            "memberOf", "DefaultGroup",
                            "memberOf", "AnotherGroup"
                    ))
                    .addObjectClasses(Arrays.asList("person", "inetOrgPerson"))
                    .build();

    public static final LdapObjectDefinition ORPHAN_USER =
            LdapObjectDefinition.builder("OrphanUser")
                    .setDistinguishedName("uid=orphanuser,ou=Asia,dc=10.25.150.158")
                    .setAttributes(ImmutableMap.of(
                            "cn", "OrphanUser",
                            "sn", "OrphanUser",
                            "password", "LDAPPass123"
                    ))
                    .addObjectClasses(Arrays.asList("person", "inetOrgPerson"))
                    .build();
}
