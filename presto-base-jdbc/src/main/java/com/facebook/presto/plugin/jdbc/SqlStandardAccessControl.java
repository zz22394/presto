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

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.ConnectorAccessControl;
import com.facebook.presto.spi.security.Identity;

import javax.inject.Inject;

import static com.facebook.presto.plugin.jdbc.PostgreSqlPrivilege.DELETE;
import static com.facebook.presto.plugin.jdbc.PostgreSqlPrivilege.INSERT;
import static com.facebook.presto.plugin.jdbc.PostgreSqlPrivilege.OWNERSHIP;
import static com.facebook.presto.plugin.jdbc.PostgreSqlPrivilege.SELECT;
import static com.facebook.presto.plugin.jdbc.PostgreSqlPrivilege.SELECT_WITH_GRANT;
import static com.facebook.presto.plugin.jdbc.PostgreSqlPrivilege.USAGE;

import static com.facebook.presto.spi.security.AccessDeniedException.denyAddColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDeleteTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyInsertTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectView;
import static com.facebook.presto.spi.security.AccessDeniedException.denySetCatalogSessionProperty;
import static java.util.Objects.requireNonNull;

public class SqlStandardAccessControl
        implements ConnectorAccessControl
{
    private static final String ADMIN_ROLE_NAME = "postgres";
    private static final String INFORMATION_SCHEMA_NAME = "information_schema";

    private final JdbcClient jdbcClient;
    private final String connectorId;
    private final boolean allowDropTable;

    @Inject
    public SqlStandardAccessControl(JdbcClient jdbcClient, JdbcConnectorId connectorId, JdbcMetadataConfig jdbcMetadataConfig)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.connectorId = requireNonNull(connectorId, "jdbcConnectorId is null").toString();
        requireNonNull(jdbcMetadataConfig, "hiveClientConfig is null");
        allowDropTable = jdbcMetadataConfig.isAllowDropTable();
    }

    @Override
    public void checkCanCreateTable(Identity identity, SchemaTableName tableName)
    {
        if (!checkSchemaPermission(identity, tableName.getSchemaName(), OWNERSHIP)) {
            denyCreateTable(tableName.toString());
        }
    }

    @Override
    public void checkCanDropTable(Identity identity, SchemaTableName tableName)
    {
        if (!allowDropTable || !checkTablePermission(identity, tableName, OWNERSHIP)) {
            denyDropTable(tableName.toString());
        }
    }

    @Override
    public void checkCanRenameTable(Identity identity, SchemaTableName tableName, SchemaTableName newTableName)
    {
        if (!checkTablePermission(identity, tableName, OWNERSHIP)) {
            denyRenameTable(tableName.toString(), newTableName.toString());
        }
    }

    @Override
    public void checkCanAddColumn(Identity identity, SchemaTableName tableName)
    {
        if (!checkTablePermission(identity, tableName, OWNERSHIP)) {
            denyAddColumn(tableName.toString());
        }
    }

    @Override
    public void checkCanRenameColumn(Identity identity, SchemaTableName tableName)
    {
        if (!checkTablePermission(identity, tableName, OWNERSHIP)) {
            denyRenameColumn(tableName.toString());
        }
    }

    @Override
    public void checkCanSelectFromTable(Identity identity, SchemaTableName tableName)
    {
        if (!checkTablePermission(identity, tableName, SELECT)) {
            denySelectTable(tableName.toString());
        }
    }

    @Override
    public void checkCanInsertIntoTable(Identity identity, SchemaTableName tableName)
    {
        if (!checkTablePermission(identity, tableName, INSERT)) {
            denyInsertTable(tableName.toString());
        }
    }

    @Override
    public void checkCanDeleteFromTable(Identity identity, SchemaTableName tableName)
    {
        if (!checkTablePermission(identity, tableName, DELETE)) {
            denyDeleteTable(tableName.toString());
        }
    }

    @Override
    public void checkCanCreateView(Identity identity, SchemaTableName viewName)
    {
        if (!checkSchemaPermission(identity, viewName.getSchemaName(), OWNERSHIP)) {
            denyCreateView(viewName.toString());
        }
    }

    @Override
    public void checkCanDropView(Identity identity, SchemaTableName viewName)
    {
        if (!checkTablePermission(identity, viewName, OWNERSHIP)) {
            denyDropView(viewName.toString());
        }
    }

    @Override
    public void checkCanSelectFromView(Identity identity, SchemaTableName viewName)
    {
        if (!checkTablePermission(identity, viewName, SELECT)) {
            denySelectView(viewName.toString());
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromTable(Identity identity, SchemaTableName tableName)
    {
        if (!checkTablePermission(identity, tableName, SELECT_WITH_GRANT)) {
            //  if (!checkTablePermission(identity, tableName, PostgreSqlPrivilege.SELECT, PostgreSqlPrivilege.GRANT)) {
            denySelectTable(tableName.toString());
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromView(Identity identity, SchemaTableName viewName)
    {
        if (!checkTablePermission(identity, viewName, SELECT_WITH_GRANT)) {
            denySelectView(viewName.toString());
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String propertyName)
    {
        if (!jdbcClient.hasRolePrivilege(identity.getUser(), ADMIN_ROLE_NAME, USAGE)) {
            denySetCatalogSessionProperty(connectorId, propertyName);
        }
    }

    private boolean checkSchemaPermission(Identity identity, String schemaName, PostgreSqlPrivilege requiredPrivileges)
    {
        if (requiredPrivileges.equals(OWNERSHIP)) {
            return jdbcClient.isDatabaseOwner(identity.getUser(), schemaName);
        }
        else {
            return jdbcClient.hasSchemaPrivilege(schemaName, identity.getUser(), requiredPrivileges);
        }
    }

    private boolean checkTablePermission(Identity identity, SchemaTableName tableName, PostgreSqlPrivilege requiredPrivileges)
    {
        if (INFORMATION_SCHEMA_NAME.equals(tableName.getSchemaName())) {
            return true;
        }

        /*build sql; execute query; get permissions back;
        //String sql = jdbcClient.buildSql(split, columnHandles); figure out a way to execute a sql query that contains function
        // to build a sql query, you need: jdbcClient, split, columnHandles
        Looked at many classes and functions. You can do getConnection(), getStatement, and then executeQuery() but you
        require different variables such as jdbcClient and jdbcSplit, which are not propagated here.
        IMO, you will have to implement a new method in JdbcMetadata that executes a SELECT query with a function inside it
        (first, write a method that executes a simple SELECT query)
        */
        //String sql = jdbcClient.buildSql(jdbcSplit, ImmutableList.of())

        //String getPermissionsQuery =  new QueryBuilder()

        if (requiredPrivileges.equals(OWNERSHIP)) {
            return jdbcClient.isTableOwner(identity.getUser(), tableName);
        }
        else {
            return jdbcClient.hasTablePrivilege(tableName, identity.getUser(), requiredPrivileges);
        }
    }
}
