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
package com.facebook.presto.tests.cli;

import com.facebook.presto.cli.Presto;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.teradata.tempto.AfterTestWithContext;
import com.teradata.tempto.ProductTest;
import com.teradata.tempto.Requirement;
import com.teradata.tempto.RequirementsProvider;
import com.teradata.tempto.configuration.Configuration;
import com.teradata.tempto.fulfillment.table.ImmutableTableRequirement;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.facebook.presto.tests.TestGroups.ACTIVE_DIRECTORY;
import static com.facebook.presto.tests.TestGroups.CLI;
import static com.facebook.presto.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.readLines;
import static com.teradata.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;
import static com.teradata.tempto.process.CliProcess.trimLines;
import static com.teradata.tempto.process.JavaProcessLauncher.defaultJavaProcessLauncher;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;

public class PrestoCliTests
        extends ProductTest
        implements RequirementsProvider
{
    private static final long TIMEOUT = 300 * 1000; // 30 secs per test
    private static final String EXIT_COMMAND = "exit";
    private static final String ACTIVE_DIRECTORY_INVALID_CREDNTIALS_ERROR = "AcceptSecurityContext error, data 52e";

    private final List<String> nationTableInteractiveLines;
    private final List<String> nationTableBatchLines;

    @Inject
    @Named("databases.presto.host")
    private String serverHost;

    @Inject
    @Named("databases.presto.server_address")
    private String serverAddress;

    @Inject(optional = true)
    @Named("databases.presto.cli_kerberos_authentication")
    private boolean kerberosAuthentication;

    @Inject(optional = true)
    @Named("databases.presto.cli_kerberos_principal")
    private String kerberosPrincipal;

    @Inject(optional = true)
    @Named("databases.presto.cli_kerberos_keytab")
    private String kerberosKeytab;

    @Inject(optional = true)
    @Named("databases.presto.cli_kerberos_config_path")
    private String kerberosConfigPath;

    @Inject(optional = true)
    @Named("databases.presto.cli_kerberos_service_name")
    private String kerberosServiceName;

    @Inject(optional = true)
    @Named("databases.presto.cli_keystore")
    private String keystorePath;

    @Inject(optional = true)
    @Named("databases.presto.cli_keystore_password")
    private String keystorePassword;

    @Inject(optional = true)
    @Named("databases.presto.cli_kerberos_use_canonical_hostname")
    private boolean kerberosUseCanonicalHostname;

    @Inject
    @Named("databases.presto.jdbc_user")
    private String jdbcUser;

    @Inject(optional = true)
    @Named("databases.presto.cli_ldap_authentication")
    private boolean ldapAuthentication;

    @Inject(optional = true)
    @Named("databases.presto.cli_ldap_truststore_path")
    private String ldapTruststorePath;

    @Inject(optional = true)
    @Named("databases.presto.cli_ldap_truststore_password")
    private String ldapTruststorePassword;

    @Inject(optional = true)
    @Named("databases.presto.cli_ldap_user_name")
    private String ldapUserName;

    @Inject(optional = true)
    @Named("databases.presto.cli_ldap_server_address")
    private String ldapServerAddress;

    @Inject(optional = true)
    @Named("databases.presto.cli_ldap_user_password")
    private String ldapUserPassword;

    private PrestoCliProcess presto;

    public PrestoCliTests()
            throws IOException
    {
        nationTableInteractiveLines = readLines(getResource("com/facebook/presto/tests/cli/interactive_query.results"), UTF_8);
        nationTableBatchLines = readLines(getResource("com/facebook/presto/tests/cli/batch_query.results"), UTF_8);
    }

    @AfterTestWithContext
    public void stopPresto()
            throws InterruptedException
    {
        if (presto != null) {
            presto.getProcessInput().println(EXIT_COMMAND);
            presto.waitForWithTimeoutAndKill();
        }
    }

    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return new ImmutableTableRequirement(NATION);
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldDisplayVersion()
            throws IOException, InterruptedException
    {
        launchPrestoCli("--version");
        String version = firstNonNull(Presto.class.getPackage().getImplementationVersion(), "(version unknown)");
        assertThat(presto.readRemainingOutputLines()).containsExactly("Presto CLI " + version);
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldRunQuery()
            throws IOException, InterruptedException
    {
        launchPrestoCliWithServerArgument();
        presto.waitForPrompt();
        presto.getProcessInput().println("select * from hive.default.nation;");
        assertThat(trimLines(presto.readLinesUntilPrompt())).containsAll(nationTableInteractiveLines);
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldRunBatchQuery()
            throws IOException, InterruptedException
    {
        launchPrestoCliWithServerArgument("--execute", "select * from hive.default.nation;");

        assertThat(trimLines(presto.readRemainingOutputLines())).containsAll(nationTableBatchLines);
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldUseCatalogAndSchemaOptions()
            throws IOException, InterruptedException
    {
        launchPrestoCliWithServerArgument("--catalog", "hive", "--schema", "default", "--execute", "select * from nation;");
        assertThat(trimLines(presto.readRemainingOutputLines())).containsAll(nationTableBatchLines);
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldRunQueryFromFile()
            throws IOException, InterruptedException
    {
        File temporayFile = File.createTempFile("test-sql", null);
        temporayFile.deleteOnExit();
        Files.write("select * from hive.default.nation;\n", temporayFile, UTF_8);

        launchPrestoCliWithServerArgument("--file", temporayFile.getAbsolutePath());
        assertThat(trimLines(presto.readRemainingOutputLines())).containsAll(nationTableBatchLines);
    }
/*
    @Test(groups = {CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldPassQueryForLdapUserInMultipleGroups()
            throws IOException, InterruptedException
    {
        ldapUserName = "UserInMultipleGroups";
        launchPrestoCliWithServerArgument("--catalog", "hive", "--schema", "default", "--execute", "select * from nation;");
        assertThat(trimLines(presto.readRemainingOutputLines())).containsAll(nationTableBatchLines);
    }
*/
    @Test(groups = {CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForLdapUserInChildGroup()
            throws IOException, InterruptedException
    {
        ldapUserName = "ChildGroupUser";
        launchPrestoCliWithServerArgument("--catalog", "hive", "--schema", "default", "--execute", "select * from nation;");
        assertTrue(trimLines(presto.readRemainingErrorLines()).stream().anyMatch(str -> str.contains("Authentication failed: User " + ldapUserName + " not a member of the group")));
    }

    @Test(groups = {CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForLdapUserInParentGroup()
            throws IOException, InterruptedException
    {
        ldapUserName = "GrandParentGroupUser";
        launchPrestoCliWithServerArgument("--catalog", "hive", "--schema", "default", "--execute", "select * from nation;");
        assertTrue(trimLines(presto.readRemainingErrorLines()).stream().anyMatch(str -> str.contains("Authentication failed: User " + ldapUserName + " not a member of the group")));
    }

    @Test(groups = {CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForOrphanLdapUser()
            throws IOException, InterruptedException
    {
        ldapUserName = "OrphanUser";
        launchPrestoCliWithServerArgument("--catalog", "hive", "--schema", "default", "--execute", "select * from nation;");
        assertTrue(trimLines(presto.readRemainingErrorLines()).stream().anyMatch(str -> str.contains("Authentication failed: User " + ldapUserName + " not a member of the group")));
    }

    @Test(groups = {CLI, ACTIVE_DIRECTORY, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForWrongLdapPassword()
            throws IOException, InterruptedException
    {
        ldapUserPassword = "wrong_password";
        launchPrestoCliWithServerArgument("--execute", "select * from hive.default.nation;");
        assertTrue(trimLines(presto.readRemainingErrorLines()).stream().anyMatch(str -> str.contains(ACTIVE_DIRECTORY_INVALID_CREDNTIALS_ERROR)));
    }

    @Test(groups = {CLI, ACTIVE_DIRECTORY, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForWrongLdapUser()
            throws IOException, InterruptedException
    {
        ldapUserName = "invalid_user";
        launchPrestoCliWithServerArgument("--execute", "select * from hive.default.nation;");
        assertTrue(trimLines(presto.readRemainingErrorLines()).stream().anyMatch(str -> str.contains(ACTIVE_DIRECTORY_INVALID_CREDNTIALS_ERROR)));
    }

    @Test(groups = {CLI, ACTIVE_DIRECTORY, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailForComputerContextType()
            throws IOException, InterruptedException
    {
        ldapUserPassword = "ad-test";
        launchPrestoCliWithServerArgument("--execute", "select * from hive.default.nation;");
        assertTrue(trimLines(presto.readRemainingErrorLines()).stream().anyMatch(str -> str.contains(ACTIVE_DIRECTORY_INVALID_CREDNTIALS_ERROR)));
    }

    @Test(groups = {CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForLdapWithoutPassword()
            throws IOException, InterruptedException
    {
        launchPrestoCli("--server", ldapServerAddress,
                "--truststore-path", ldapTruststorePath,
                "--truststore-password", ldapTruststorePassword,
                "--user", ldapUserName,
                "--execute", "select * from hive.default.nation;");
        assertTrue(trimLines(presto.readRemainingErrorLines()).stream().anyMatch(str -> str.contains("statusMessage=Unauthorized")));
    }

    @Test(groups = {CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForLdapWithoutHttps()
            throws IOException, InterruptedException
    {
        ldapServerAddress = format("http://%s:8443", serverHost);
        launchPrestoCliWithServerArgument("--execute", "select * from hive.default.nation;");
        assertTrue(trimLines(presto.readRemainingErrorLines()).stream().anyMatch(str -> str.contains("Authentication using username/password requires HTTPS to be enabled")));
        skipAfterTestWithContext();
    }

    private void skipAfterTestWithContext()
    {
        presto.close();
        presto = null;
    }

    @Test(groups = {CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailForIncorrectTrustStore()
            throws IOException, InterruptedException
    {
        ldapTruststorePassword = "wrong_password";
        launchPrestoCliWithServerArgument("--execute", "select * from hive.default.nation;");
        assertTrue(trimLines(presto.readRemainingErrorLines()).stream().anyMatch(str -> str.contains("Keystore was tampered with, or password was incorrect")));
        skipAfterTestWithContext();
    }

    private void launchPrestoCliWithServerArgument(String... arguments)
            throws IOException, InterruptedException
    {
        if (ldapAuthentication) {
            requireNonNull(ldapTruststorePath, "databases.presto.cli_ldap_truststore_path is null");
            requireNonNull(ldapTruststorePassword, "databases.presto.cli_ldap_truststore_password is null");
            requireNonNull(ldapUserName, "databases.presto.cli_ldap_user_name is null");
            requireNonNull(ldapServerAddress, "databases.presto.cli_ldap_server_address is null");
            requireNonNull(ldapUserPassword, "databases.presto.cli_ldap_user_password is null");

            ImmutableList.Builder<String> prestoClientOptions = ImmutableList.builder();
            prestoClientOptions.add(
                    "--server", ldapServerAddress,
                    "--truststore-path", ldapTruststorePath,
                    "--truststore-password", ldapTruststorePassword,
                    "--user", ldapUserName,
                    "--password");

            prestoClientOptions.add(arguments);
            launchPrestoCli(prestoClientOptions.build());
            setLdapPassword();
        }
        else if (kerberosAuthentication) {
            requireNonNull(kerberosPrincipal, "databases.presto.cli_kerberos_principal is null");
            requireNonNull(kerberosKeytab, "databases.presto.cli_kerberos_keytab is null");
            requireNonNull(kerberosServiceName, "databases.presto.cli_kerberos_service_name is null");
            requireNonNull(kerberosConfigPath, "databases.presto.cli_kerberos_config_path is null");
            requireNonNull(keystorePath, "databases.presto.cli_keystore is null");
            requireNonNull(keystorePassword, "databases.presto.cli_keystore_password is null");

            ImmutableList.Builder<String> prestoClientOptions = ImmutableList.builder();
            prestoClientOptions.add(
                    "--server", serverAddress,
                    "--user", jdbcUser,
                    "--enable-authentication",
                    "--krb5-principal", kerberosPrincipal,
                    "--krb5-keytab-path", kerberosKeytab,
                    "--krb5-remote-service-name", kerberosServiceName,
                    "--krb5-config-path", kerberosConfigPath,
                    "--keystore-path", keystorePath,
                    "--keystore-password", keystorePassword);
            if (!kerberosUseCanonicalHostname) {
                prestoClientOptions.add("--krb5-disable-remote-service-hostname-canonicalization");
            }
            prestoClientOptions.add(arguments);
            launchPrestoCli(prestoClientOptions.build());
        }
        else {
            ImmutableList.Builder<String> prestoClientOptions = ImmutableList.builder();
            prestoClientOptions.add(
                    "--server", serverAddress,
                    "--user", jdbcUser);
            prestoClientOptions.add(arguments);
            launchPrestoCli(prestoClientOptions.build());
        }
    }

    private void launchPrestoCli(String... arguments)
            throws IOException, InterruptedException
    {
        launchPrestoCli(asList(arguments));
    }

    private void launchPrestoCli(List<String> arguments)
            throws IOException, InterruptedException
    {
        presto = new PrestoCliProcess(defaultJavaProcessLauncher().launch(Presto.class, arguments));
    }

    private void setLdapPassword()
    {
        presto.waitForLdapPasswordPrompt();
        presto.getProcessInput().println(ldapUserPassword);
        presto.nextOutputToken();
    }
}
