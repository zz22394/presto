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
package com.facebook.presto.cli;

import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.PrestoServerException;
import com.facebook.presto.spi.PrestoException.SerializedPrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import io.airlift.http.client.HttpStatus;

import java.io.EOFException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ConnectException;

import static com.google.common.base.Throwables.getCausalChain;
import static java.lang.String.format;

public class ErrorMessages
{
    private static final String PRESTO_COORDINATOR_NOT_FOUND = "There was a problem with a response from Presto Coordinator.\n";
    private static final String PRESTO_COORDINATOR_404 = "Presto HTTP interface returned 404 (file not found).\n";
    private static final String PRESTO_STARTING_UP  = "Presto is still starting up.\n";
    private static final String PRESTO_SHUTTING_DOWN  = "Presto is shutting down\n";

    private static final String TECHNICAL_DETAILS_HEADER = "\n=========   TECHNICAL DETAILS   =========\n";
    private static final String ERROR_MESSAGE_INTRO = "Error message:\n";
    private static final String STACKTRACE_INTRO = "Stack trace:\n";
    private static final String TECHNICAL_DETAILS_END = "========= TECHNICAL DETAILS END =========\n\n";

    private enum Tips {
        VERIFY_PRESTO_RUNNING("Verify that Presto is running on %s."),
        DEFINE_SERVER_AS_CLI_PARAM("Use '--server' argument when starting Presto CLI to define server host and port."),
        CHECK_NETWORK("Check the network conditions between client and server."),
        USE_DEBUG_MODE("Use '--debug' argument to get more technical details."),
        WAIT_FOR_INITIALIZATION("Wait for server to complete initialization."),
        WAIT_FOR_SERVER_RESTART("Wait for server to restart as it may have restart scheduled."),
        START_SERVER_AGAIN("Start server again manually."),
        CHECK_OTHER_HTTP_SERVICE("Make sure that none other HTTP service is running on %s."),
        CLIENT_IS_UP_TO_DATE_WITH_SERVER("Update CLI to match Presto server version.");

        private static final String TIPS_INTRO = "To solve this problem you may try to:\n";

        private final String message;

        private Tips(String message)
        {
            this.message = message;
        }

        @Override
        public String toString()
        {
            return message;
        }

        public static Builder builder()
        {
            return new Builder();
        }

        private static class Builder
        {
            private StringBuilder builder = new StringBuilder();

            public Builder()
            {
                builder.append(TIPS_INTRO);
            }

            public Builder addTip(Tips tip, Object ...toFormat)
            {
                builder.append(format(" * " + tip.toString() + "\n", toFormat));
                return this;
            }

            public String build()
            {
                return builder.toString();
            }
        }
    }

    private final StringBuilder builder = new StringBuilder();

    public static String createErrorMessage(Throwable throwable, ClientSession session)
    {
        if (throwable instanceof PrestoServerException) {
            return prestoServerExceptionErrorMesage((PrestoServerException) throwable, session);
        }
        else {
            return runtimeExceptionErrorMessage(throwable, session);
        }
    }

    private static String prestoServerExceptionErrorMesage(PrestoServerException serverException, ClientSession session)
    {
        StringBuilder builder = new StringBuilder();

        if (serverException.getServerException().isPresent()) {
            SerializedPrestoException exception = serverException.getServerException().get();
            if (exception.getErrorCode().equals(StandardErrorCode.SERVER_STARTING_UP)) {
                serverStartingUpErrorMessage(builder, session);
            }
            else if (exception.getErrorCode().equals(StandardErrorCode.SERVER_SHUTTING_DOWN)) {
                serverShuttingDownErrorMessage(builder, session);
            }
        }
        else if (serverException.getResponse().getStatusCode() == HttpStatus.NOT_FOUND.code()) {
            serverFileNotFoundErrorMessage(builder, session);
        }
        else {
            builder.append(serverException.getMessage());
        }

        if (session.isDebug()) {
            technicalDetailsPrestoServerExceptionErrorMessage(builder, serverException, session);
        }

        return builder.toString();
    }

    private static String runtimeExceptionErrorMessage(Throwable throwable, ClientSession session)
    {
        StringBuilder builder = new StringBuilder();

        if (getCausalChain(throwable).stream().anyMatch(x -> x instanceof EOFException || x instanceof ConnectException)) {
            serverNotFoundErrorMessage(builder, session);
        }
        else {
            // We have no clue about what went wrong, just display what we obtained.
            builder.append("Error running command:\n" + throwable.getMessage() + "\n");
        }

        if (session.isDebug()) {
            technicalDetailsRuntimeExceptionErrorMessage(builder, throwable, session);
        }

        return builder.toString();
    }

    private static String getStackTraceString(Throwable throwable)
    {
        StringWriter writer = new StringWriter();
        throwable.printStackTrace(new PrintWriter(writer));
        return writer.toString();
    }

    //region Messages for given problems
    private static void serverNotFoundErrorMessage(StringBuilder builder, ClientSession session)
    {
        builder.append(PRESTO_COORDINATOR_NOT_FOUND);
        Tips.Builder tipsBuilder = Tips.builder();
        tipsBuilder.addTip(Tips.VERIFY_PRESTO_RUNNING, session.getServer())
                .addTip(Tips.DEFINE_SERVER_AS_CLI_PARAM)
                .addTip(Tips.CHECK_NETWORK).build();
        if (!session.isDebug()) {
            tipsBuilder.addTip(Tips.USE_DEBUG_MODE);
        }
        builder.append(tipsBuilder.build());
    }

    private static void serverStartingUpErrorMessage(StringBuilder builder, ClientSession session)
    {
        builder.append(PRESTO_STARTING_UP);
        Tips.Builder tipsBuilder = Tips.builder();
        tipsBuilder.addTip(Tips.WAIT_FOR_INITIALIZATION);
        if (!session.isDebug()) {
            tipsBuilder.addTip(Tips.USE_DEBUG_MODE);
        }
        builder.append(tipsBuilder.build());
    }

    private static void serverShuttingDownErrorMessage(StringBuilder builder, ClientSession session)
    {
        builder.append(PRESTO_SHUTTING_DOWN);
        Tips.Builder tipsBuilder = Tips.builder();
        tipsBuilder.addTip(Tips.WAIT_FOR_SERVER_RESTART)
                .addTip(Tips.START_SERVER_AGAIN);
        if (!session.isDebug()) {
            tipsBuilder.addTip(Tips.USE_DEBUG_MODE);
        }
        builder.append(tipsBuilder.build());
    }

    private static void serverFileNotFoundErrorMessage(StringBuilder builder, ClientSession session)
    {
        builder.append(PRESTO_COORDINATOR_404);
        Tips.Builder tipsBuilder = Tips.builder();
        tipsBuilder.addTip(Tips.VERIFY_PRESTO_RUNNING, session.getServer())
                .addTip(Tips.DEFINE_SERVER_AS_CLI_PARAM)
                .addTip(Tips.CHECK_NETWORK)
                .addTip(Tips.CHECK_OTHER_HTTP_SERVICE)
                .addTip(Tips.CLIENT_IS_UP_TO_DATE_WITH_SERVER);
        if (!session.isDebug()) {
            tipsBuilder.addTip(Tips.USE_DEBUG_MODE);
        }
        builder.append(tipsBuilder.build());
    }

    private static void technicalDetailsPrestoServerExceptionErrorMessage(StringBuilder builder, PrestoServerException serverException, ClientSession session)
    {
        builder.append(TECHNICAL_DETAILS_HEADER);
        builder.append(STACKTRACE_INTRO);
        if (serverException.getServerException().isPresent()) {
            builder.append(getStackTraceString(serverException));
        }
        else {
            builder.append(serverException.getServerException().get().getStackTraceString());
        }
        builder.append(TECHNICAL_DETAILS_END);
    }

    private static void technicalDetailsRuntimeExceptionErrorMessage(StringBuilder builder, Throwable throwable, ClientSession session)
    {
        builder.append(TECHNICAL_DETAILS_HEADER);
        builder.append(ERROR_MESSAGE_INTRO);
        builder.append(throwable.getMessage() + "\n");
        builder.append(STACKTRACE_INTRO);
        builder.append(getStackTraceString(throwable));
        builder.append(TECHNICAL_DETAILS_END);
    }

    //endregion
}
