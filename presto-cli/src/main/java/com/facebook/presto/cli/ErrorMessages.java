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

import java.io.EOFException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ConnectException;

import static com.google.common.base.Throwables.getCausalChain;

public class ErrorMessages
{
    public static String createErrorMessage(Throwable throwable, ClientSession session)
    {
        StringBuilder builder = new StringBuilder();
        if (getCausalChain(throwable).stream().anyMatch(x -> x instanceof EOFException || x instanceof ConnectException)) {
            builder.append("There was a problem with a response from Presto Coordinator.\n");
            builder.append("To solve this problem you may try to:\n");
            builder.append(" * Verify that Presto is running on " + session.getServer() + "\n");
            builder.append(" * Use '--server' argument when starting Presto CLI to define server host and port.\n");
            builder.append(" * Check the network conditions between client and server.\n");
            if (!session.isDebug()) {
                builder.append(" * Use '--debug' argument to get more technical details.\n");
            }
        }
        else {
            builder.append("Error running command: " + throwable.getMessage() + "\n");
        }

        if (session.isDebug()) {
            builder.append("\n=========   TECHNICAL DETAILS   =========\n");
            builder.append("Error message:\n");
            builder.append(throwable.getMessage() + "\n");
            builder.append("Stack trace:\n");
            builder.append(getStackTraceString(throwable));
            builder.append("========= TECHNICAL DETAILS END =========\n\n");
        }

        return builder.toString();
    }

    private static String getStackTraceString(Throwable throwable)
    {
        StringWriter writer = new StringWriter();
        throwable.printStackTrace(new PrintWriter(writer));
        return writer.toString();
    }
}
