/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.util.shell.commands;

import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.format.Formatter;
import org.apache.accumulo.core.util.format.TeeFormatter;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.accumulo.core.util.shell.Token;
import org.apache.commons.cli.CommandLine;

/**
 * copies scan output to an accumulo table as well as displaying to screen. An
 * error is thrown in the tee table is the sample as the current table.
 */
public class TeeCommand extends Command {
    
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
        String tableName = cl.getArgs()[0];
        String currentTableName = shellState.getTableName();
        if (currentTableName.equals(tableName)) {
            throw new RuntimeException("You can't tee to the current table.");
        }
        if (!shellState.getConnector().tableOperations().exists(tableName)) {
            shellState.getConnector().tableOperations().create(tableName);
        }

        String subcommand = cl.getArgs()[1];
        if ("on".equals(subcommand)) {
            shellState.setTeeTableName(tableName);
            shellState.getConnector().tableOperations().setProperty(shellState.getTableName(), Property.TABLE_FORMATTER_CLASS.toString(), TeeFormatter.class.getName());

        } else if ("off".equals(subcommand)) {
            shellState.setTeeTableName(null);
            shellState.getConnector().tableOperations().removeProperty(shellState.getTableName(), Property.TABLE_FORMATTER_CLASS.toString());
        }
        
        return 0;
    }

    @Override
    public String description() {
        return "tee scan output to specified table (must not be the current table).";
    }

    @Override
    public void registerCompletion(Token root, Map<Command.CompletionSet, Set<String>> special) {
        registerCompletionForTables(root, special);
    }

    @Override
    public String usage() {
        return getName() + " <tableName> <on|off>";
    }

    @Override
    public int numArgs() {
        return 2;
    }
}