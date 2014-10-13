/*   Copyright (C) 2013-2014 Computer Sciences Corporation
 *
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
 * limitations under the License. */

package ezbake.services.graph.cmd;

import ezbake.base.thrift.EzBakeBaseService;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.DirectoryConfigurationLoader;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.services.graph.thrift.EzGraphService;
import ezbake.thrift.ThriftClientPool;
import org.apache.thrift.TException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;

public class GraphCMDTool {

    @Option(name="-f", aliases = "--configFile",
            usage="The configuration file for the properties",
            required = true)
    private String configFile;

    @Option(name="-c", aliases = "--create",
            usage="RunCreateSchema",
            required = true)
    private String runCreate;

    ThriftClientPool pool;

    public static void main(String[] args) {
        log("Initializing command");
        GraphCMDTool tool = new GraphCMDTool();
        CmdLineParser parser = new CmdLineParser(tool);

        try {
            parser.parseArgument(args);
            tool.run();
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    public void run() throws IOException, TException, EzConfigurationLoaderException {
        log("Searching for Ezbake properties");
        Properties props = new Properties();
        DirectoryConfigurationLoader loader = new DirectoryConfigurationLoader(Paths.get(configFile));
        props = loader.loadConfiguration();
        log("Found Ezbake properties:");
        log(props);
        pool = new ThriftClientPool(props);

        GraphClient graphClient = new GraphClient(props);
        log("Initialized GraphClient\nSending basicGraph");
        EzGraphService.Client client = getClient(props);
        try {
            graphClient.sendBasicGraph(client, getToken(new EzbakeSecurityClient(props)),
                    (runCreate.equals("true") ? true : false));
        } finally {
            pool.returnToPool(client);
        }
    }

    private EzGraphService.Client getClient(Properties props) throws IOException {
        try {
            log("Obtaining GraphService.Client");
            return pool.getClient("graph-service", EzGraphService.Client.class);
        } catch (TException e) {
            log("Error Retrieving GraphService Client " + e.getMessage());
            log("Error Retrieving GraphService Client 2 {}" + e);
            throw new IOException("Error Retrieving GraphService Client", e);
        }
    }

    private EzSecurityToken getToken(EzbakeSecurityClient client) throws IOException {
        try {
            log("Obtaining EzSecurityToken");
            log(client.toString());
            EzSecurityToken token =  client.fetchAppToken();
            log("Outputting token:");
            log(token);
            return token;
        } catch (TException e) {
            log("Error Retrieving EzSecurityToken Client" + e.getMessage());
            log("Could not push data to graph service {} " + e);
            throw new IOException("Could not push data to graph service", e);
        }
    }

    public static void log(Object obj) {
        System.out.println(obj);
    }
}
