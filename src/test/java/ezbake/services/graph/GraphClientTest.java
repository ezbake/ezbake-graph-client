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

package ezbake.services.graph;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.Visibility;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.data.test.TestUtils;
import ezbake.services.graph.archive.MockTransactionArchive;
import ezbake.services.graph.cmd.GraphClient;
import ezbake.services.graph.thrift.types.Vertex;
import ezbake.services.graph.types.GraphConverter;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static junit.framework.TestCase.assertTrue;

public class GraphClientTest {

    private EzConfiguration ezConfiguration;
    private GraphDataSetHandler handler;
    private Visibility transArchiveClassi;

    private static final EzSecurityToken U_Auth_Token;

    static {
        U_Auth_Token = TestUtils.createTestToken(new String[]{"U//FOUO"});
    }

    @Before
    public void setUp() throws EzConfigurationLoaderException {
        ClasspathConfigurationLoader loader = new ClasspathConfigurationLoader();
        Properties properties = loader.loadConfiguration();
        ezConfiguration = new EzConfiguration(loader);

        TestUtils.addSettingsForMock(properties);

        // these are ez configs that would've been set by the infrastructure
        // these will overwrite what's in the spring context
        properties.put("accumulo.instance.name", "spock");
        properties.put("accumulo.zookeepers", "localhost");
        properties.put("accumulo.username", "root");
        properties.put("accumulo.password", "");
        properties.put("accumulo.use.mock", "false");

        handler = new GraphDataSetHandler(new MockTransactionArchive());
        handler.setConfigurationProperties(properties);
        handler.getThriftProcessor(); // calls init();

        transArchiveClassi = new Visibility();
        transArchiveClassi.setFormalVisibility("U");
    }

    @After
    public void tearDown() throws Exception {
        handler.shutdown();
    }

    @Test
    public void basicTest() throws IOException, TException {
        GraphClient client = new GraphClient(ezConfiguration.getProperties());

        client.sendBasicGraph(handler, U_Auth_Token, true);
        List<Vertex> vlist = client.findVertices(handler, U_Auth_Token);
        assertTrue(vlist.size() == 1);
    }
}
