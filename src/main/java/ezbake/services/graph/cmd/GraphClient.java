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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import ezbake.base.thrift.EzBakeBaseService;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.Visibility;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.services.graph.thrift.EzGraphService;
import ezbake.services.graph.thrift.GraphName;
import ezbake.services.graph.thrift.InvalidRequestException;
import ezbake.services.graph.thrift.types.*;
import ezbake.services.graph.types.GraphConverter;
import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;
import jline.internal.TestAccessible;
import org.apache.thrift.TException;
import ezbake.thrift.ThriftClientPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static ezbake.services.graph.cmd.GraphCMDTool.log;

public class GraphClient {
    private static final Logger logger = LoggerFactory.getLogger(GraphClient.class);

    private String               appId;
    private static final String  KEY_NAME = "NAME";

    private final GraphName graphName = new GraphName(); // loads default
    private final Visibility visibility = new Visibility();
    private final String applicationName = "testGraph";
    private final List<PropertyKey> keys;
    private final List<EdgeLabel> labels = Lists.newArrayList(new EdgeLabel("friend"));

    public GraphClient(Properties props) {
        keys = makeIndexableKeys();

        initialize(props);
    }

    private List<PropertyKey> makeIndexableKeys() {
        log("creating KEYS");
        PropertyKey key = new PropertyKey(KEY_NAME);
        key.setDataType(DataType.STRING);
        Index index = new Index(Element.VERTEX);
        index.setIndexName(IndexName.SEARCH);
        List<Index> list = new ArrayList<Index>();
        list.add(index);
        key.setIndices(list);

        return Lists.newArrayList(key);
    }

    /**
     * Initializes the quarantine processor object with the provided configuration.
     * The appId is used to communicate with the data warehouse and to obtain the security
     * token.
     * The client pool is used to communicate with the security service and quarantine service
     * @param config configurations to use for initialization
     */
    private void initialize(Properties config) {
        config.setProperty("ezbake.security.app.id", "client");
        logger.info("config properties {}", config);

        appId = (new EzBakeApplicationConfigurationHelper(config)).getSecurityID();
        visibility.setFormalVisibility("U//FOUO");
    }

    public void sendBasicGraph(EzGraphService.Iface client, EzSecurityToken token, boolean runCreateSchema) throws IOException, TException {
        log("Starting sendBasicGraph()");

        log("Obtained clients\nCreating Schema");
        if (runCreateSchema) {
            createSchema(client, token);
            log("Created Schema");
        }

        Graph graph = new Graph();

        log("Created Vertex");
        ElementId id1 = ElementId.localId("1");
        Vertex v1 = new Vertex(id1);
        Map<String, List<Property>> props = Maps.newHashMap();
        List<Property> propsList = Lists.newArrayList();
        propsList.add((new Property(PropValue.string_val("stevejobs"))).setVisibility(visibility));
        props.put(KEY_NAME, propsList);
        v1.setProperties(props);

        ElementId id2 = ElementId.localId("2");
        Vertex v2 = new Vertex(id2);
        Map<String, List<Property>> props2 = Maps.newHashMap();
        propsList = Lists.newArrayList();
        propsList.add((new Property(PropValue.string_val("stevewoz"))).setVisibility(visibility));
        props2.put(KEY_NAME, propsList);
        v2.setProperties(props2);

        log("Creating Edge");

        Edge edge = new Edge(id1, id2, "friend");
        edge.setVisibility(visibility);
        Map<String, Property> map = Maps.newTreeMap();
        map.put(KEY_NAME, new Property(PropValue.string_val("friendz")));
        map.get(KEY_NAME).setVisibility(visibility);
        edge.setProperties(map);
        graph.addToEdges(edge);
        graph.addToVertices(v1);
        graph.addToVertices(v2);

        log("Created Graph");

        writeGraph(client, token, graph);
    }

    public List<Vertex> findVertices(EzGraphService.Iface client, EzSecurityToken token) throws TException {
        return client.findVertices(graphName, KEY_NAME, PropValue.string_val("stevejobs"), token);
    }

    private void writeGraph(EzGraphService.Iface client, EzSecurityToken token, Graph graph) {
        try {
            client.writeGraph(applicationName, visibility, graphName, graph, token);
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    private void createSchema(EzGraphService.Iface client, EzSecurityToken token) {
        try {
            client.createSchema(applicationName, visibility, graphName, keys, labels, token);
        } catch (TException e) {
            e.printStackTrace();
        }
    }
}
