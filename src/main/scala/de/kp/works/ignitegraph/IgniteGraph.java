package de.kp.works.ignitegraph;
/*
 * Copyright (c) 20129 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import de.kp.works.ignite.client.*;
import de.kp.works.ignitegraph.IndexMetadata.State;
import de.kp.works.ignitegraph.exception.IgniteGraphException;
import de.kp.works.ignitegraph.exception.IgniteGraphNotValidException;
import de.kp.works.ignitegraph.models.EdgeModel;
import de.kp.works.ignitegraph.models.VertexModel;
import de.kp.works.ignitegraph.process.strategy.optimization.IgniteGraphStepStrategy;
import de.kp.works.ignitegraph.process.strategy.optimization.IgniteVertexStepStrategy;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn("io.hgraphdb.StructureBasicSuite")
@Graph.OptIn("io.hgraphdb.CustomSuite")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest$Traversals",
        method = "g_VX1AsStringX_out_hasXid_2AsStringX",
        reason = "Attempts to retrieve an element that has a numeric id with a String")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest$Traversals",
        method = "g_EX11X_outV_outE_hasXid_10AsStringX",
        reason = "Attempts to retrieve an element that has a numeric id with a String")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest$Traversals",
        method = "g_VX1AsStringX_outXknowsX",
        reason = "Attempts to retrieve an element that has a numeric id with a String")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest$Traversals",
        method = "g_EX11AsStringX",
        reason = "Attempts to retrieve an element that has a numeric id with a String")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest$Traversals",
        method = "g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup",
        reason = "Requires metaproperties")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest$Traversals",
        method = "g_V_withSideEffectXsgX_outEXknowsX_subgraphXsgX_name_capXsgX",
        reason = "Requires metaproperties")
public class IgniteGraph implements Graph {

    private static final Logger LOGGER = LoggerFactory.getLogger(IgniteGraph.class);

    static {
        TraversalStrategies.GlobalCache.registerStrategies(IgniteGraph.class,
                TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(
                        IgniteVertexStepStrategy.instance(),
                        IgniteGraphStepStrategy.instance()
                ));
    }

    private final IgniteGraphConfiguration config;

    private final IgniteGraphFeatures features;
    private final IgniteAdmin admin;

    private final EdgeModel edgeModel;
    private final VertexModel vertexModel;

    private Cache<ByteBuffer, Edge> edgeCache;
    private Cache<ByteBuffer, Vertex> vertexCache;
    private Map<IndexMetadata.Key, IndexMetadata> indices = new ConcurrentHashMap<>();
    private Map<LabelMetadata.Key, LabelMetadata> labels = new ConcurrentHashMap<>();

    private Set<LabelConnection> labelConnections = Collections.newSetFromMap(new ConcurrentHashMap<>());
    //private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    /**
     * This method is invoked by Gremlin's GraphFactory
     * and defines the starting point for further tasks.
     */
    public static IgniteGraph open(final Configuration properties) throws IgniteGraphException {
        return new IgniteGraph(properties);
    }

    public IgniteGraph(Configuration properties) {
        this(new IgniteGraphConfiguration(properties));
    }

    public IgniteGraph(IgniteGraphConfiguration config) throws IgniteGraphException {
        this(config, IgniteGraphUtils.getConnection(config));
    }

    public IgniteGraph(IgniteGraphConfiguration config, IgniteConnection connection) throws IgniteGraphException {

        this.config = config;
        this.admin = connection.getAdmin();

        this.features = new IgniteGraphFeatures(true);
        /*
         * Create the Ignite caches that are used as the
         * backed for this graph implementation
         */
        try {
            IgniteGraphUtils.createTables(config, admin);
        } catch (Exception e) {
            throw new IgniteGraphException(e);
        }

        /* Edge & Vertex models */
        this.edgeModel = new EdgeModel(this,
                admin.getTable(IgniteGraphUtils.getTableName(config, IgniteConstants.EDGES)));

        this.vertexModel = new VertexModel(this,
                admin.getTable(IgniteGraphUtils.getTableName(config, IgniteConstants.VERTICES)));

        this.edgeCache = CacheBuilder.<ByteBuffer, Edge>newBuilder()
                .maximumSize(config.getElementCacheMaxSize())
                .expireAfterAccess(config.getElementCacheTtlSecs(), TimeUnit.SECONDS)
                .removalListener((RemovalListener<ByteBuffer, Edge>) notif -> ((IgniteEdge) notif.getValue()).setCached(false))
                .build();

        this.vertexCache = CacheBuilder.<ByteBuffer, Vertex>newBuilder()
                .maximumSize(config.getElementCacheMaxSize())
                .expireAfterAccess(config.getElementCacheTtlSecs(), TimeUnit.SECONDS)
                .removalListener((RemovalListener<ByteBuffer, Vertex>) notif -> ((IgniteVertex) notif.getValue()).setCached(false))
                .build();

        refreshSchema();

    }

    public EdgeModel getEdgeModel() {
        return edgeModel;
    }

    public VertexModel getVertexModel() {
        return vertexModel;
    }

    /** VERTEX RELATED **/

    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        /*
         * Vertices that define an `id` in the provided keyValues
         * use this value (long or numeric). Otherwise `null` is
         * returned.
         */
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        /*
         * The `idValue` either is a provided [Long] or a random
         * UUID as [String].
         */
        idValue = IgniteGraphUtils.generateIdIfNeeded(idValue);
        long now = System.currentTimeMillis();
        IgniteVertex newVertex = new IgniteVertex(this, idValue, label, now, now, IgniteGraphUtils.propertiesToMap(keyValues));
        newVertex.validate();
        newVertex.writeToModel();

        Vertex vertex = findOrCreateVertex(idValue);
        ((IgniteVertex) vertex).copyFrom(newVertex);
        return vertex;
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        if (vertexIds.length == 0) {
            return allVertices();
        } else {
            Stream<Object> stream = Stream.of(vertexIds);
            List<Vertex> vertices = stream
                    .map(id -> {
                        if (id == null)
                            throw Exceptions.argumentCanNotBeNull("id");
                        else if (id instanceof Long)
                            return id;
                        else if (id instanceof Number)
                            return ((Number) id).longValue();
                        else if (id instanceof Vertex)
                            return ((Vertex) id).id();
                        else
                            return id;
                    })
                    .map(this::findOrCreateVertex)
                    .collect(Collectors.toList());
            getVertexModel().load(vertices);
            return vertices.stream()
                    .filter(v -> ((IgniteVertex) v).arePropertiesFullyLoaded())
                    .iterator();
        }
    }

    public Vertex vertex(Object id) {
        if (id == null) {
            throw Exceptions.argumentCanNotBeNull("id");
        }
        Vertex v = findOrCreateVertex(id);
        ((IgniteVertex) v).load();
        return v;
    }

    public Vertex findOrCreateVertex(Object id) {
        return findVertex(id, true);
    }

    /**
     * Retrieve vertex from cache or build new
     * vertex instance with the provided `id`.
     */
    protected Vertex findVertex(Object id, boolean createIfNotFound) {
        if (id == null) {
            throw Exceptions.argumentCanNotBeNull("id");
        }
        id = IgniteGraphUtils.generateIdIfNeeded(id);
        ByteBuffer key = ByteBuffer.wrap(ValueUtils.serialize(id));
        Vertex cachedVertex = vertexCache.getIfPresent(key);
        if (cachedVertex != null && !((IgniteVertex) cachedVertex).isDeleted()) {
            return cachedVertex;
        }
        if (!createIfNotFound) return null;
        IgniteVertex vertex = new IgniteVertex(this, id);
        vertexCache.put(key, vertex);
        vertex.setCached(true);
        return vertex;
    }

    public void removeVertex(Vertex vertex) {
        vertex.remove();
    }

    /** VERTEX RETRIEVAL METHODS (see VertexModel) **/

    public Iterator<Vertex> allVertices() {
        return vertexModel.vertices();
    }

    public Iterator<Vertex> allVertices(Object fromId, int limit) {
        return vertexModel.vertices(fromId, limit);
    }

    public Iterator<Vertex> verticesByLabel(String label) {
        return vertexModel.vertices(label);
    }

    public Iterator<Vertex> verticesByLabel(String label, String key, Object value) {
        return vertexModel.vertices(label, key, value);
    }

    public Iterator<Vertex> verticesInRange(String label, String key, Object inclusiveFromValue, Object exclusiveToValue) {
        return vertexModel.verticesInRange(label, key, inclusiveFromValue, exclusiveToValue);
    }

    public Iterator<Vertex> verticesWithLimit(String label, String key, Object fromValue, int limit) {
        return verticesWithLimit(label, key, fromValue, limit, false);
    }

    public Iterator<Vertex> verticesWithLimit(String label, String key, Object fromValue, int limit, boolean reversed) {
        return vertexModel.verticesWithLimit(label, key, fromValue, limit, reversed);
    }

    /** EDGE RELATED **/

    public Edge addEdge(Vertex outVertex, Vertex inVertex, String label, Object... keyValues) {
        return outVertex.addEdge(label, inVertex, keyValues);
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        if (edgeIds.length == 0) {
            return allEdges();
        } else {
            Stream<Object> stream = Stream.of(edgeIds);
            List<Edge> edges = stream
                    .map(id -> {
                        if (id == null)
                            throw Exceptions.argumentCanNotBeNull("id");
                        else if (id instanceof Long)
                            return id;
                        else if (id instanceof Number)
                            return ((Number) id).longValue();
                        else if (id instanceof Edge)
                            return ((Edge) id).id();
                        else
                            return id;
                    })
                    .map(this::findOrCreateEdge)
                    .collect(Collectors.toList());
            getEdgeModel().load(edges);
            return edges.stream()
                    .filter(e -> ((IgniteEdge) e).arePropertiesFullyLoaded())
                    .iterator();
        }
    }

    public Edge edge(Object id) {
        if (id == null) {
            throw Exceptions.argumentCanNotBeNull("id");
        }
        Edge edge = findOrCreateEdge(id);
        ((IgniteEdge) edge).load();
        return edge;
    }

    public Edge findOrCreateEdge(Object id) {
        return findEdge(id, true);
    }

    protected Edge findEdge(Object id, boolean createIfNotFound) {
        if (id == null) {
            throw Exceptions.argumentCanNotBeNull("id");
        }
        id = IgniteGraphUtils.generateIdIfNeeded(id);
        ByteBuffer key = ByteBuffer.wrap(ValueUtils.serialize(id));
        Edge cachedEdge = edgeCache.getIfPresent(key);
        if (cachedEdge != null && !((IgniteEdge) cachedEdge).isDeleted()) {
            return cachedEdge;
        }
        if (!createIfNotFound) {
            return null;
        }
        IgniteEdge edge = new IgniteEdge(this, id);
        edgeCache.put(key, edge);
        edge.setCached(true);
        return edge;
    }

    public void removeEdge(Edge edge) {
        edge.remove();
    }

    public Iterator<Edge> allEdges() {
        return edgeModel.edges();
    }

    public Iterator<Edge> allEdges(Object fromId, int limit) {
        return edgeModel.edges(fromId, limit);
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public Transaction tx() {
        throw Graph.Exceptions.transactionsNotSupported();
    }

    @Override
    public Variables variables() {
        throw Graph.Exceptions.variablesNotSupported();
    }


    @Override
    public IgniteGraphConfiguration configuration() {
        return this.config;
    }

    @Override
    public Features features() {
        return features;
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, IgniteGraphConfiguration.IGNITE_GRAPH_CLASS.getSimpleName().toLowerCase());
    }

    @VisibleForTesting
    protected void refreshSchema() {
    }

    public boolean hasIndex(OperationType op, ElementType type, String label, String propertyKey) {
        return false;
    }

    public Iterator<IndexMetadata> getIndices(OperationType op, ElementType type, String label, Collection<String> propertyKeys) {
        return indices.values().stream()
                .filter(index -> isIndexActive(op, index)
                        && index.type().equals(type)
                        && index.label().equals(label)
                        && propertyKeys.contains(index.propertyKey())).iterator();
    }

    private boolean isIndexActive(OperationType op, IndexMetadata index) {
        State state = index.state();
        switch (op) {
            case READ:
                return state == State.ACTIVE;
            case WRITE:
                return state == State.CREATED || state == State.BUILDING || state == State.ACTIVE;
            case REMOVE:
                return state != State.DROPPED;
        }
        return false;
    }

    public LabelMetadata getLabel(ElementType type, String label) {
        LabelMetadata labelMetadata = labels.get(new LabelMetadata.Key(type, label));
        if (labelMetadata == null) {
            throw new IgniteGraphNotValidException(type + " LABEL " + label + " does not exist");
        }
        return labelMetadata;
    }

    public Iterator<LabelMetadata> getLabels(ElementType type) {
        return labels.values().stream().filter(label -> label.type().equals(type)).iterator();
    }

    public Iterator<LabelConnection> getLabelConnections() {
        return labelConnections.iterator();
    }

    public void validateEdge(String label, Object id, Map<String, Object> properties, Vertex inVertex, Vertex outVertex) {
        LabelMetadata labelMetadata = getLabel(ElementType.EDGE, label);
        LabelConnection labelConnection = new LabelConnection(outVertex.label(), label, inVertex.label(), null);
        if (!labelConnections.contains(labelConnection)) {
            throw new IgniteGraphNotValidException("Edge label '" + label + "' has not been connected with inVertex '" + inVertex.label()
                    + "' and outVertex '" + outVertex.label() + "'");
        }
        validateTypes(labelMetadata, id, properties);
    }

    public void validateVertex(String label, Object id, Map<String, Object> properties) {
        LabelMetadata labelMetadata = getLabel(ElementType.VERTEX, label);
        validateTypes(labelMetadata, id, properties);
    }

    private void validateTypes(LabelMetadata labelMetadata, Object id, Map<String, Object> properties) {
        ValueType idType = labelMetadata.idType();
        if (idType != ValueType.ANY && idType != ValueUtils.getValueType(id)) {
            throw new IgniteGraphNotValidException("ID '" + id + "' not of type " + idType);
        }
        properties.forEach((key, value) ->
            getPropertyType(labelMetadata, key, value, true)
        );
    }

    public ValueType validateProperty(ElementType type, String label, String propertyKey, Object value) {
        return getPropertyType(getLabel(type, label), propertyKey, value, true);
    }

    public ValueType getPropertyType(ElementType type, String label, String propertyKey) {
        return getPropertyType(getLabel(type, label), propertyKey, null, false);
    }

    private ValueType getPropertyType(LabelMetadata labelMetadata, String propertyKey, Object value, boolean doValidate) {
        Map<String, ValueType> propertyTypes = labelMetadata.propertyTypes();
        if (!Graph.Hidden.isHidden(propertyKey)) {
            ValueType propertyType = propertyTypes.get(propertyKey);
            if (doValidate) {
                if (propertyType == null) {
                    throw new IgniteGraphNotValidException("Property '" + propertyKey + "' has not been defined");
                }
                ValueType valueType = ValueUtils.getValueType(value);
                if (value != null && propertyType != ValueType.ANY
                        && (!(propertyType == ValueType.COUNTER && valueType == ValueType.LONG))
                        && (propertyType != valueType)) {
                    throw new IgniteGraphNotValidException("Property '" + propertyKey + "' not of type " + propertyType);
                }
            }
            return propertyType;
        }
        return null;
    }

    @Override
    public void close() {
        close(false);
    }

    @VisibleForTesting
    protected void close(boolean clear) {
        //executor.shutdown();
        this.edgeModel.close(clear);
        this.vertexModel.close(clear);
    }

    public void dump() {
        System.out.println("Vertices:");
        vertices().forEachRemaining(System.out::println);
        System.out.println("Edges:");
        edges().forEachRemaining(System.out::println);
    }
}
