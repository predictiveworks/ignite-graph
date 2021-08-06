package de.kp.works.ignitegraph;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.Serializable;
import java.util.Iterator;

public class IgniteGraphConfiguration extends AbstractConfiguration implements Serializable {

    private static final long serialVersionUID = -7150699702127992270L;

    private PropertiesConfiguration conf;

    public static final Class<? extends Graph> IGNITE_GRAPH_CLASS = IgniteGraph.class;

    public static final String IGNITE_GRAPH_CLASSNAME = IGNITE_GRAPH_CLASS.getCanonicalName();

    public static class Keys {
        public static final String GRAPH_NAMESPACE             = "gremlin.ignite.namespace";
        public static final String GRAPH_CLASS                 = "gremlin.graph";
        public static final String GLOBAL_CACHE_MAX_SIZE       = "gremlin.ignite.globalCacheMaxSize";
        public static final String GLOBAL_CACHE_TTL_SECS       = "gremlin.ignite.globalCacheTtlSecs";
        public static final String RELATIONSHIP_CACHE_MAX_SIZE = "gremlin.ignite.relationshipCacheMaxSize";
        public static final String RELATIONSHIP_CACHE_TTL_SECS = "gremlin.ignite.relationshipCacheTtlSecs";

    }

    /**
     * A minimal configuration for the IgniteGraph
     */
    public IgniteGraphConfiguration() {
        conf = new PropertiesConfiguration();
        conf.setProperty(Keys.GRAPH_CLASS, IGNITE_GRAPH_CLASSNAME);
    }

    public IgniteGraphConfiguration(Configuration config) {
        conf = new PropertiesConfiguration();
        conf.setProperty(Keys.GRAPH_CLASS, IGNITE_GRAPH_CLASSNAME);
        if (config != null) {
            config.getKeys().forEachRemaining(key ->
                    conf.setProperty(key.replace("..", "."), config.getProperty(key)));
        }
    }

    public String getGraphNamespace() {
        return conf.getString(Keys.GRAPH_NAMESPACE, "default");
    }

    public IgniteGraphConfiguration setGraphNamespace(String name) {
        if (!isValidGraphName(name)) {
            throw new IllegalArgumentException("Invalid graph namespace."
                    + " Only alphanumerics and underscores are allowed");
        }

        conf.setProperty(Keys.GRAPH_NAMESPACE, name);
        return this;
    }

    private static boolean isValidGraphName(String name) {
        return name.matches("^[A-Za-z0-9_]+$");
    }

    public long getElementCacheMaxSize() {
        return conf.getLong(Keys.GLOBAL_CACHE_MAX_SIZE, 1000000);
    }

    public long getElementCacheTtlSecs() {
        return conf.getLong(Keys.GLOBAL_CACHE_TTL_SECS, 60);
    }

    public long getRelationshipCacheMaxSize() {
        return conf.getLong(Keys.RELATIONSHIP_CACHE_MAX_SIZE, 1000);
    }

    public long getRelationshipCacheTtlSecs() {
        return conf.getLong(Keys.RELATIONSHIP_CACHE_TTL_SECS, 60);
    }

    @Override
    protected boolean isEmptyInternal() {
        return conf.isEmpty();
    }

    @Override
    protected boolean containsKeyInternal(String key) {
        return conf.containsKey(key);
    }

    @Override
    protected Object getPropertyInternal(String key) {
        return conf.getProperty(key);
    }

    public IgniteGraphConfiguration set(String key, Object value) {
        conf.setProperty(key, value);
        return this;
    }

    @Override
    protected Iterator<String> getKeysInternal() {
        return conf.getKeys();
    }

    @Override
    protected void addPropertyDirect(String key, Object value) {
        conf.setProperty(key, value);
    }

    @Override
    protected void clearPropertyDirect(String key) {
        conf.clearProperty(key);
    }

    public IgniteConfiguration toIgniteConfiguration() {
        return null;
    }

}
