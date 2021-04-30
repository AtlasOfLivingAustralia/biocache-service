/**************************************************************************
 *  Copyright (C) 2013 Atlas of Living Australia
 *  All Rights Reserved.
 *
 *  The contents of this file are subject to the Mozilla Public
 *  License Version 1.1 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://www.mozilla.org/MPL/
 *
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 ***************************************************************************/
package au.org.ala.biocache.dao;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.extras.codecs.MappingCodec;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.MapMaker;
import com.google.common.reflect.TypeToken;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JsonConfig;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.text.ParseException;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * Cassandra 3 based implementation of a persistence manager.
 * This should maintain most of the cassandra 3 logic.
 * <p>
 * Merged from biocache-store
 */
@Component("storeDao")
public class CassandraStoreDAOImpl implements StoreDAO {

    private static final Logger logger = Logger.getLogger(CassandraStoreDAOImpl.class);

    private Cluster cluster;
    private Session session;

    @Value("${cassandra.hosts:localhost}")
    String host;

    @Value("${cassandra.port:9042}")
    Integer port;

    @Value("${cassandra.keyspace:biocache}")
    String keyspace;

    /**
     * Cassandra schema sql.
     */
    @Value("${cassandra.keyspace.default.cql:CREATE KEYSPACE biocache WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;}")
    String createKeyspaceCql;

    Map<String, PreparedStatement> preparedStatementCache = new MapMaker().weakValues().makeMap();

    @PostConstruct
    public void init() throws Exception {
        logger.debug("Initialising CassandraStoreDAOImpl");

        Cluster.Builder builder =
                Cluster.builder()
                        .withoutJMXReporting() // Workaround for conflict with SOLR 8
                        .withReconnectionPolicy(new ExponentialReconnectionPolicy(10000, 60000))
                        .withCodecRegistry(
                                CodecRegistry.DEFAULT_INSTANCE.register(
                                        new TimestampAsStringCodec(TypeCodec.timestamp(), String.class)));

        String[] host_port = host.split(":");
        if (host_port.length > 1) {
            builder.withPort(Integer.parseInt(host_port[1])).addContactPoint(host_port[0]);
        } else {
            builder.withPort(port).addContactPoint(host_port[0]);
        }

        cluster = builder.build();

        if (cluster.getMetadata().getKeyspace(keyspace) == null) {
            logger.warn("Keyspace '" + keyspace + "' not found. Creating the keyspace with: " + createKeyspaceCql);

            // Create keyspace
            cluster.connect().execute(createKeyspaceCql);
        }
        session = cluster.connect(keyspace);
    }

    class TimestampAsStringCodec extends MappingCodec<String, Date> {
        public TimestampAsStringCodec(TypeCodec<Date> innerCodec, Class<String> javaType) {
            super(innerCodec, javaType);
        }

        public TimestampAsStringCodec(TypeCodec<Date> innerCodec, TypeToken<String> javaType) {
            super(innerCodec, javaType);
        }

        @Override
        protected String deserialize(Date date) {
            return org.apache.commons.lang.time.DateFormatUtils.format(
                    new java.util.Date(), "yyyy-MM-dd HH:mm:ss");
        }

        @Override
        protected Date serialize(String string) {
            try {
                return DateUtils.parseDate(string, "yyyy-MM-dd HH:mm:ss");
            } catch (ParseException e) {
                logger.error("Failed to parse date string: " + string);
            }

            return null;
        }
    }

    @Override
    public <T> Optional<T> get(Class<T> dataClass, String key) throws IOException {
        String className = dataClass.getSimpleName();

        T result = null;
        PreparedStatement stmt =
                getPreparedStmt("SELECT * FROM " + className + " where key = ? ", className);
        BoundStatement boundStatement = stmt.bind(key);
        ResultSet rs = session.execute(boundStatement);
        Iterator<Row> rows = rs.iterator();

        if (rows.hasNext()) {
            Row row = rows.next();

            String jsonString = row.get(1, String.class);

            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            mapper.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true);
            result = mapper.readValue(jsonString, dataClass);
        }

        return Optional.ofNullable(result);
    }

    @Override
    public <T> void put(String key, T data) throws IOException {
        JsonConfig jsonConfig = new JsonConfig();
        // don't save null fields
        jsonConfig.setJsonPropertyFilter((Object source, String name, Object val) -> val == null);

        String value;
        if (data instanceof Collection) {
            value = JSONArray.fromObject(data, jsonConfig).toString();
        } else {
            value = JSONObject.fromObject(data, jsonConfig).toString();
        }

        String className = data.getClass().getSimpleName();
        try {
            BoundStatement boundStatement = createPutStatement(key, className, value);
            executeWithRetries(session, boundStatement);
        } catch (Exception e) {
            logger.error(
                    "Problem persisting the following to "
                            + className
                            + " key="
                            + key
                            + " value="
                            + value
                            + " "
                            + e.getMessage(),
                    e);
            throw e;
        }
    }

    private ResultSet executeWithRetries(Session session, Statement statement) {
        int MAX_QUERY_RETRIES = 10;
        int retryCount = 0;
        boolean needToRetry = true;
        ResultSet resultSet = null;
        while (retryCount < MAX_QUERY_RETRIES && needToRetry) {
            try {
                resultSet = session.execute(statement);
                needToRetry = false;
                retryCount = 0; // reset
            } catch (Exception e) {
                logger.error("Exception thrown during paging. Retry count $retryCount - " + e.getMessage());
                retryCount++;
                needToRetry = true;

                try {

                    if (retryCount > 5) {
                        logger.error("Backing off for 10 minutes. Retry count " + retryCount + ", " + e.getMessage());
                        Thread.sleep(600000);
                    } else {
                        logger.error("Backing off for 5 minutes. Retry count " + retryCount + ", " + e.getMessage());
                        Thread.sleep(300000);
                    }
                } catch (InterruptedException interruptedException) {
                    logger.error("Failed to sleep. " + interruptedException.getMessage());
                }
            }
        }
        return resultSet;
    }

    @Override
    public <T> Boolean delete(Class<T> dataClass, String key) throws IOException {
        String className = dataClass.getSimpleName();

        PreparedStatement deleteStmt =
                getPreparedStmt("DELETE FROM " + className + " WHERE key = ?", className);
        BoundStatement boundStatement = deleteStmt.bind(key);
        ResultSet resultSet = session.execute(boundStatement);

        return resultSet != null;
    }

    @PreDestroy
    public void destroy() {
        session.close();
    }

    private PreparedStatement getPreparedStmt(String cql, String table) {

        boolean tryQuery = true;
        PreparedStatement result = null;
        while (tryQuery) {
            tryQuery = false;

            try {
                PreparedStatement preparedStatement = preparedStatementCache.get(cql);
                if (preparedStatement == null){
                    preparedStatement = session.prepare(cql);
                    preparedStatementCache.put(cql, preparedStatement);
                }
                return preparedStatement;
            } catch (com.datastax.driver.core.exceptions.InvalidQueryException e) {
                logger.info("Creating table " + table);
                try {
                    session.execute("CREATE TABLE " + table + "( key text PRIMARY KEY, value text );");
                    tryQuery = true;
                } catch (Exception ex) {
                    logger.error("Failed to create table " + table);
                }
            }
        }

        return result;
    }

    private BoundStatement createPutStatement(String key, String table, String value) {
        String cql = "INSERT INTO " + table + " (key,value) VALUES (?,?);";

        PreparedStatement statement = getPreparedStmt(cql, table);

        BoundStatement boundStatement = statement.bind(key, value);

        statement.setIdempotent(true); // this will allow retries

        return boundStatement;
    }
}
