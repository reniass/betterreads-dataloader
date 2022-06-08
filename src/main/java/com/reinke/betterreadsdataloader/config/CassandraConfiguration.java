package com.reinke.betterreadsdataloader.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.AbstractCassandraConfiguration;
import org.springframework.data.cassandra.config.SchemaAction;

@Configuration
public class CassandraConfiguration extends AbstractCassandraConfiguration {


    @Value(value = "${spring.data.cassandra.keyspace-name}")
    private String keySpace;
    @Value(value = "${spring.data.cassandra.contact-points}")
    private String contactPoints;
    @Value(value = "${spring.data.cassandra.port}")
    private int port;
    @Value(value = "${spring.data.cassandra.schema-action}")
    private SchemaAction schemaAction;


    @Override
    protected String getKeyspaceName() {
        return keySpace;
    }

    @Override
    public String getContactPoints() {
        return contactPoints;
    }

    @Override
    public int getPort() {
        return port;
    }


    @Override
    public String[] getEntityBasePackages() {
        return new String[] {"com.reinke.betterreadsdataloader"};
    }

    @Override
    public SchemaAction getSchemaAction() {
        return schemaAction;
    }
}
