package com.master.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

/**
 * @author artmaster
 */
public class HbaseFactory {
    private Configuration configuration=null;
    private Connection connection=null;
    private Admin admin=null;

    @Before
    public void init() throws IOException {
        configuration= HBaseConfiguration.create();
        connection= ConnectionFactory.createConnection(configuration);
        admin=connection.getAdmin();
    }

    @After
    public void destroy() {
        if (admin!=null){
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (connection!=null){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public Admin getAdmin() {
        return admin;
    }

    public void setAdmin(Admin admin) {
        this.admin = admin;
    }


}
