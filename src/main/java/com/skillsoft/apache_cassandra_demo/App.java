package com.skillsoft.apache_cassandra_demo;

import java.util.UUID;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

/**
 * Hello world!
 *
 */
public class App{
	private Cluster cluster;
	private Session session;
	
	public void connect(final String node){
		cluster = Cluster.builder().addContactPoint(node).build();
		session = cluster.connect();
		Metadata metadata = cluster.getMetadata();
		System.out.println("Connected to cluster name: " + metadata.getClusterName());
		for(Host host : metadata.getAllHosts()){
			System.out.println(String.format("Data Center: " + 
						host.getDatacenter()
						+ "Host: " +
						host.getAddress()
						+ "Rack: " +
						host.getRack()
						+ "Version: " +
						host.getCassandraVersion()
					));
		}
	}
	
	public void createSchema(){
		session.execute("CREATE KEYSPACE IF NOT EXISTS vacation WITH replication" +
					" = {'class':'SimpleStrategy','replication_factor':1};");
		session.execute("CREATE TABLE IF NOT EXISTS vacation.destinations (" +
						"id uuid PRIMARY KEY, " +
						"name text, " +
						"rating int " +
						");");
	}
	
	public void loadData(){
		session.execute("INSERT INTO vacation.destinations(id,name,rating)" +
					"VALUES(" +
					"a9299f19-78e2-4236-98eb-755e971fe440, " +
					"'Andros,Greece'," +
					"4" +
					");");
	}
	
	public void updateData(){
		Update.Where update = QueryBuilder.update("vacation","destinations")
							.with(QueryBuilder.set("rating", 10))
							.where(QueryBuilder.eq("id", UUID.fromString("a9299f19-78e2-4236-98eb-755e971fe440")));
		session.execute(update);
	}
	
	public void delete(){
		session.execute("DELETE from vacation.destinations where id=a9299f19-78e2-4236-98eb-755e971fe440");
	}
	
	public void query(){
		ResultSet resultSet = session.execute("SELECT * FROM vacation.destinations");
		System.out.println(String.format("%-30s\t%-5s", "name","rating"));
		for(Row row:resultSet){
			System.out.println(String.format("%-30s\t%d", row.getString("name"),row.getInt("rating")));
		}
	}
	public void close(){
		cluster.close();
		session.close();
	}
	
    public static void main(String[] args){
        System.out.println( "Hello Cassandra!" );
        App apacheCassandraDemo = new App();
        apacheCassandraDemo.connect("127.0.0.1");
        apacheCassandraDemo.createSchema();
        apacheCassandraDemo.loadData();
        apacheCassandraDemo.query();
        apacheCassandraDemo.updateData();
        apacheCassandraDemo.query();
        apacheCassandraDemo.delete();
        apacheCassandraDemo.query();
        apacheCassandraDemo.close();
    }
}
