import java.sql.DriverManager

// Load Hive JDBC Driver
Class.forName( "org.apache.hive.jdbc.HiveDriver" );

// Configure JDBC connection
cnx_url = "aws-us-east-1c-dv-hdpmaster13.qualifacts.us:2181,aws-us-east-1c-dv-hdpmaster12.qualifacts.us:2181,aws-us-east-1c-dv-hdpmaster14.qualifacts.us:2181/srp_dev;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;principal=hive/_HOST@QUALIFACTS.US"

//CERT
cnx_url = "ndc-pr-hdpmaster1.qualifacts.us:2181,ndc-pr-hdpmaster2.qualifacts.us:2181,ndc-pr-hdpmaster3.qualifacts.us:2181/default;principal=hive/_HOST@QUALIFACTS.US;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
tableName = "rpt_srp_all_cert.report_cache"
account = "almamha"
batchId = "5e5999557899660001d62b1b"

connection = DriverManager.getConnection( "jdbc:hive2://$cnx_url", "", "" );

statement = connection.createStatement();

resultSet = statement.executeQuery( """
SELECT report_cache_id,`data`
FROM $tableName
where `schema` = '$account'
and batch_id = '$batchId'
and status = 'R'
and data["numberOfArrests"] > 30
""" );

import groovy.json.JsonSlurper

def jsonSlurper = new JsonSlurper()

while ( resultSet.next() ) {
    println resultSet.getString(1)
    def object = jsonSlurper.parseText(resultSet.getString( 2 ) )
    object.each { entry ->
        println "Name: $entry.key Age: $entry.value"
    }
    //println ((Map)object).getKeys()
    object.numberOfArrests = 0
    println object.numberOfArrests
}

resultSet.close();
statement.close();
connection.close();
