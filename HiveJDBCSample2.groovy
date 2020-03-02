
/*
How to load jdbc jar:
https://stackoverflow.com/questions/32180388/import-java-library-groovy
*/

import groovy.sql.Sql

//CERT
cnx_url = "ndc-pr-hdpmaster1.qualifacts.us:2181,ndc-pr-hdpmaster2.qualifacts.us:2181,ndc-pr-hdpmaster3.qualifacts.us:2181/default;principal=hive/_HOST@QUALIFACTS.US;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
tableName = "rpt_srp_all_cert.report_cache"
account = "almamha"
batchId = "5e5999557899660001d62b1b"

if(this.args.length >= 1) account = this.args[0]
if(this.args.length >= 2) batchId = this.args[1]

println "Account: $account"
println "Batch Id: $batchId"

def url = "jdbc:hive2://"+cnx_url
def user = ''
def password = ''
def driver = 'org.apache.hive.jdbc.HiveDriver'
def sql = Sql.newInstance(url, user, password, driver)

str_query = String.format("""
SELECT report_cache_id,`data`
FROM %s
where `schema` = '%s'
and batch_id = '%s'
and status = 'R'
--and data["numberOfArrests"] > 30
""",tableName,account,batchId)

List lst_rows = sql.rows(str_query)
println 'Total rows: '+lst_rows.size

if(lst_rows.size == 0){
    sql.close()
    println "Don't create file"
    return
}

def jsonSlurper = new groovy.json.JsonSlurper()
file_name = "query-update-${new Date().getTime()}.sql"
File file = new File(file_name)

lst_rows.each {
    
    def object = jsonSlurper.parseText(it[1] )
    //println object
    object.numberOfArrests = "";

    StringBuilder sb = new StringBuilder();
    sb.append("MAP(");
    object.eachWithIndex { key, value, i ->
       sb.append("${i>0?',':''}\"" + key + "\"" +", \"" + value + "\"");    
    }
    sb.append(")");
    //System.out.println(sb.toString());

    sql_update = "update " + tableName +
              " set `data` = "+ sb.toString() +
              " where `schema` = '"+account+"'" +
              " and batch_id = '"+batchId+"'" +
              " and report_cache_id = '"+it.report_cache_id+"'" +
              " ;";
    //println sql_update
    
    file << sql_update + "\n"
}

sql.close()
println "File generated: $file_name"

