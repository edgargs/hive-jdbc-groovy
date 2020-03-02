import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;

public class HiveJdbcClient {
  private static String driverName = "org.apache.hive.jdbc.HiveDriver";
 
  /**
   * @param args
   * @throws SQLException
   */
  public static void main(String[] args) throws Exception {
      try {
        Class.forName(driverName);
      } catch (ClassNotFoundException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        System.exit(1);
      }

    //replace
    //DEV
    String cnx_url = "aws-us-east-1c-dv-hdpmaster13.qualifacts.us:2181,aws-us-east-1c-dv-hdpmaster12.qualifacts.us:2181,aws-us-east-1c-dv-hdpmaster14.qualifacts.us:2181/srp_dev;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;principal=hive/_HOST@QUALIFACTS.US";
    String tableName = "report_cache_2019";
    String account = "pin";
    String batchId = "5d0959e2cca1850001c612bb";
    String status = "P";

    //CERT
    cnx_url = "ndc-pr-hdpmaster1.qualifacts.us:2181,ndc-pr-hdpmaster2.qualifacts.us:2181,ndc-pr-hdpmaster3.qualifacts.us:2181/default;principal=hive/_HOST@QUALIFACTS.US;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
    tableName = "rpt_srp_all_cert.report_cache";
    account = "almamha";
    batchId = "5e5999557899660001d62b1b";
    status = "R";

    //Connection con = DriverManager.getConnection("jdbc:hive2://"+cnx_url, "", "");
    //Statement stmt = con.createStatement();

    /*stmt.execute("drop table if exists " + tableName);
    stmt.execute("create table " + tableName + " (key int, value string)");
    */

    String sql;
    ResultSet res;
    
    // show tables
    /*String sql = "show tables '" + tableName + "'";
    System.out.println("Running: " + sql);
    ResultSet res = stmt.executeQuery(sql);
    if (res.next()) {
      System.out.println(res.getString(1));
    }
    */
       // describe table
    /*sql = "describe " + tableName;
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(res.getString(1) + "\t" + res.getString(2));
    }
    */
 
    // load data into table
    // NOTE: filepath has to be local to the hive server
    // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
    /*String filepath = "/tmp/a.txt";
    sql = "load data local inpath '" + filepath + "' into table " + tableName;
    System.out.println("Running: " + sql);
    stmt.execute(sql);
    */

    // select * query
    /*sql = "select * from " + tableName;
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(String.valueOf(res.getString(1)) + "\t" + res.getString(2));
    }
    */
 
    // regular hive query
    /*sql = "select count(1) from " + tableName;
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(res.getString(1));
    }
    */

/*
https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients#HiveServer2Clients-JDBCClientSampleCode
*/
    ObjectMapper objectMapper = new ObjectMapper();
    List<Map<String,String>> lstRows = new ArrayList<>();

    try (Connection con = DriverManager.getConnection("jdbc:hive2://"+cnx_url, "", ""); 
        Statement stmt = con.createStatement();) {

        //find 
        sql = "select report_cache_id,`data` from " + tableName + 
        " where `schema` = '"+account+"'" +
        " and batch_id = '"+batchId+"'" +
        " and status = '"+status+"'" +
        " and `data`[\"numberOfArrests\"] > 30" +
        ""
        ;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
          System.out.println(res.getString(1));
          String json = res.getString(2);
          Map<String, String> map = objectMapper.readValue(json, new TypeReference<Map<String,String>>(){});

          //map.put("numberOfArrests","55");
          map.remove("numberOfArrests");
          System.out.println(map);

          StringBuilder sb = new StringBuilder();
          sb.append("MAP(");
          for (Map.Entry<String,String> entry : map.entrySet())  
                sb.append("\"" + entry.getKey() + "\"" +
                                ", \"" + entry.getValue() + "\","); 
          
          sb.append("\"numberOfArrests\",\"null\"");
          sb.append(")");
          System.out.println(sb.toString());

          Map<String,String> mpRow = new HashMap<>();
          mpRow.put(res.getString(1),sb.toString());
          lstRows.add(mpRow);
        }

        System.out.println("<<Init Script>>");
        for(Map<String,String> row : lstRows){
            for (Map.Entry<String,String> entry : row.entrySet()) {
              sql = "update " + tableName +
              " set `data` = "+ entry.getValue() +
              " where `schema` = '"+account+"'" +
              " and batch_id = '"+batchId+"'" +
              " and report_cache_id = '"+entry.getKey()+"'" +
              " ;";
              System.out.println(sql);
              //stmt.executeUpdate(sql);

            }
        }


    }

  }
}
