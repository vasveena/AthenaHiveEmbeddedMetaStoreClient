package example;

import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.thrift.TException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
// import org.apache.hadoop.hive.metastore.conf.MetastoreConf;


public class AthenaEmbeddedHiveMetastoreClient {
        // implements RequestHandler<String, String> {
     // public static void main(String[] args) throws Exception
     // @Override
      public String handleRequest(String arg, final Context context)
      {
        HiveConf hiveConf = new HiveConf(AthenaEmbeddedHiveMetastoreClient.class);
        //hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://127.0.0.1:9083");
        //hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://ip-172-31-38-203.ec2.internal:9083");
        // hiveConf.set("javax.jdo.option.ConnectionURL",
        //        "jdbc:derby:memory:${test.tmp.dir}/junit_metastore_db1;create=true");
        // Configuration conf = new Configuration();
        // conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        // conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        // conf.set("fs.defaultFS", "hdfs://ip-172-31-38-203.ec2.internal:8020/user/hive/warehouse");
        // FileSystem fs = FileSystem.get(conf);

        // Create local/embedded metastore client with remote metastore (RDS Aurora)
          System.out.println("In AthenaEmbeddedHiveMetastoreClient");

          // Encryption configs
          // AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
          // s3Client.getObject(new GetObjectRequest('vasveena-test-demo','encryption/keystore.jks'),'/tmp/keystore.jks');

          //Encrypt HMS Client
          /* hiveConf.setVar(HiveConf.ConfVars.HIVE_METASTORE_USE_SSL,"true");
          hiveConf.setVar(HiveConf.ConfVars.HIVE_METASTORE_SSL_KEYSTORE_PATH,"/tmp/keystore.jks");
          hiveConf.setVar(HiveConf.ConfVars.HIVE_METASTORE_SSL_KEYSTORE_PASSWORD,"w7Ge0M0WJF");

          //Encrypt HMS server

          hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_USE_SSL,"true");
          hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH,"/tmp/keystore.jks");
          hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PASSWORD,"w7Ge0M0WJF"); */

          //Store secrets in Secret manager
          hiveConf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME,"mars");
          hiveConf.setVar(HiveConf.ConfVars.METASTOREPWD,"xxxx");
          hiveConf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER,"org.mariadb.jdbc.Driver");
          hiveConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY,
                  "jdbc:mysql://hivemetadb-instance-1.cxlolvlnaden.us-east-1.rds.amazonaws.com:3306/hivedb");
          hiveConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE,"s3a://vasveena-test-finr/rootdb");
        //hiveConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE,"hdfs://ip-172-31-38-203.ec2.internal:8020/user/hive/warehouse");

        try {
            System.out.println("Creating Embedded Hive Client");
            long start = System.currentTimeMillis();
            HiveMetaStoreClient hiveClient = new HiveMetaStoreClient(hiveConf);
            long end = System.currentTimeMillis();
            float sec = (end - start) / 1000F; System.out.println("Time taken to initiate Hive client: "+ sec +" seconds");
            String dbName = "testHiveDb";
            String tableName = "testHiveTbl";
            System.out.println("Creating database");
            createTestDatabase(hiveClient, dbName);
            System.out.println("Creating table");
            createTestTable(hiveClient, dbName, tableName);
            System.out.println("Adding Partitions");
            createPartitions(hiveClient);
            System.out.println("Listing all DBs and tables");
            listAllTablesAndDBs(hiveClient);
            hiveClient.close();
        }
        catch (Exception e)
        {
            System.out.println("error initializing hive client");
            e.printStackTrace();
        }

       return arg;
    }
    public static void listAllTablesAndDBs(HiveMetaStoreClient hiveClient)
    {
        long start = System.currentTimeMillis();
        try {

            List<String> dbsList = hiveClient.getAllDatabases();

            for (String dbName : dbsList) {
                System.out.println("DataBase Name : " + dbName);

                // getting the list of tables under each database
                List<String> tblsList = hiveClient.getAllTables(dbName);
                for (String tableName : tblsList) {
                    System.out.println("Table Name : " + tableName);

                    // getting the list of columns for each table
                    List<FieldSchema> fldsList = hiveClient.getFields(dbName, tableName);
                    for (FieldSchema flds : fldsList) {
                        System.out.println("Column" + flds);
                    }
                }
            }
            long end = System.currentTimeMillis();
            float sec = (end - start) / 1000F; System.out.println("Time taken in listing all DBs and tables: "+ sec +" seconds");
        }
        catch (MetaException e)
        {
            e.getMessage();
        }
        catch (TException e) {
            System.out.println("thrift error");
        }
        catch (Exception e) {
            System.out.println("some other error");
            e.printStackTrace();
        }
    }

    public static void createTestDatabase(HiveMetaStoreClient hiveClient, String dbName)
    {
        long start = System.currentTimeMillis();
        try {
            String dbFolder = "s3a://vasveena-test-finr/rootdb/"+dbName+".db";
            Map<String, String> emptyParam = new HashMap<String, String>();
            Database db = new Database();
            // Database db = new Database(dbName, "", dbFolder, emptyParam);
            // Database db  = new DatabaseBuilder().setName(dbName).setLocation(dbFolder).build(conf);
            db.setName(dbName);
            db.setOwnerName("mars");
            // db.setLocationUri("s3a://vasveena-test-finr/rootdb/"+dbName+".db");
            hiveClient.createDatabase(db);

            long end = System.currentTimeMillis();
            float sec = (end - start) / 1000F; System.out.println("Time taken in creating DB: "+ sec +" seconds");
        }
        catch (AlreadyExistsException e)
        {
            System.out.println("database already exists");
        }
        catch (Exception e) {
            System.out.println("some other error");
            e.printStackTrace();
        }
    }

    public static void createTestTable(HiveMetaStoreClient hiveClient, String dbName, String tableName)
    {
        long start = System.currentTimeMillis();
        try {
            Table table = new Table();
            table.setDbName("default");
            table.setTableName(tableName);
            table.setOwner("mars");
            table.setPartitionKeys(Arrays.asList(new FieldSchema("partitionFld", "int", null)));
            table.setSd(new StorageDescriptor());
            table.getSd().setLocation("s3a://vasveena-test-finr/xlsx/");
            table.getSd().setCols(Arrays.asList(new FieldSchema("id", "int", null), new FieldSchema("name", "string", null)));
            table.getSd().setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
            table.getSd().setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
            table.getSd().setSerdeInfo(new SerDeInfo());
            table.getSd().getSerdeInfo().setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");

            hiveClient.createTable(table);

            long end = System.currentTimeMillis();
            float sec = (end - start) / 1000F; System.out.println("Time taken in creating table: "+ sec +" seconds");
        }
        catch (AlreadyExistsException e)
        {
            System.out.println("table already exists");
        }
        catch (InvalidObjectException e) {
            System.out.println("invalid object error");
        }
        catch (Exception e) {
            System.out.println("some other error");
            e.printStackTrace();
        }
    }

    public static void createPartitions(HiveMetaStoreClient hiveClient, String dbName, String tableName)
    {
        long start = System.currentTimeMillis();
        try {
            List<String> vals = Arrays.asList("1998-01-01");
            String location = "1998-01-01";
            Table table = hiveClient.getTable(tableName);
            Partition part = new Partition();
            part.setDbName(table.getDbName());
            part.setTableName(table.getTableName());
            part.setValues(vals);
            part.setParameters(new HashMap<>());
            part.setSd(table.getSd().deepCopy());
            part.getSd().setSerdeInfo(table.getSd().getSerdeInfo());
            part.getSd().setLocation(table.getSd().getLocation() + location);
            hiveClient.add_partition(part);

            long end = System.currentTimeMillis();
            float sec = (end - start) / 1000F; System.out.println("Time taken in creating table: "+ sec +" seconds");
        }
        catch (MetaException e)
        {
            e.getMessage();
        }
        catch (InvalidObjectException e) {
            System.out.println("invalid object error");
        }
        catch (Exception e) {
            System.out.println("some other error");
            e.printStackTrace();
        }
    }
}