package com.agentlog.demo;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.*;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.batch.BatchAutoConfiguration;

import java.util.Date;
import java.util.Map;
import java.util.HashMap;

import org.apache.spark.eventhubs.EventHubsUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import scala.collection.Seq;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.lag;
import static org.apache.spark.sql.functions.not;

import java.util.Iterator;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Instant;

@SpringBootApplication
@EnableAutoConfiguration(exclude = {org.springframework.boot.autoconfigure.gson.GsonAutoConfiguration.class})
public class DemoApplication implements  CommandLineRunner{

	public static void main(String[] args) {
		try {
			SpringApplication.run(DemoApplication.class, args);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	private static SparkSession spark;
	private Dataset<Row> intermediateDf;
	private static final String EH_NAMESPACE_CONNECTION_STRING = "Endpoint=sb://eeventhubspace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=td2eQVC2RKwzc/tmBJF0CwGBCZG1IRsgLnPH4dkM+2M=";
	private static final String eventHubName = "testeventhub";

	private void WriteToSqlDataBase(Dataset<Row> batchDF,Long batchId){

	}
	@Override
	public void run(String... args) throws Exception {
		System.out.println("Starting my spark app...");

		//spark job parameters are sent as argumments. Parsing here
		ReportConf reportParams = new ReportConf();
		String inputParam = args[0];

		System.out.println("Command line arguments are args[0] = " + inputParam);

		inputParam = inputParam.replace("'","\"");

		System.out.println("Command line arguments after replacing single quotes = " + inputParam);

		JSONParser parser = new JSONParser();
		JSONObject json = (JSONObject) parser.parse(inputParam);

		//System.out.println("Fields from input json " + "Tenant : " + json.get("Tenant") + "Start Event" +  json.get("StartEvent"));

		reportParams.Tenant = json.getOrDefault("Tenant","Default").toString();
		reportParams.Environment = json.getOrDefault("Environment","Default").toString();
		reportParams.StartEvent = json.getOrDefault("StartEvent","Default").toString();
		reportParams.EndEvent = json.getOrDefault("EndEvent","Default").toString();
		reportParams.MetricType = json.getOrDefault("MetricType","Rec").toString();
		reportParams.ObjectType = json.getOrDefault("ObjectType","Agent").toString();
		reportParams.ReportField = json.getOrDefault("ReportField","Duration").toString();
		reportParams.Filter = json.getOrDefault("Filter","1 == 1").toString();


		spark = SparkSession.builder()
				.appName("Agent Reports")
				.master("local[2]")
				.getOrCreate();


		System.out.println("Got the session");
		String namespaceName = "EEventHubSpace";
		String eventHubName = "TestEventHub";
		String sasKeyName = "RootManageSharedAccessKey";
		String sasKey = "ssiB5rZJ4mCmr8g7wWZ+H9uU04XSjHCxkRZx6DuYyFM=";

		ConnectionStringBuilder eventHubConnectionString = new ConnectionStringBuilder()
				.setNamespaceName(namespaceName)
				.setEventHubName(eventHubName)
				.setSasKeyName(sasKeyName)
				.setSasKey(sasKey);

		StructType jsonSchema = new StructType(new StructField[]{
				new StructField("attributeEventName",DataTypes.StringType,false,Metadata.empty()),
				new StructField("attributeEventTypeId", DataTypes.IntegerType, false,Metadata.empty()),
				new StructField("attributeWorkMode", DataTypes.IntegerType, false,Metadata.empty()),
				new StructField("attributeReasonCode", DataTypes.StringType, false,Metadata.empty()),
				new StructField("attributeUser", DataTypes.StringType, false,Metadata.empty()),
				new StructField("attributeSid", DataTypes.StringType, false,Metadata.empty()),
				new StructField("attributeActivitySid", DataTypes.StringType, false,Metadata.empty()),
				new StructField("attributeThisDN", DataTypes.StringType, false,Metadata.empty()),
				new StructField("attributeTimestamp", DataTypes.TimestampType, false,Metadata.empty())
		});



		System.out.println("About to connect to event stream");
		Dataset<Row>  intermediateDf = spark
				.readStream()
				.format("eventhubs")
				.option("eventhubs.connectionString",EventHubsUtils.encrypt(eventHubConnectionString.toString()))
				.schema(jsonSchema)
				.load();

		System.out.println("=========  dataframe size : ===========");

		Dataset<Row> msgs = intermediateDf
				//.withColumn("Time", intermediateDf.col("enqueuedTime").cast(DataTypes.TimestampType))
				//.withColumn("Timestamp", intermediateDf.col("enqueuedTime").cast(DataTypes.LongType))
				.withColumn("Body", from_json(intermediateDf.col("body").cast(DataTypes.StringType),jsonSchema))
				.select("Body.*");
				//.select("Time", "Timestamp","Body.*");
				//.withColumn("attributeThisDN",col("Body.attributeThisDN"))
				//.select("Body.attributeThisDN");


		msgs.printSchema();

//				msgs.writeStream()
//				.format("console")
//				.outputMode("append")
//				.start()
//				.awaitTermination();
		System.out.println("======================== Report ==================");
//		Dataset<Row> report = msgs.select(col(reportParams.ReportField),col("attributeWorkMode"), col("attributeReasonCode"))
//						//.where("attributeWorkMode == 0 and attributeReasonCode == 'Idle'")
//				        .where(reportParams.Filter)
//						.groupBy(reportParams.ReportField).count()
//						.select(col(reportParams.ReportField),col("count"));

		Dataset<Row> report = msgs.groupBy(
				functions.window(msgs.col("attributeTimestamp"), "10 minutes", "10 minutes").as("window"),
				msgs.col("attributeThisDN"))
				.count();
//		report.writeStream()
//				.format("console")
//				.outputMode("complete")
//				.option("truncate", "false")
//				.start()
//		        .awaitTermination();


		//		Dataset<Row> interval = temp.withColumn("prevTime", lag(col("t"), 1).over(functions.window(col("t"), "10 minutes", "5 minutes")))
//				                    .withColumn("duration", (unix_timestamp(col("t")).$minus(unix_timestamp(col("prevTime")))))
//				                    .select(col("attributeThisDN"), col("duration"));
//		interval.writeStream()
//				.format("console")
//				.outputMode("append")
//				.start()
//		        .awaitTermination();

		//Writing the results to cosmos db
//		Map configMap = Map(
//				"Endpoint" -> {"https://eswari.documents.azure.com:443/"},
//		"Masterkey" -> {"glmIeYKGxkvDIoM46ajBTYhuOikPUkCmOEKATRbQv3fbgnm96ANdcrz8j7YYICYI5836Du2pPK4W2QJYC9ZWxw=="},
//		"Database" -> {"eswari"},
//		"Collection" -> {"calldata"});
//		Config DBconfig = Config(configMap);
//
//        // Start the stream writer
//        msgs.writeStream.format(classOf[CosmosDBSinkProvider].getName).outputMode("append").options(configMap).option("checkpointLocation", "/log")
		System.out.println("End of run method");

		String jdbcUrl = "jdbc:mysql://localhost:3306/reports?useSSL=false&serverTimezone=UTC&useLegacyDatetimeCode=false&allowPublicKeyRetrieval=true";
		String user = "root";
		String password = "password";

		//writing into a local sql
//		report.select(col("window"),col("attributeThisDN"),col("count"))
//				.writeStream()
//				.format("jdbc")
//				.outputMode("complete")
//				.option("url","")
//				.option("dbtable","")
//				.option("user",user)
//				.option("password",password)
//				.start();




//		report.writeStream().foreach(
//				new ForeachWriter<Row>() {
//
//					@Override public boolean open(long partitionId, long version) {
//						// Open connection
//						System.out.println("Connection open call");
//						return true;
//					}
//
//					@Override public void process(Row record) {
//						// Write string to connection
//						try{
//							Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
//							PreparedStatement statement = conn.prepareStatement("INSERT INTO agentEvents (timewindow,agentId,count) VALUES (?,?,?)");
//
//							//statement.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
//							statement.setString(2, "test");
//							statement.setLong(3, 1);
//							System.out.println("Storing data to db..going to call executeUpdate");
//							statement.executeUpdate();
//						}catch(Exception e){
//							e.printStackTrace();
//						}
//					}
//
//					@Override public void close(Throwable errorOrNull) {
//						// Close the connection
//						System.out.println("----------------------- Close called ----------------");
//							errorOrNull.printStackTrace();
//					}
//				}
//				)
//				.outputMode("complete")
//				.start().awaitTermination();

		//lets write into a cassandara db
		//report.select(col("window"),col("attributeThisDN"),col("count"))
		report
				.select(report.col("window").cast(StringType),report.col("attributeThisDN"),report.col("count"))
				.writeStream()
				.foreachBatch(
						new VoidFunction2<Dataset<Row>, Long>() {
					public void call(Dataset<Row> dataset, Long batchId) {
						// Transform and write batchDF
						dataset.withColumn("Window",col("window"))
								.write().format("jdbc")

								.option("url",jdbcUrl)
				                .option("user",user)
								.option("password",password)
								.option("driver", "com.mysql.jdbc.Driver")
								.option("dbtable","agentEvents")
								.mode(SaveMode.Append)
								.save();
					}
				})
				.outputMode("update")
				.start()
				.awaitTermination();
		System.out.println("Wrote to DB");

	}
}
