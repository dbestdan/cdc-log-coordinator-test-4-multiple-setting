import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.PGReplicationStream;

public class Server implements Config {

	public static void main(String[] args) {
		long sessionEndTime = 0L;
		PGReplicationStream stream = null;
		PGConnection replConnection = null;
		try {

			// reading System Property
			Properties prop = new Properties();
			try {
				prop.load(new FileInputStream(System.getProperty("prop")));
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			long sleepDuration = 0L;
			String tableGroup = System.getProperty("tableGroup");
			String replicationSlot = System.getProperty("replicationSlot");

			// decide table group
			List<String> tableList = null;

			if (tableGroup.equals("first")) {
				tableList = table_group_1;

			} else if (tableGroup.equals("second")) {
				tableList = table_group_2;
			} else if (tableGroup.equals("third")) {
				tableList = table_group_3;
			}

			String user = prop.getProperty("user");
			String password = prop.getProperty("password");
			String url = prop.getProperty("url");

			// create replication connection
			Properties props = new Properties();
			PGProperty.USER.set(props, user);
			PGProperty.PASSWORD.set(props, password);
			PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
			PGProperty.REPLICATION.set(props, "database");
			PGProperty.PREFER_QUERY_MODE.set(props, "simple");

			Connection conn = DriverManager.getConnection(url, props);
			replConnection = conn.unwrap(PGConnection.class);

			Connection con = getConnection();

			// create replication stream
			stream = replConnection.getReplicationAPI().replicationStream().logical().withSlotName(replicationSlot)
					.withSlotOption("include-xids", true).withSlotOption("include-timestamp", "on")
					.withSlotOption("skip-empty-xacts", true).withStatusInterval(20, TimeUnit.SECONDS).start();

			Timestamp freshness = new Timestamp(System.currentTimeMillis());
			long cummulativeStaleness = 0L;
			long averageStaleness = 0L;
			long transactionId = 0L;
			long previousTransactionId = 0L;
			long count = 0L;
			long sessionStartTime = System.currentTimeMillis();
			String stalenessFilename = "staleness_" + dateFormat.format(new Date());
			String dataFilename = "data_" + dateFormat.format(new Date());

			Writer stalenessOutPut = new BufferedWriter(
					new OutputStreamWriter(new FileOutputStream(stalenessFilename, true), "UTF-8"));
			Writer dataWriter = new BufferedWriter(
					new OutputStreamWriter(new FileOutputStream(dataFilename, true), "UTF-8"));

			int noOfSet = Integer.parseInt(prop.getProperty("no_of_set"));
			int numberOfExperiment = Integer.parseInt(prop.getProperty("no_of_experiment"));
			for (int l = 0; l < noOfSet; l++) {

				for (int i = 1; i <= numberOfExperiment; i++) {
					String experiment = prop.getProperty("experiment_" + i);
					String experimentParameter[] = experiment.split("_");
					sleepDuration = Long.parseLong(experimentParameter[1]);
					long runDuration = Long.parseLong(experimentParameter[3]) * 60000L;
					sessionEndTime = System.currentTimeMillis() + runDuration;

					long tempTime = System.currentTimeMillis();

					while (sessionEndTime > System.currentTimeMillis()) {
						// non blocking receive message

						ByteBuffer msg = stream.readPending();

						if (msg == null) {
							TimeUnit.MILLISECONDS.sleep(10L);
							continue;
						}

						int offset = msg.arrayOffset();
						byte[] source = msg.array();
						int length = source.length - offset;
						// convert byte buffer into string
						String line = new String(source, offset, length);

						if (line.contains("BEGIN") || line.contains("COMMIT")) {
							if (line.contains("BEGIN")) {
								try {
									transactionId = Long.parseLong(line.split(" ")[1]);
									previousTransactionId = transactionId;
								} catch (NumberFormatException e) {
									System.err.println(new Date() + "Problem in getting the transaction Id ");
									System.err.println("Sleep Duration: "+ sleepDuration + "Replication Slot: "+replicationSlot);
									System.err.println(line);
									transactionId = previousTransactionId;
								}
								ResultSet rs = con.createStatement()
										.executeQuery("select pg_xact_commit_timestamp('" + transactionId + "'::xid)");
								// System.out.println("hello!");
								rs.next();
								Timestamp t = rs.getTimestamp(1);
								if (freshness.before(t)) {
									freshness = t;
									// System.out.println("Freshness: "+
									// freshness);
								}
							} else {
								long staleness = System.currentTimeMillis() - freshness.getTime();
								count++;
								cummulativeStaleness += staleness;
								averageStaleness = cummulativeStaleness / count;

								stalenessOutPut.append((System.currentTimeMillis() - sessionStartTime) + "," + staleness
										+ "," + averageStaleness + "\n");
								// System.out.println("Staleness:"+staleness+"
								// AvgStaleness:"+averageStaleness+ "\n");
								stalenessOutPut.flush();

							}
						} else {

							for (String tableName : tableList) {
								if (line.contains(tableName)) {
									System.out.println(replicationSlot + " " + line);
									dataWriter.append(line);
									break;
								}
							}

						}
						dataWriter.flush();

						// feedback
						stream.setAppliedLSN(stream.getLastReceiveLSN());
						stream.setFlushedLSN(stream.getLastReceiveLSN());

						// sleep for appropriate time
						if (sleepDuration > 0) {
							if ((System.currentTimeMillis() - tempTime) > 1000) {
								System.out.println("sleep time :" + sleepDuration);
								Thread.sleep(sleepDuration);
								tempTime = System.currentTimeMillis();
							}
						}

					}
				}
			}

		} catch (SQLException | InterruptedException | IOException | NumberFormatException e) {
			// TODO Auto-generated catch block
			long errorRecordedTime = (sessionEndTime - System.currentTimeMillis()) / 60000;
			System.out.println("Error recoreded time: " + errorRecordedTime);
			e.printStackTrace();
		} finally {
			try {
				stream.close();
				Connection conn = (Connection) replConnection;
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}

	public static Connection getConnection() {
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream(System.getProperty("prop")));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		String user = prop.getProperty("user");
		String password = prop.getProperty("password");
		String url = prop.getProperty("url");

		Connection conn = null;
		Properties connectionProps = new Properties();
		connectionProps.put("user", user);
		connectionProps.put("password", password);
		try {
			Class.forName("org.postgresql.Driver");
			conn = DriverManager.getConnection(url, connectionProps);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}

}
