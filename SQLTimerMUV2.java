import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import oracle.jdbc.pool.OracleDataSource;
import oracle.jdbc.OracleConnection;

public class SQLTimerMUV2 {  

	//final static String DB_URL= "jdbc:oracle:thin:@myhost:1521/myorcldbservicename";
	// For ATP and ADW - use the TNS Alias name along with the TNS_ADMIN
	final static String DB_URL="";
	final static String DB_USER = "";
	final static String DB_PASSWORD = "";
	static StringBuffer GL_Balances_Hierarchies = new StringBuffer();
	static StringBuffer GL_Balances_No_Hierarchies = new StringBuffer();
	static StringBuffer GL_Journals_Hierarchies = new StringBuffer();
	static StringBuffer GL_Journals_No_Hierarchies = new StringBuffer();
	final static int no_of_threads=100;
	final static int test_duration_in_secs=360;    
	final static int thinktime_in_secs=1;
	static AtomicInteger sql_counter = new AtomicInteger(0);
	final static List<Double> duration1  = Collections.synchronizedList(new ArrayList <Double>());
	final static List<Double> duration2  = Collections.synchronizedList(new ArrayList <Double>());
	final static List<Double> duration3  = Collections.synchronizedList(new ArrayList <Double>());
	final static List<Double> duration4  = Collections.synchronizedList(new ArrayList <Double>());
	

	public static void main(String args[]) throws SQLException,IOException {

		Properties info = new Properties();     
		info.put(OracleConnection.CONNECTION_PROPERTY_USER_NAME, DB_USER);
		info.put(OracleConnection.CONNECTION_PROPERTY_PASSWORD, DB_PASSWORD);         

		try {
			final OracleDataSource ods = new OracleDataSource();
			ods.setURL(DB_URL);    
			ods.setConnectionProperties(info);
			final long startRunTime = System.currentTimeMillis();
			final long maxDurationInMilliseconds =   test_duration_in_secs * 1000;

			GL_Balances_Hierarchies=getSQLs("GL_Balances_Hierarchies.sql");
			GL_Balances_No_Hierarchies=getSQLs("GL_Balances_No_Hierarchies.sql");
			GL_Journals_Hierarchies=getSQLs("GL_Journals_Hierarchies.sql");
			GL_Journals_No_Hierarchies=getSQLs("GL_Journals_No_Hierarchies.sql");

			for(int i = 1;i <= no_of_threads; i++)
			{
				new Thread(new Runnable()
				{
					@Override
					public void run()
					{


						// With AutoCloseable, the connection is closed automatically.
						try (OracleConnection connection = (OracleConnection) ods.getConnection()) {
							// Print some connection properties
							//System.out.println("Default Row Prefetch Value is: " + 
							//   connection.getDefaultRowPrefetch());

							long startTime,endTime;
							System.out.println(Thread.currentThread().getName() + " started");

							while (System.currentTimeMillis() < startRunTime + maxDurationInMilliseconds) {
								startTime = System.nanoTime();
								sendQuery(connection,GL_Balances_Hierarchies);
								endTime = System.nanoTime();
								synchronized (duration1) {
								duration1.add((double) ((endTime - startTime)/1000000)) ;
								}
								
								startTime = System.nanoTime();
								sendQuery(connection,GL_Balances_No_Hierarchies);
								endTime = System.nanoTime();
								synchronized (duration2) {
								duration2.add((double) ((endTime - startTime)/1000000)) ;
								}

								startTime = System.nanoTime();
								sendQuery(connection,GL_Journals_Hierarchies);
								endTime = System.nanoTime();
								synchronized (duration3) {
								duration3.add((double) ((endTime - startTime)/1000000)) ;
								}

								startTime = System.nanoTime();
								sendQuery(connection,GL_Journals_No_Hierarchies);
								endTime = System.nanoTime();
								synchronized (duration4) {
								duration4.add((double) ((endTime - startTime)/1000000)) ;
								}
								Thread.sleep(thinktime_in_secs*1000);
								
							}

							System.out.println(Thread.currentThread().getName() + " done");

						} catch (Exception e) {
							e.printStackTrace();
						}   
					}
				}).start();
			}
			
			
			// Caclculate avg RT and SQLs per sec
			int sql_counter_temp=0;
			Double sqls_per_sec=0.0,avg_temp=0.0,sum_temp=0.0,sum=0.0,avg_sql1=0.0,avg_sql2=0.0,avg_sql3=0.0,avg_sql4=0.0,avg_sql_per_sec=0.0,avg_all_sqls=0.0;
			List<Double> duration_temp  = new ArrayList <Double>();
			List<Double> sqls_per_sec_list  = new ArrayList <Double>();
			DecimalFormat df = new DecimalFormat("#0.00");
			
			while (System.currentTimeMillis() < startRunTime + maxDurationInMilliseconds) {
				Thread.sleep(5000);
				sqls_per_sec=(double) ((sql_counter.get() - sql_counter_temp)/10);
				sql_counter_temp=sql_counter.get();
				sqls_per_sec_list.add(sqls_per_sec);
				synchronized (duration1) {
					duration_temp.addAll(duration1);
				}
				synchronized (duration2) {
					duration_temp.addAll(duration2);
				}
				synchronized (duration3) {
					duration_temp.addAll(duration3);
				}
				synchronized (duration4) {
					duration_temp.addAll(duration4);
				}
				
				if(!duration_temp.isEmpty()) {
					for (Double mark : duration_temp) {
						sum_temp += mark;
					}
					avg_temp= sum_temp / duration_temp.size();
				}
				else
					avg_temp= sum_temp; 
				System.out.println("sqls_per_sec: " + sqls_per_sec + ", avg_RT: " + df.format((avg_temp/1000)));
				
				duration_temp.clear();
				
			}
			
			
			//Calculate test average

			if(!duration1.isEmpty()) {
				for (Double mark : duration1) {
					sum += mark;
				}
				avg_sql1= sum / duration1.size();
			}
			else
				avg_sql1= sum; 

			sum=0.0;
			if(!duration2.isEmpty()) {
				for (Double mark : duration2) {
					sum += mark;
				}
				avg_sql2= sum / duration2.size();
			}
			else
				avg_sql2= sum; 

			sum=0.0;
			if(!duration3.isEmpty()) {
				for (Double mark : duration3) {
					sum += mark;
				}
				avg_sql3= sum / duration3.size();
			}
			else
				avg_sql3= sum; 

			sum=0.0;
			if(!duration4.isEmpty()) {
				for (Double mark : duration4) {
					sum += mark;
				}
				avg_sql4= sum / duration4.size();
			}
			else
				avg_sql4= sum; 
			
			sum=0.0;
			if(!sqls_per_sec_list.isEmpty()) {
				for (Double mark : sqls_per_sec_list) {
					sum += mark;
				}
				avg_sql_per_sec= sum / sqls_per_sec_list.size();
			}
			else
				avg_sql_per_sec= sum; 
			
			avg_all_sqls= (avg_sql1 + avg_sql2 + avg_sql3 + avg_sql4)/4;

			File fileH = new File("./logs/output.txt");
			fileH.createNewFile();
			FileOutputStream foutH = new FileOutputStream(fileH);
			String header =  "Avg SQLs per sec, Average RT for all SQLs, Average RT in ms for SQL1, SQL2, SQL3, SQL4 \n";
			byte[] contentInBytes = header.getBytes();
			foutH.write(contentInBytes);
			String content =  avg_sql_per_sec + "," + avg_all_sqls + "," + avg_sql1 + ", " + avg_sql2 + ", " + avg_sql3 + ", " + avg_sql4;
			contentInBytes = content.getBytes();
			foutH.write(contentInBytes);
			foutH.close();
			
			
		}catch (Exception e) {
			e.printStackTrace();
		}   

	}

	public static StringBuffer getSQLs(String fileName) throws IOException{

		StringBuffer sb = new StringBuffer();
		try {
			BufferedReader in = new BufferedReader(new FileReader(fileName));
			String str;
			while ((str = in.readLine().trim()) != null) {
				sb.append(str + "\n ");
			}
			in.close();
		}catch(Exception e) {
		}
		//System.out.println(sb);
		return sb;
	}


	public static void sendQuery(Connection connection,StringBuffer SQL) throws SQLException {
		// Statement and ResultSet are AutoCloseable and closed automatically. 

		try (Statement statement = connection.createStatement()) {      
			try (
					ResultSet resultSet = statement.executeQuery(SQL.toString())) {
				//System.out.println("---------------------");
				while (resultSet.next())
				{
					//System.out.println(resultSet.getString(1) + " " + resultSet.getString(2) + " ");       
				}
				
				sql_counter.incrementAndGet();

			}
		}   
	} 
}
