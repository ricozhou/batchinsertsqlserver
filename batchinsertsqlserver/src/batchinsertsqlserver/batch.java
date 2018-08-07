package batchinsertsqlserver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

public class batch {
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		batchInsert();
	}

	public static void batchInsert() throws ClassNotFoundException, SQLException {
		// 起始时间
		long start = System.currentTimeMillis();
		// 连接
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		Connection connection = DriverManager.getConnection("jdbc:sqlserver://192.168.1.xx:1433;DatabaseName=xxx",
				"xxx", "xxx");

		connection.setAutoCommit(false);
		// 执行插入
		PreparedStatement cmd = connection.prepareStatement(
				"insert into [xxx].[dbo].[BATCH] values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

		int n = 0;

		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:MM:ss.SSS");

		// 注意，此处一次性插入一亿条会溢出，导致程序崩溃，因此最好每3000万到5000万条数据一次，3000万条约需要20分钟
		// 把运行文件与数据库放在同一环境下速度更快
		// int num=1;
		for (int num = 0; num < 100000001; num++) {// 10000万条数据
			cmd.setInt(1, num);
			cmd.setInt(2, 921681220);
			cmd.setString(3, "rico" + num);

			cmd.setString(4, null);
			cmd.setString(5, null);

			cmd.setString(6, df.format(new Date()));
			cmd.setString(7, df.format(new Date()));
			cmd.setString(8, "America/Los_Angeles");

			cmd.setString(9, null);
			cmd.setString(10, null);

			cmd.setInt(11, 0);
			cmd.setInt(12, num);
			cmd.setInt(13, 0);

			cmd.setString(14, null);
			cmd.setString(15, null);

			if ((num - 1) % 500 == 0) {
				n++;
			}
			cmd.setInt(16, n);

			cmd.setString(17, df.format(new Date()));
			cmd.setString(18, df.format(new Date()));
			cmd.setString(19, "America/Los_Angeles");

			cmd.setInt(20, n);

			cmd.setString(21, df.format(new Date()));
			cmd.setString(22, df.format(new Date()));
			cmd.setString(23, "America/Los_Angeles");

			cmd.setInt(24, n);

			cmd.setInt(25, n);

			cmd.setString(26, "{" + UUID.randomUUID().toString().toUpperCase() + "}");

			cmd.setString(27, null);
			cmd.setString(28, null);
			cmd.setString(29, null);
			cmd.setString(30, null);

			cmd.setInt(31, 105);

			cmd.setString(32, null);
			cmd.setString(33, null);
			cmd.setString(34, null);

			cmd.addBatch();
			if (num % 100000 == 0) {
				cmd.executeBatch();
				System.out.println(num);
			}
		}
		cmd.executeBatch();
		connection.commit();

		cmd.close();
		connection.close();

		long end = System.currentTimeMillis();
		System.out.println("批量插入需要时间:" + (end - start));
	}
}
