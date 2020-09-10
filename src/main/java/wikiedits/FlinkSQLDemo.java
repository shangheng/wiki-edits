package wikiedits;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

public class FlinkSQLDemo {
	
	public static void main(String[] args) throws Exception{

        //sql查询结果列类型
        @SuppressWarnings("rawtypes")
		TypeInformation[] fieldTypes = new TypeInformation[] {

                BasicTypeInfo.STRING_TYPE_INFO,        //第一列数据类型
                BasicTypeInfo.STRING_TYPE_INFO,     //第二类数据类型
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO    

        };

        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);
        JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
                //数据库连接信息
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://192.168.1.35:3306/ambari?useSSL=false&useUnicode=true&characterEncoding=UTF-8")
                .setUsername("ambari")
                .setPassword("ambari")
                .setQuery("SELECT os_type,host_name,public_host_name,ipv4,rack_info FROM hosts")//查询sql
                .setRowTypeInfo(rowTypeInfo)
                .finish();

//        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        //搭建flink
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
        DataSource<Row> s1 = env.createInput(jdbcInputFormat); // datasource
        System.out.println("数据行："+s1.count());
        s1.print();//此处打印
	}
}
