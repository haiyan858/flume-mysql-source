package com.atguigu;

import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @Author cuihaiyan
 * @Create_Time 2019-09-30 00:26
 */
public class SQLSourceHelper {
    private static final Logger LOG = LoggerFactory.getLogger(SQLSourceHelper.class);

    private int runQueryDealy, //两次查询的时间间隔
            startFrom,//开始ID
            currentIndex,//当前ID
            recordSize,//每次查询返回结果的条数
            maxRow;//每次查询的最大条数

    private String table,//要操作的表
            columnsToSelect,//用户传入的查询的列
            customQuery,//用户传入的查询语句
            query,//构建的查询语句
            defaultCharacterResultSet;//编码集


    //上下文， 用来获取配置文件
    private Context context;

    //为定义的变量赋值（默认值），可在flume任务的配置文件中修改
    private static final int DEFAULT_QUERY_DELAY = 10000;
    private static final int DEFAULT_START_VALUE = 0;
    private static final int DEFAULT_MAX_ROWS = 2000;
    private static final String DEFAULT_COLUMNS_SELECT = "*";
    private static final String DEFAULT_CHARSET_RESULTSET = "UTF-8";

    private static Connection conn = null;
    private static PreparedStatement ps = null;
    private static String connectionURL, connectionUserName, connectionPassword;

    //加载静态资源
    static {
        Properties p = new Properties();
        try {
            p.load(SQLSourceHelper.class.getClassLoader().getResourceAsStream("jdbc.propertis"));
            connectionURL = p.getProperty("dbUrl");
            connectionUserName = p.getProperty("dbUser");
            connectionPassword = p.getProperty("dbPassword");
            Class.forName(p.getProperty("dbDriver"));
        } catch (IOException | ClassNotFoundException e) {
            //e.printStackTrace();
            LOG.error(e.toString());
        }
    }

    //获取jdbc 连接
    private static Connection InitConnectionn(String url, String user, String pw) {
        try {
            Connection conn = DriverManager.getConnection(url, user, pw);
            if (conn == null) {
                throw new SQLException();
            }
            return conn;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    //构造方法


    public SQLSourceHelper(Context context) {
        //初始化上下文
        this.context = context;

        // 有默认值参数：获取flume 任务配置文件中的参数，读不到的采用默认值
        this.columnsToSelect = context.getString("columns.to.select", DEFAULT_COLUMNS_SELECT);
        this.runQueryDealy = context.getInteger("run.query.delay", DEFAULT_QUERY_DELAY);
        this.startFrom = context.getInteger("start.from", DEFAULT_START_VALUE);
        this.defaultCharacterResultSet = context.getString("default.charset.resultset", DEFAULT_CHARSET_RESULTSET);

        // 无默认参数，获取flume 任务配置文件中的参数
        this.table = context.getString("table");
        this.customQuery = context.getString("custom.query");
        connectionURL = context.getString("connection.url");
        connectionUserName = context.getString("connection.user");
        connectionPassword = context.getString("connection.password");
        conn = InitConnectionn(connectionURL, connectionUserName, connectionPassword);

        //校验相应的配置信息，如果没有默认值的参数也没赋值，抛出异常
        checkMandatoryProperties();

        //获取当前的ID
        currentIndex = getStatusDBIndex(startFrom);

        //构建查询语句
        query = buildQuery();
    }


    //校验相应的配置信息（表，查询语句以及数据库连接的参数）
    private void checkMandatoryProperties() {
        if (table == null) {
            throw new ConfigurationException("property table not set");
        }

        if (connectionURL == null) {
            throw new ConfigurationException("connection.url property not set");
        }
        if (connectionUserName == null) {
            throw new ConfigurationException("connection.user property not set");
        }
        if (connectionPassword == null) {
            throw new ConfigurationException("connection.password property not set");
        }
    }

    // 构建SQL 语句
    private String buildQuery() {
        String sql = "";
        //获取当前ID
        currentIndex = getStatusDBIndex(startFrom);
        LOG.info(currentIndex + "");

        if (customQuery == null) {
            sql = "SELECT " + columnsToSelect + " FROM " + table;
        } else {
            sql = customQuery;
        }

        StringBuilder execSql = new StringBuilder(sql);
        //以ID 作为offset
        if (!sql.contains("where")) {
            execSql.append(" where ");
            execSql.append("id").append(">").append(currentIndex);
            execSql.toString();
            return execSql.toString();
        } else {
            int length = execSql.toString().length();
            return execSql.toString().substring(0, length - String.valueOf(currentIndex).length());
        }

    }

    /**
     * 执行查询
     *
     * @return
     */
    public List<List<Object>> executeQuery() {
        try {
            //每次执行查询时，都要重新生成sql，因为ID不同
            customQuery = buildQuery();
            List<List<Object>> results = new ArrayList<>();

            if (ps == null) {
                ps = conn.prepareStatement(customQuery);
            }

            ResultSet result = ps.executeQuery(customQuery);
            while (result.next()) {
                //存放一条数据的集合（多个列）
                ArrayList<Object> row = new ArrayList<>();
                for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
                    row.add(result.getObject(i));
                }
                results.add(row);
            }

            LOG.info("execSql: " + customQuery + "\nresultSize:" + results.size());
            return results;
        } catch (SQLException e) {
            LOG.error(e.toString());
            //e.printStackTrace();
            //重新连接
            conn = InitConnectionn(connectionURL, connectionUserName, connectionPassword);
        }

        return null;
    }


    /**
     * 获取当前ID的offset
     *
     * @param startFrom
     * @return
     */
    private int getStatusDBIndex(int startFrom) {
        //从flume_meta 表中查出当前的ID是多少
        String dbIndex = queryOne("select currentIndex from flume_meta where source_tab= '" + table + "'");
        if (dbIndex != null) {
            return Integer.parseInt(dbIndex);
        }
        return startFrom;
    }


    /**
     * 将结果集转化为字符串，每一条数据是一个list集合，将每一个小的list集合转为字符串
     *
     * @param queryResult
     * @return
     */
    public List<String> getAllRows(List<List<Object>> queryResult) {
        List<String> allRows = new ArrayList<>();
        if (queryResult == null || queryResult.isEmpty()) {
            return allRows;
        }

        StringBuilder row = new StringBuilder();
        for (List<Object> rawRow : queryResult) {
            Object value = null;
            for (Object aRawRow : rawRow) {
                value = aRawRow;
                if (value == null) {
                    row.append(",");
                } else {
                    row.append(aRawRow.toString()).append(",");
                }
            }
            allRows.add(row.toString());
            row = new StringBuilder();
        }

        return allRows;
    }

    /**
     * 更新offset元数据状态,每次返回结果集后调用.
     * 必须记录每次查询的offset值,为程序中断续跑数据时使用,
     * 以ID 为
     *
     * @param size
     */
    public void updateOffset2DB(int size) {
        //sourceTab作为key,如果不存在则插入,存在则更新(每个源表对应一条记录)
        String sql = " inset into flume_meta(source_tab,currentIndex) VALUES(' "
                + this.table
                + "','" + (recordSize += size)
                + "') on DUPLICATE key update source_tab=values(source_tab),currentIndex=values(currentIndex) ";
        LOG.info("Update status sql: " + sql);
        execSql(sql);

    }

    /**
     * 执行SQL语句
     *
     * @param sql
     */
    private void execSql(String sql) {
        try {
            ps = conn.prepareStatement(sql);
            LOG.info("exec: " + sql);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public long getRunQueryDelay() {
        return 0;
    }

    public void close() {
        //关闭资源
        try {

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //查询一条数据的执行语句（当前ID）
    private String queryOne(String sql) {
        ResultSet result = null;
        try {
            ps = conn.prepareStatement(sql);
            result = ps.executeQuery();
            while (result.next()) {
                return result.getString(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }


}
