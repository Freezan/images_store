using System;
using System.Collections;
using System.Collections.Specialized;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;
using System.Data.Common;
using System.Collections.Generic;

namespace SSINT_Center_Manager
{
    /// <summary>
    /// 数据访问基础类(基于SQLServer)	
    /// Copyright (C) 2004-2008 By LiTianPing 
    /// </summary>
    public class DbHelperSQL
    {
        //数据库连接字符串(web.config来配置)		
        public static string connectionString = PubConstant.ConnectionString;

        public DbHelperSQL()
        {
        }

        #region 公用方法
        /// <summary>
        /// 判断是否存在某表的某个字段
        /// </summary>
        /// <param name="tableName">表名称</param>
        /// <param name="columnName">列名称</param>
        /// <returns>是否存在</returns>
        public static bool ColumnExists(string tableName, string columnName)
        {
            string sql = "select count(1) from syscolumns where [id]=object_id('" + tableName + "') and [name]='" + columnName + "'";
            object res = GetSingle(sql);
            if (res == null)
            {
                return false;
            }
            return Convert.ToInt32(res) > 0;
        }
        public static int GetMaxID(string FieldName, string TableName)
        {
            string strsql = "select max(" + FieldName + ")+1 from " + TableName;
            object obj = DbHelperSQL.GetSingle(strsql);
            if (obj == null)
            {
                return 1;
            }
            else
            {
                return int.Parse(obj.ToString());
            }
        }
        public static bool Exists(string strSql)
        {
            object obj = GetSingle(strSql);
            int cmdresult;
            if ((Object.Equals(obj, null)) || (Object.Equals(obj, System.DBNull.Value)))
            {
                cmdresult = 0;
            }
            else
            {
                cmdresult = int.Parse(obj.ToString());
            }
            if (cmdresult == 0)
            {
                return false;
            }
            else
            {
                return true;
            }
        }
        /// <summary>
        /// 表是否存在
        /// </summary>
        /// <param name="TableName"></param>
        /// <returns></returns>
        public static bool TabExists(string TableName)
        {
            string strsql = "select count(*) from sysobjects where id = object_id(N'[" + TableName + "]') and OBJECTPROPERTY(id, N'IsUserTable') = 1";
            //string strsql = "SELECT count(*) FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[" + TableName + "]') AND type in (N'U')";
            object obj = GetSingle(strsql);
            int cmdresult;
            if ((Object.Equals(obj, null)) || (Object.Equals(obj, System.DBNull.Value)))
            {
                cmdresult = 0;
            }
            else
            {
                cmdresult = int.Parse(obj.ToString());
            }
            if (cmdresult == 0)
            {
                return false;
            }
            else
            {
                return true;
            }
        }

        public static bool Exists(string strSql, params SqlParameter[] cmdParms)
        {
            object obj = GetSingle(strSql, cmdParms);
            int cmdresult;
            if ((Object.Equals(obj, null)) || (Object.Equals(obj, System.DBNull.Value)))
            {
                cmdresult = 0;
            }
            else
            {
                cmdresult = int.Parse(obj.ToString());
            }
            if (cmdresult == 0)
            {
                return false;
            }
            else
            {
                return true;
            }
        }
        /// <summary>
        /// 分页提取数据,指定数据服务器上必须建立GetRecordFromPage分页存储过程
        /// </summary>
        /// <param name="TableName">表名</param>
        /// <param name="statement">条件语句，不含where</param>
        /// <param name="SortField">排序字段</param>
        /// <param name="PageNum">页号</param>
        /// <param name="PageSize">页尺寸</param>
        /// <param name="Sort">是否排序</param>
        /// <returns>数据集合</returns>
        public static DataTable GetDataByPageNum(string TableName, string statement, string SortField, int PageNum, int PageSize, bool Sort)
        {
            IDbDataParameter[] ps = new SqlParameter[6];
            ps[0] = new SqlParameter("tblName", DbType.String);
            ps[1] = new SqlParameter("strWhere", DbType.String);
            ps[2] = new SqlParameter("PageIndex", DbType.Int16);
            ps[3] = new SqlParameter("PageSize", DbType.Int16);
            ps[4] = new SqlParameter("fldName", DbType.String);
            ps[5] = new SqlParameter("OrderType", DbType.Boolean);
            ps[0].Value = TableName;
            ps[1].Value = statement;
            ps[2].Value = PageNum;
            ps[3].Value = PageSize;
            ps[4].Value = SortField;
            ps[5].Value = Sort;
            return RunProcedure("GetRecordFromPage", ps, "Table0").Tables[0];
        }
        /// <summary>
        /// 分页提取数据
        /// </summary>
        /// <param name="SQLStatement">SQL语句序列，不允许含排序字段</param>
        /// <param name="SortField">排序字段</param>
        /// <param name="PageNum">页号</param>
        /// <param name="PageSize">页尺寸</param>
        /// <param name="Sort">排序，为真是降序排列，默认升序</param>
        /// <returns></returns>
        public static DataSet GetDataByPageNum(string SQLStatement, string SortField, int PageNum, int PageSize, bool Sort)
        {
            IDbDataParameter[] ps = new SqlParameter[6];
            ps[0] = new SqlParameter("selectsql", DbType.String);
            ps[1] = new SqlParameter("PageIndex", DbType.Int16);
            ps[2] = new SqlParameter("PageSize", DbType.Int16);
            ps[3] = new SqlParameter("fldName", DbType.String);
            ps[4] = new SqlParameter("OrderType", DbType.Boolean);
            ps[0].Value = SQLStatement.ToLower();
            ps[1].Value = PageNum;
            ps[2].Value = PageSize;
            ps[3].Value = SortField;
            ps[4].Value = Sort;
            return RunProcedure("GetRecordFromPageBySql", ps, "Table0");
        }
        /// <summary>
        /// 存储过程无参提取数据
        /// </summary>
        /// <param name="StoreProcName">过程名</param>
        /// <returns></returns>
        public static DataSet GetDataByStoreProc(string StoreProcName)
        {
            return RunProcedure(StoreProcName, null, "Table0");
        }
        /// <summary>
        /// 动态参数方式使用存储过程提取数据
        /// </summary>
        /// <param name="StoreProcName">存储过程名</param>
        /// <param name="ParamValueList">参数列表</param>
        /// <returns></returns>
        public static DataSet GetDataByStoreProc(string spName, params object[] parameterValues)
        {
            try
            {
                //if we got parameter values, we need to figure out where they go
                if ((parameterValues != null) && (parameterValues.Length > 0))
                {
                    //pull the parameters for this stored procedure from the parameter cache (or discover them & populet the cache)
                    //string connectionString = Funlib.GetConfigByName("SQLConnction");
                    string connectionString = PubConstant.ConnectionString;
                    SqlParameter[] commandParameters = SqlHelperParameterCache.GetSpParameterSet(connectionString, spName);
                    //assign the provided values to these parameters based on parameter order
                    SqlAssignParameterValues(commandParameters, parameterValues);

                    //call the overload that takes an array of SqlParameters
                    return RunProcedure(spName, commandParameters, "Table0");
                }
                //otherwise we can just call the SP without params
                else
                {
                    return RunProcedure(spName, null, "Table0");
                }
            }
            catch (Exception E)
            {
                Log.WriteLog(E.Message);
                throw E;
            };

        }

        /// <summary>
        /// 执行存储过程
        /// </summary>
        /// <param name="StoreProcName"></param>
        /// <returns></returns>
        public static int ExecuteByStoreProc(string StoreProcName)
        {
            int RowAff = 0;
            return RunProcedure(StoreProcName, null, out RowAff);
        }

        public static int ExecuteByStoreProc(string storedProcName, IDataParameter[] parameters)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                int i = 0;
                connection.Open();
                SqlCommand command = BuildQueryCommand(connection, storedProcName, parameters);
                command.CommandType = CommandType.StoredProcedure;
                i = command.ExecuteNonQuery();
                //returnReader = command.ExecuteReader(CommandBehavior.CloseConnection);

                return i;
            }
        }
        /// <summary>
        /// 要执行此函数，程序的返回值参数必须是ReturnValue
        /// </summary>
        /// <param name="storedProcName">存储过程</param>
        /// <param name="parameters">参数列表</param>
        /// <param name="ResultValue">返回值</param>
        /// <returns></returns>
        public static object ExecuteByStoreProc(string storedProcName, IDataParameter[] parameters, bool ResultValue)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                object Result;
                connection.Open();
                SqlCommand command = BuildQueryCommand(connection, storedProcName, parameters);
                if (ResultValue)
                {
                    SqlParameter sp = new SqlParameter();
                    sp.ParameterName = "ReturnValue";
                    sp.Size = 100;
                    sp.Direction = ParameterDirection.Output;
                    command.Parameters.Add(sp);
                }
                command.CommandType = CommandType.StoredProcedure;
                command.ExecuteNonQuery();
                Result = command.Parameters["ReturnValue"].Value;
                return Result;
            }
        }


        /// <summary>
        /// 保存数据到库
        /// </summary>
        /// <param name="dt">数据表，DataTable对象</param>
        /// <param name="filter">过滤字符串</param>
        /// <param name="TableName">表名</param>
        /// <returns></returns>
        public bool SaveData(DataTable dt, string filter, string TableName)
        {
            using (SqlConnection dbcon = new SqlConnection(PubConstant.ConnectionString))
            {
                dbcon.Open();
                string ft = "*";
                for (int i = 0; i < dt.Columns.Count; i++)
                {
                    if (ft.Equals("*"))
                    {
                        ft = dt.Columns[i].ColumnName;
                    }
                    else
                    {
                        ft = ft + "," + dt.Columns[i].ColumnName;
                    }
                }
                string wherestr = filter == "" ? "1<>1" : filter;
                string Sqlstr = "select " + ft + " from " + TableName + " where " + wherestr;
                try
                {
                    SqlDataAdapter da = new SqlDataAdapter(Sqlstr, dbcon as SqlConnection);
                    SqlCommandBuilder objCommandBuilder = new SqlCommandBuilder(da);
                    da.InsertCommand = objCommandBuilder.GetInsertCommand();
                    da.UpdateCommand = objCommandBuilder.GetUpdateCommand();
                    da.Update(dt);
                    da.Dispose();
                    return true;
                }
                catch (Exception ex)
                {
                    Log.WriteLog(ex.Message);
                    throw ex;
                }
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="FieldName"></param>
        /// <param name="FieldValue"></param>
        /// <returns></returns>
        public int DelByID(string TableName, string FieldName, object FieldValue, DbType ValueType)
        {
            bool AddSign = false;
            if (ValueType == DbType.String || ValueType == DbType.Date || ValueType == DbType.DateTime || ValueType == DbType.Guid)
                AddSign = true;

            string SqlStr = string.Format("delete {0} where {1}={2}", TableName, FieldName, AddSign ? "'" + FieldValue + "'" : FieldValue);
            return ExecuteSql(SqlStr);
        }
        /// <summary>
        /// 返回指定条件的表的记录数
        /// </summary>
        /// <param name="TableName">表名</param>
        /// <param name="Strwhere">条件字符串，不含where</param>
        /// <returns></returns>
        public static int GetTableRecordCount(string TableName, string Strwhere)
        {
            string Sql = string.Format("select count(*) from {0} ", TableName);
            if (Strwhere != "")
                Sql += " where " + Strwhere;
            return (int)GetSingle(Sql);
        }
        #endregion

        #region  执行简单SQL语句

        /// <summary>
        /// 执行SQL语句，返回影响的记录数
        /// </summary>
        /// <param name="SQLString">SQL语句</param>
        /// <returns>影响的记录数</returns>
        public static int ExecuteSql(string SQLString)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                using (SqlCommand cmd = new SqlCommand(SQLString, connection))
                {
                    try
                    {
                        connection.Open();
                        int rows = cmd.ExecuteNonQuery();
                        return rows;
                    }
                    catch (System.Data.SqlClient.SqlException e)
                    {
                        connection.Close();
                        Log.WriteLog(e.Message);
                        throw e;
                    }
                }
            }
        }
        /// <summary>
        /// 执行SQL语句，返回记录行数
        /// </summary>
        /// <param name="SQLString">SQL语句</param>
        /// <param name="Times">超时时间值</param>
        /// <returns></returns>
        public static int ExecuteSqlByTime(string SQLString, int Times)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                using (SqlCommand cmd = new SqlCommand(SQLString, connection))
                {
                    try
                    {
                        connection.Open();
                        cmd.CommandTimeout = Times;
                        int rows = cmd.ExecuteNonQuery();
                        return rows;
                    }
                    catch (System.Data.SqlClient.SqlException e)
                    {
                        connection.Close();
                        Log.WriteLog(e.Message);
                        throw e;
                    }
                }
            }
        }

        /// <summary>
        /// 执行多条SQL语句，实现数据库事务。
        /// </summary>
        /// <param name="SQLStringList">多条SQL语句</param>		
        public static int ExecuteSqlTran(List<String> SQLStringList)
        {
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.Connection = conn;
                SqlTransaction tx = conn.BeginTransaction();
                cmd.Transaction = tx;
                try
                {
                    int count = 0;
                    for (int n = 0; n < SQLStringList.Count; n++)
                    {
                        string strsql = SQLStringList[n];
                        if (strsql.Trim().Length > 1)
                        {
                            cmd.CommandText = strsql;
                            count += cmd.ExecuteNonQuery();
                        }
                    }
                    tx.Commit();
                    return count;
                }
                catch (Exception e)
                {
                    tx.Rollback();
                    Log.WriteLog(e.Message);
                    return 0;
                }
            }
        }
        /// <summary>
        /// 执行带一个存储过程参数的的SQL语句。
        /// </summary>
        /// <param name="SQLString">SQL语句</param>
        /// <param name="content">参数内容,比如一个字段是格式复杂的文章，有特殊符号，可以通过这个方式添加</param>
        /// <returns>影响的记录数</returns>
        public static int ExecuteSql(string SQLString, string content)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand cmd = new SqlCommand(SQLString, connection);
                System.Data.SqlClient.SqlParameter myParameter = new System.Data.SqlClient.SqlParameter("@content", SqlDbType.NText);
                myParameter.Value = content;
                cmd.Parameters.Add(myParameter);
                try
                {
                    connection.Open();
                    int rows = cmd.ExecuteNonQuery();
                    return rows;
                }
                catch (System.Data.SqlClient.SqlException e)
                {
                    Log.WriteLog(e.Message);
                    throw e;
                }
                finally
                {
                    cmd.Dispose();
                    connection.Close();
                }
            }
        }
        /// <summary>
        /// 执行带一个存储过程参数的的SQL语句。
        /// </summary>
        /// <param name="SQLString">SQL语句</param>
        /// <param name="content">参数内容,比如一个字段是格式复杂的文章，有特殊符号，可以通过这个方式添加</param>
        /// <returns>影响的记录数</returns>
        public static object ExecuteSqlGet(string SQLString, string content)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand cmd = new SqlCommand(SQLString, connection);
                System.Data.SqlClient.SqlParameter myParameter = new System.Data.SqlClient.SqlParameter("@content", SqlDbType.NText);
                myParameter.Value = content;
                cmd.Parameters.Add(myParameter);
                try
                {
                    connection.Open();
                    object obj = cmd.ExecuteScalar();
                    if ((Object.Equals(obj, null)) || (Object.Equals(obj, System.DBNull.Value)))
                    {
                        return null;
                    }
                    else
                    {
                        return obj;
                    }
                }
                catch (System.Data.SqlClient.SqlException e)
                {
                    Log.WriteLog(e.Message);
                    throw e;
                }
                finally
                {
                    cmd.Dispose();
                    connection.Close();
                }
            }
        }
        /// <summary>
        /// 向数据库里插入图像格式的字段(和上面情况类似的另一种实例)
        /// </summary>
        /// <param name="strSQL">SQL语句</param>
        /// <param name="fs">图像字节,数据库的字段类型为image的情况</param>
        /// <returns>影响的记录数</returns>
        public static int ExecuteSqlInsertImg(string strSQL, byte[] fs)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand cmd = new SqlCommand(strSQL, connection);
                System.Data.SqlClient.SqlParameter myParameter = new System.Data.SqlClient.SqlParameter("@fs", SqlDbType.Image);
                myParameter.Value = fs;
                cmd.Parameters.Add(myParameter);
                try
                {
                    connection.Open();
                    int rows = cmd.ExecuteNonQuery();
                    return rows;
                }
                catch (System.Data.SqlClient.SqlException e)
                {
                    Log.WriteLog(e.Message);
                    throw e;
                }
                finally
                {
                    cmd.Dispose();
                    connection.Close();
                }
            }
        }

        /// <summary>
        /// 执行一条计算查询结果语句，返回查询结果（object）。
        /// </summary>
        /// <param name="SQLString">计算查询结果语句</param>
        /// <returns>查询结果（object）</returns>
        public static object GetSingle(string SQLString)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                using (SqlCommand cmd = new SqlCommand(SQLString, connection))
                {
                    try
                    {
                        connection.Open();
                        object obj = cmd.ExecuteScalar();
                        if ((Object.Equals(obj, null)) || (Object.Equals(obj, System.DBNull.Value)))
                        {
                            return null;
                        }
                        else
                        {
                            return obj;
                        }
                    }
                    catch (System.Data.SqlClient.SqlException e)
                    {
                        connection.Close();
                        Log.WriteLog(e.Message);
                        throw e;
                    }
                }
            }
        }
        /// <summary>
        /// 执行一条计算查询结果语句，返回查询结果（object）。
        /// </summary>
        /// <param name="SQLString">计算查询结果语句</param>
        /// <param name="Times">超时设置</param>
        /// <returns>查询结果</returns>
        public static object GetSingle(string SQLString, int Times)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                using (SqlCommand cmd = new SqlCommand(SQLString, connection))
                {
                    try
                    {
                        connection.Open();
                        cmd.CommandTimeout = Times;
                        object obj = cmd.ExecuteScalar();
                        if ((Object.Equals(obj, null)) || (Object.Equals(obj, System.DBNull.Value)))
                        {
                            return null;
                        }
                        else
                        {
                            return obj;
                        }
                    }
                    catch (System.Data.SqlClient.SqlException e)
                    {
                        connection.Close();
                        Log.WriteLog(e.Message);
                        throw e;
                    }
                }
            }
        }
        /// <summary>
        /// 执行查询语句，返回SqlDataReader ( 注意：调用该方法后，一定要对SqlDataReader进行Close )
        /// </summary>
        /// <param name="strSQL">查询语句</param>
        /// <returns>SqlDataReader</returns>
        public static SqlDataReader ExecuteReader(string strSQL)
        {
            SqlConnection connection = new SqlConnection(connectionString);
            SqlCommand cmd = new SqlCommand(strSQL, connection);
            try
            {
                connection.Open();
                SqlDataReader myReader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                return myReader;
            }
            catch (System.Data.SqlClient.SqlException e)
            {
                Log.WriteLog(e.Message);
                throw e;
            }

        }
        /// <summary>
        /// 执行查询语句，返回DataSet
        /// </summary>
        /// <param name="SQLString">查询语句</param>
        /// <returns>DataSet</returns>
        public static DataTable Query(string SQLString)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                DataSet ds = new DataSet();
                try
                {
                    connection.Open();
                    SqlDataAdapter command = new SqlDataAdapter(SQLString, connection);
                    command.Fill(ds, "ds");
                }
                catch (System.Data.SqlClient.SqlException ex)
                {
                    Log.WriteLog(ex.Message);
                    throw new Exception(ex.Message);
                }
                return ds.Tables[0];
            }
        }


        public static DataSet Query(string SQLString, int Times)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                DataSet ds = new DataSet();
                try
                {
                    connection.Open();
                    SqlDataAdapter command = new SqlDataAdapter(SQLString, connection);
                    command.SelectCommand.CommandTimeout = Times;
                    command.Fill(ds, "ds");
                }
                catch (System.Data.SqlClient.SqlException ex)
                {
                    Log.WriteLog(ex.Message);
                    throw new Exception(ex.Message);
                }
                return ds;
            }
        }


        /// <summary>
        /// 根据表名得到数据
        /// </summary>
        /// <param name="TableName">表名</param>
        /// <returns></returns>
        public static DataTable QueryData(string TableName)
        {
            return Query(string.Format("select * from {0}", TableName));
        }
        /// <summary>
        /// 返回表结构
        /// </summary>
        /// <param name="TableName">指定表名</param>
        /// <returns></returns>
        public static DataTable GetSchema(string TableName)
        {
            return Query(string.Format("select * from {0} where 1<>1", TableName));
        }

        public static bool SaveData(DataTable dt, string TableName)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                string ft = "*";
                for (int i = 0; i < dt.Columns.Count; i++)
                {
                    if (ft.Equals("*"))
                    {
                        ft = dt.Columns[i].ColumnName;
                    }
                    else
                    {
                        ft = ft + "," + dt.Columns[i].ColumnName;
                    }
                }
                string Sqlstr = "select " + ft + " from " + TableName + " where 1<>1";
                connection.Open();
                try
                {
                    SqlDataAdapter da = new SqlDataAdapter(Sqlstr, connection);
                    SqlCommandBuilder objCommandBuilder = new SqlCommandBuilder(da);
                    da.InsertCommand = objCommandBuilder.GetInsertCommand();
                    da.UpdateCommand = objCommandBuilder.GetUpdateCommand();
                    da.Update(dt);
                    da.Dispose();
                    return true;
                }
                catch (Exception ex)
                {
                    Log.WriteLog(ex.Message);
                    throw ex;
                }
            }



        }
        #endregion

        #region 执行带参数的SQL语句

        /// <summary>
        /// 执行SQL语句，返回影响的记录数
        /// </summary>
        /// <param name="SQLString">SQL语句</param>
        /// <returns>影响的记录数</returns>
        public static int ExecuteSql(string SQLString, params SqlParameter[] cmdParms)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                using (SqlCommand cmd = new SqlCommand())
                {
                    try
                    {
                        PrepareCommand(cmd, connection, null, SQLString, cmdParms);
                        int rows = cmd.ExecuteNonQuery();
                        cmd.Parameters.Clear();
                        return rows;
                    }
                    catch (System.Data.SqlClient.SqlException e)
                    {
                        Log.WriteLog(e.Message);
                        throw e;
                    }
                }
            }
        }


        /// <summary>
        /// 执行多条SQL语句，实现数据库事务。
        /// </summary>
        /// <param name="SQLStringList">SQL语句的哈希表（key为sql语句，value是该语句的SqlParameter[]）</param>
        public static void ExecuteSqlTran(Hashtable SQLStringList)
        {
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                using (SqlTransaction trans = conn.BeginTransaction())
                {
                    SqlCommand cmd = new SqlCommand();
                    try
                    {
                        //循环
                        foreach (DictionaryEntry myDE in SQLStringList)
                        {
                            string cmdText = myDE.Key.ToString();
                            SqlParameter[] cmdParms = (SqlParameter[])myDE.Value;
                            PrepareCommand(cmd, conn, trans, cmdText, cmdParms);
                            int val = cmd.ExecuteNonQuery();
                            cmd.Parameters.Clear();
                        }
                        trans.Commit();
                    }
                    catch (Exception e)
                    {
                        trans.Rollback();
                        Log.WriteLog(e.Message);
                        throw;
                    }
                }
            }
        }
        /// <summary>
        /// 执行多条SQL语句，实现数据库事务。
        /// </summary>
        /// <param name="SQLStringList">SQL语句的哈希表（key为sql语句，value是该语句的SqlParameter[]）</param>
        public static int ExecuteSqlTran(System.Collections.Generic.List<CommandInfo> cmdList)
        {
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                using (SqlTransaction trans = conn.BeginTransaction())
                {
                    SqlCommand cmd = new SqlCommand();
                    try
                    {
                        int count = 0;
                        //循环
                        foreach (CommandInfo myDE in cmdList)
                        {
                            string cmdText = myDE.CommandText;
                            SqlParameter[] cmdParms = (SqlParameter[])myDE.Parameters;
                            PrepareCommand(cmd, conn, trans, cmdText, cmdParms);

                            if (myDE.EffentNextType == EffentNextType.WhenHaveContine || myDE.EffentNextType == EffentNextType.WhenNoHaveContine)
                            {
                                if (myDE.CommandText.ToLower().IndexOf("count(") == -1)
                                {
                                    trans.Rollback();
                                    return 0;
                                }

                                object obj = cmd.ExecuteScalar();
                                bool isHave = false;
                                if (obj == null && obj == DBNull.Value)
                                {
                                    isHave = false;
                                }
                                isHave = Convert.ToInt32(obj) > 0;

                                if (myDE.EffentNextType == EffentNextType.WhenHaveContine && !isHave)
                                {
                                    trans.Rollback();
                                    return 0;
                                }
                                if (myDE.EffentNextType == EffentNextType.WhenNoHaveContine && isHave)
                                {
                                    trans.Rollback();
                                    return 0;
                                }
                                continue;
                            }
                            int val = cmd.ExecuteNonQuery();
                            count += val;
                            if (myDE.EffentNextType == EffentNextType.ExcuteEffectRows && val == 0)
                            {
                                trans.Rollback();
                                return 0;
                            }
                            cmd.Parameters.Clear();
                        }
                        trans.Commit();
                        return count;
                    }
                    catch (Exception e)
                    {
                        trans.Rollback();
                        Log.WriteLog(e.Message);
                        throw;
                    }
                }
            }
        }
        /// <summary>
        /// 执行多条SQL语句，实现数据库事务。
        /// </summary>
        /// <param name="SQLStringList">SQL语句的哈希表（key为sql语句，value是该语句的SqlParameter[]）</param>
        public static void ExecuteSqlTranWithIndentity(System.Collections.Generic.List<CommandInfo> SQLStringList)
        {
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                using (SqlTransaction trans = conn.BeginTransaction())
                {
                    SqlCommand cmd = new SqlCommand();
                    try
                    {
                        int indentity = 0;
                        //循环
                        foreach (CommandInfo myDE in SQLStringList)
                        {
                            string cmdText = myDE.CommandText;
                            SqlParameter[] cmdParms = (SqlParameter[])myDE.Parameters;
                            foreach (SqlParameter q in cmdParms)
                            {
                                if (q.Direction == ParameterDirection.InputOutput)
                                {
                                    q.Value = indentity;
                                }
                            }
                            PrepareCommand(cmd, conn, trans, cmdText, cmdParms);
                            int val = cmd.ExecuteNonQuery();
                            foreach (SqlParameter q in cmdParms)
                            {
                                if (q.Direction == ParameterDirection.Output)
                                {
                                    indentity = Convert.ToInt32(q.Value);
                                }
                            }
                            cmd.Parameters.Clear();
                        }
                        trans.Commit();
                    }
                    catch (Exception e)
                    {
                        trans.Rollback();
                        Log.WriteLog(e.Message);
                        throw;
                    }
                }
            }
        }
        /// <summary>
        /// 执行多条SQL语句，实现数据库事务。
        /// </summary>
        /// <param name="SQLStringList">SQL语句的哈希表（key为sql语句，value是该语句的SqlParameter[]）</param>
        public static void ExecuteSqlTranWithIndentity(Hashtable SQLStringList)
        {
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                using (SqlTransaction trans = conn.BeginTransaction())
                {
                    SqlCommand cmd = new SqlCommand();
                    try
                    {
                        int indentity = 0;
                        //循环
                        foreach (DictionaryEntry myDE in SQLStringList)
                        {
                            string cmdText = myDE.Key.ToString();
                            SqlParameter[] cmdParms = (SqlParameter[])myDE.Value;
                            foreach (SqlParameter q in cmdParms)
                            {
                                if (q.Direction == ParameterDirection.InputOutput)
                                {
                                    q.Value = indentity;
                                }
                            }
                            PrepareCommand(cmd, conn, trans, cmdText, cmdParms);
                            int val = cmd.ExecuteNonQuery();
                            foreach (SqlParameter q in cmdParms)
                            {
                                if (q.Direction == ParameterDirection.Output)
                                {
                                    indentity = Convert.ToInt32(q.Value);
                                }
                            }
                            cmd.Parameters.Clear();
                        }
                        trans.Commit();
                    }
                    catch (Exception e)
                    {
                        trans.Rollback();
                        Log.WriteLog(e.Message);
                        throw;
                    }
                }
            }
        }
        /// <summary>
        /// 执行一条计算查询结果语句，返回查询结果（object）。
        /// </summary>
        /// <param name="SQLString">计算查询结果语句</param>
        /// <returns>查询结果（object）</returns>
        public static object GetSingle(string SQLString, params SqlParameter[] cmdParms)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                using (SqlCommand cmd = new SqlCommand())
                {
                    try
                    {
                        PrepareCommand(cmd, connection, null, SQLString, cmdParms);
                        object obj = cmd.ExecuteScalar();
                        cmd.Parameters.Clear();
                        if ((Object.Equals(obj, null)) || (Object.Equals(obj, System.DBNull.Value)))
                        {
                            return null;
                        }
                        else
                        {
                            return obj;
                        }
                    }
                    catch (System.Data.SqlClient.SqlException e)
                    {
                        Log.WriteLog(e.Message);
                        throw e;
                    }
                }
            }
        }

        /// <summary>
        /// 执行查询语句，返回SqlDataReader ( 注意：调用该方法后，一定要对SqlDataReader进行Close )
        /// </summary>
        /// <param name="strSQL">查询语句</param>
        /// <returns>SqlDataReader</returns>
        public static SqlDataReader ExecuteReader(string SQLString, params SqlParameter[] cmdParms)
        {
            SqlConnection connection = new SqlConnection(connectionString);
            SqlCommand cmd = new SqlCommand();
            try
            {
                PrepareCommand(cmd, connection, null, SQLString, cmdParms);
                SqlDataReader myReader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                cmd.Parameters.Clear();
                return myReader;
            }
            catch (System.Data.SqlClient.SqlException e)
            {
                Log.WriteLog(e.Message);
                throw e;
            }
            //			finally
            //			{
            //				cmd.Dispose();
            //				connection.Close();
            //			}	

        }

        /// <summary>
        /// 执行查询语句，返回DataSet
        /// </summary>
        /// <param name="SQLString">查询语句</param>
        /// <returns>DataSet</returns>
        public static DataSet Query(string SQLString, params SqlParameter[] cmdParms)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand cmd = new SqlCommand();
                PrepareCommand(cmd, connection, null, SQLString, cmdParms);
                using (SqlDataAdapter da = new SqlDataAdapter(cmd))
                {
                    DataSet ds = new DataSet();
                    try
                    {
                        da.Fill(ds, "ds");
                        cmd.Parameters.Clear();
                    }
                    catch (System.Data.SqlClient.SqlException ex)
                    {
                        throw new Exception(ex.Message);
                    }
                    return ds;
                }
            }
        }


        private static void PrepareCommand(SqlCommand cmd, SqlConnection conn, SqlTransaction trans, string cmdText, SqlParameter[] cmdParms)
        {
            if (conn.State != ConnectionState.Open)
                conn.Open();
            cmd.Connection = conn;
            cmd.CommandText = cmdText;
            if (trans != null)
                cmd.Transaction = trans;
            cmd.CommandType = CommandType.Text;//cmdType;
            if (cmdParms != null)
            {
                foreach (SqlParameter parameter in cmdParms)
                {
                    if ((parameter.Direction == ParameterDirection.InputOutput || parameter.Direction == ParameterDirection.Input) &&
                        (parameter.Value == null))
                    {
                        parameter.Value = DBNull.Value;
                    }
                    cmd.Parameters.Add(parameter);
                }
            }
        }

        #endregion

        #region 存储过程操作

        /// <summary>
        /// 执行存储过程，返回SqlDataReader ( 注意：调用该方法后，一定要对SqlDataReader进行Close )
        /// </summary>
        /// <param name="storedProcName">存储过程名</param>
        /// <param name="parameters">存储过程参数</param>
        /// <returns>SqlDataReader</returns>
        public static SqlDataReader RunProcedure(string storedProcName, IDataParameter[] parameters)
        {
            SqlConnection connection = new SqlConnection(connectionString);
            SqlDataReader returnReader;
            connection.Open();
            SqlCommand command = BuildQueryCommand(connection, storedProcName, parameters);
            command.CommandType = CommandType.StoredProcedure;
            returnReader = command.ExecuteReader(CommandBehavior.CloseConnection);
            return returnReader;

        }


        /// <summary>
        /// 执行存储过程
        /// </summary>
        /// <param name="storedProcName">存储过程名</param>
        /// <param name="parameters">存储过程参数</param>
        /// <param name="tableName">DataSet结果中的表名</param>
        /// <returns>DataSet</returns>
        public static DataSet RunProcedure(string storedProcName, IDataParameter[] parameters, string tableName)
        {
            SqlConnection connection = new SqlConnection(PubConstant.ConnectionString);
            DataSet dataSet = new DataSet();
            connection.Open();
            SqlDataAdapter sqlDA = new SqlDataAdapter();
            sqlDA.SelectCommand = BuildQueryCommand(connection, storedProcName, parameters);
            sqlDA.Fill(dataSet, tableName);
            connection.Close();
            return dataSet;

        }

        public static DataSet RunProcedure(string storedProcName, IDataParameter[] parameters, string tableName, int Times)
        {
            using (SqlConnection connection = new SqlConnection(PubConstant.ConnectionString))
            {
                DataSet dataSet = new DataSet();
                connection.Open();
                SqlDataAdapter sqlDA = new SqlDataAdapter();
                sqlDA.SelectCommand = BuildQueryCommand(connection, storedProcName, parameters);
                sqlDA.SelectCommand.CommandTimeout = Times;
                sqlDA.Fill(dataSet, tableName);
                connection.Close();
                return dataSet;
            }
        }
        /// <summary>
        /// 执行存储过程，返回影响的记录行数
        /// </summary>
        /// <param name="storeProcName">存储过程</param>
        /// <returns></returns>
        public static int RunProcedure(string storeProcName)
        {
            int i = 0;
            using (SqlConnection conn = new SqlConnection(PubConstant.ConnectionString))
            {
                SqlCommand sqlcomm = new SqlCommand(storeProcName, conn);
                sqlcomm.CommandType = CommandType.StoredProcedure;
                i = sqlcomm.ExecuteNonQuery();
            }
            return i;
        }
        /// <summary>
        /// 构建 SqlCommand 对象(用来返回一个结果集，而不是一个整数值)
        /// </summary>
        /// <param name="connection">数据库连接</param>
        /// <param name="storedProcName">存储过程名</param>
        /// <param name="parameters">存储过程参数</param>
        /// <returns>SqlCommand</returns>
        private static SqlCommand BuildQueryCommand(SqlConnection connection, string storedProcName, IDataParameter[] parameters)
        {
            SqlCommand command = new SqlCommand(storedProcName, connection);
            command.CommandType = CommandType.StoredProcedure;

            if (parameters != null)
                foreach (SqlParameter parameter in parameters)
                {
                    if (parameter != null)
                    {
                        // 检查未分配值的输出参数,将其分配以DBNull.Value.
                        if ((parameter.Direction == ParameterDirection.InputOutput || parameter.Direction == ParameterDirection.Input) &&
                            (parameter.Value == null))
                        {
                            parameter.Value = DBNull.Value;
                        }
                        command.Parameters.Add(parameter);
                    }
                }

            return command;
        }

        /// <summary>
        /// 执行存储过程，返回影响的行数		
        /// </summary>
        /// <param name="storedProcName">存储过程名</param>
        /// <param name="parameters">存储过程参数</param>
        /// <param name="rowsAffected">影响的行数</param>
        /// <returns></returns>
        public static int RunProcedure(string storedProcName, IDataParameter[] parameters, out int rowsAffected)
        {
            using (SqlConnection connection = new SqlConnection(PubConstant.ConnectionString))
            {
                int result;
                connection.Open();
                SqlCommand command = BuildIntCommand(connection, storedProcName, parameters);
                rowsAffected = command.ExecuteNonQuery();
                result = (int)command.Parameters["ReturnValue"].Value;
                //Connection.Close();
                return result;
            }
        }
        /// <summary>
        /// 动态参数执行执储过程
        /// </summary>
        /// <param name="spName">参数名称</param>
        /// <param name="parameterValues">参数值列表</param>
        /// <returns></returns>
        public static int RunProcedure(string spName, params object[] parameterValues)
        {
            int RowAff = 0;
            try
            {
                //if we got parameter values, we need to figure out where they go
                if ((parameterValues != null) && (parameterValues.Length > 0))
                {
                    //pull the parameters for this stored procedure from the parameter cache (or discover them & populet the cache)
                    //string connectionString = Funlib.GetConfigByName("SQLConnction");
                    string connectionString = PubConstant.ConnectionString;
                    SqlParameter[] commandParameters = SqlHelperParameterCache.GetSpParameterSet(connectionString, spName);
                    //assign the provided values to these parameters based on parameter order
                    SqlAssignParameterValues(commandParameters, parameterValues);

                    //call the overload that takes an array of SqlParameters
                    RowAff = RunProcedure(spName, commandParameters, RowAff);
                }
                //otherwise we can just call the SP without params
                else
                {
                    RowAff = RunProcedure(spName, null, RowAff);
                }
            }
            catch (Exception E)
            {
                Log.WriteLog(E.Message);
                throw E;
            };
            return RowAff;
        }

        private static void SqlAssignParameterValues(SqlParameter[] commandParameters, object[] parameterValues)
        {
            try
            {
                if ((commandParameters == null) || (parameterValues == null))
                {
                    //do nothing if we get no data
                    return;
                }

                // we must have the same number of values as we pave parameters to put them in
                if (commandParameters.Length != parameterValues.Length)
                {
                    throw new ArgumentException("参数数目与过程的数据不一致！");
                }

                //iterate through the SqlParameters, assigning the values from the corresponding position in the 
                //value array
                for (int i = 0, j = commandParameters.Length; i < j; i++)
                {
                    commandParameters[i].Value = parameterValues[i];
                }
            }
            catch (Exception E)
            {
                throw E;
            };

        }

        /// <summary>
        /// 创建 SqlCommand 对象实例(用来返回一个整数值)	
        /// </summary>
        /// <param name="storedProcName">存储过程名</param>
        /// <param name="parameters">存储过程参数</param>
        /// <returns>SqlCommand 对象实例</returns>
        private static SqlCommand BuildIntCommand(SqlConnection connection, string storedProcName, IDataParameter[] parameters)
        {
            SqlCommand command = BuildQueryCommand(connection, storedProcName, parameters);
            command.Parameters.Add(new SqlParameter("ReturnValue",
                SqlDbType.Int, 4, ParameterDirection.ReturnValue,
                false, 0, 0, string.Empty, DataRowVersion.Default, null));
            return command;
        }
        #endregion

        #region 添加一个传连接字符串的简单执行sql语句 2009-2-4
        /// <summary>
        /// 执行SQL语句，返回影响的记录数
        /// </summary>
        /// <param name="SQLString">SQL语句</param>
        /// <param name="strConnectionString">连接字符串</param>
        /// <returns>影响的记录数</returns>
        public static int ExecuteSql(string SQLString, string strConnectionString, params SqlParameter[] cmdParms)
        {
            using (SqlConnection connection = new SqlConnection(strConnectionString))
            {
                using (SqlCommand cmd = new SqlCommand())
                {
                    try
                    {
                        PrepareCommand(cmd, connection, null, SQLString, cmdParms);
                        int rows = cmd.ExecuteNonQuery();
                        cmd.Parameters.Clear();
                        return rows;
                    }
                    catch (System.Data.SqlClient.SqlException e)
                    {
                        throw e;
                    }
                }
            }
        }

        /// <summary>
        /// 执行查询语句，返回DataSet
        /// </summary>
        /// <param name="SQLString">查询语句</param>
        /// <param name="strConnectionString">连接字符串</param>
        /// <returns>DataSet</returns>
        public static DataSet Query(string SQLString, string strConnectionString)
        {
            using (SqlConnection connection = new SqlConnection(strConnectionString))
            {
                DataSet ds = new DataSet();
                try
                {
                    connection.Open();
                    SqlDataAdapter command = new SqlDataAdapter(SQLString, connection);
                    command.Fill(ds, "ds");
                }
                catch (System.Data.SqlClient.SqlException ex)
                {
                    throw new Exception(ex.Message);
                }
                return ds;
            }
        }

        #endregion
    }
}
