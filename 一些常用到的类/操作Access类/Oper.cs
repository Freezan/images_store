using System;
using System.Collections.Generic;
using System.Text;
using System.Data.OleDb;
using System.Data;

namespace Safe_Train_System.DLL
{
    public class Oper
    {
        //string connStr = @"Provider=Microsoft.Jet.OLEDB.4.0;User ID='admin';Password='';Jet OLEDB:Database Password='@#sdev8801538';Data Source={0}\\Db.mdb;Mode=Share Deny Read|Share Deny Write;Persist Security Info=False;";
        string connStr = @"Provider=Microsoft.Jet.OLEDB.4.0;Data Source=C:\Users\Administrator\Desktop\Safe-Train-System\Safe-Train-System\Safe-Train-System\DataBase\data.mdb";
        string FileDir = "";

        public Oper(string _FileDir)
        {
            FileDir = _FileDir;
        }

        /// <summary>
        /// 返回数据库连接
        /// </summary>
        /// <returns></returns>
        private OleDbConnection GetConnection()
        {
            return new OleDbConnection(string.Format(connStr,FileDir));
        }

        public List<string> GetProvince()
        {
            OleDbConnection db = GetConnection();
            db.Open();
            OleDbCommand dbcomm = db.CreateCommand();
            dbcomm.CommandText = "select * from Video";
            OleDbDataReader dr = dbcomm.ExecuteReader();
            List<string> lists = new List<string>();
            while (dr.Read())
            {
                lists.Add(dr["VideoName"].ToString());
            }
            db.Close();
            return lists;
        }

        public List<string> GetLamp()
        {
            OleDbConnection db = GetConnection();
            db.Open();
            OleDbCommand dbcomm = db.CreateCommand();
            dbcomm.CommandText = "select * from Lamp";
            OleDbDataReader dr = dbcomm.ExecuteReader();
            List<string> lists = new List<string>();
            while (dr.Read())
            {
                lists.Add(dr["LampName"].ToString());
            }
            db.Close();
            return lists;            
        }

        public DataTable GetGuestInfo(string VideoName)
        {
            OleDbConnection db = GetConnection();
            db.Open();
            OleDbCommand dbcomm = db.CreateCommand();
            string Sql = string.Format("select id,seconds,stepname from Step1 where MoviceName='{0}' order by seconds", VideoName);
            dbcomm.CommandText = Sql;
            DataSet ds=new DataSet();
            OleDbDataAdapter da = new OleDbDataAdapter(dbcomm);
            da.Fill(ds);
            db.Close();
            return ds.Tables[0];
        }

        /// <summary>
/// 获取灯具数据
/// </summary>
/// <param name="LampName">灯具名称</param>
/// <returns></returns>
        public DataTable GetLampInfo(string LampName)
        {
            OleDbConnection db = GetConnection();
            db.Open();
            OleDbCommand dbcomm = db.CreateCommand();
            string Sql = string.Format("select * from lamp");
            if (LampName != "")
                Sql = Sql + string.Format(" where lampname='{0}'", LampName);
            dbcomm.CommandText = Sql;
            DataSet ds = new DataSet();
            OleDbDataAdapter da = new OleDbDataAdapter(dbcomm);
            da.Fill(ds);
            db.Close();
            return ds.Tables[0];
        }

        public DataTable GetLampInfo()
        {
            return GetLampInfo("");
        }

        public int GetStepInfo(string TableName,string VideoName,int seconds)
        {
            OleDbConnection db = GetConnection();
            db.Open();
            OleDbCommand dbcomm = db.CreateCommand();
            string Sql = string.Format("select id from {0} where seconds={1} and MoviceName='{2}'", TableName, seconds, VideoName);

            dbcomm.CommandText = Sql;
            DataSet ds = new DataSet();
            OleDbDataAdapter da = new OleDbDataAdapter(dbcomm);
            da.Fill(ds);
            db.Close();
            return ds.Tables[0].Rows.Count;
        }

        /// <summary>
        /// 插入一条记录
        /// </summary>
        /// <param name="Macid"></param>
        /// <param name="province"></param>
        /// <param name="City"></param>
        /// <param name="Regstr"></param>
        /// <param name="Startdate"></param>
        /// <param name="AllowNum"></param>
        /// <param name="UserName"></param>
        /// <param name="SchoolName"></param>
        /// <param name="Tel"></param>
        /// <param name="Mobile"></param>
        /// <param name="QQ"></param>
        /// <returns></returns>
        public int InsertGuest(string Macid, string province, string City, string Regstr, string Startdate, int AllowNum, string UserName,string SchoolName, string Tel, string Mobile, string QQ)
        {
            int i = 0;
            //OleDbConnection db = GetConnection();
            //db.Open();
            //OleDbCommand dbcomm = db.CreateCommand();
            //dbcomm.CommandText = string.Format("select count(*) as total from guestinfo where macid='{0}'", Macid);
            //i =Convert.ToInt16(dbcomm.ExecuteScalar());
            //db.Close();
            //if (i > 0)
            //{
            //    return -1;
           // }
           // else
           // {
                OleDbConnection db2 = GetConnection();
                db2.Open();
                string Sql = "insert into GuestInfo(macid,startdate,allownum,regstr,username,province,city,schoolname,tel,mobile,qq) " +
                    "values('{0}','{1}',{2},'{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}')";
                Sql = string.Format(Sql, Macid, Startdate, AllowNum, Regstr, UserName, province, City, SchoolName, Tel, Mobile, QQ);
                OleDbCommand dbcomm2 = db2.CreateCommand();
                dbcomm2.CommandText = Sql;
                i = dbcomm2.ExecuteNonQuery();
                db2.Close();
                return i;
            //}
        }

        public int ExecuteQuery(string SQL)
        {
            int i = 0;
            OleDbConnection db2 = GetConnection();
            db2.Open();
            OleDbCommand dbcomm2 = db2.CreateCommand();
            dbcomm2.CommandText = SQL;
            i = dbcomm2.ExecuteNonQuery();
            db2.Close();
            return i;
            //}
        }

        public DataTable GetDataByID(int Seconds,string VideoName,string Table1,string Table2)
        {
            OleDbConnection db = GetConnection();
            db.Open();
            OleDbCommand dbcomm = db.CreateCommand();
            string Sql="";
            if (Table2 != "")
            {
                Sql = string.Format("select * from {1} s1,{2} s2 where s1.seconds={0} and s1.MoviceName='{3}' and s1.seconds=s2.seconds and s1.MoviceName=s2.MoviceName", Seconds, Table1, Table2, VideoName);
            }
            else
            {
                Sql = string.Format("select * from {1} s1 where s1.seconds={0} and s1.MoviceName='{2}' ", Seconds, Table1,VideoName);
            }
            dbcomm.CommandText = Sql;
            DataSet ds = new DataSet();
            OleDbDataAdapter da = new OleDbDataAdapter(dbcomm);
            da.Fill(ds);
            db.Close();
            return ds.Tables[0];      
        }

        public int UpdateAllowNum(int Gid, int AllowNum, string Regstr)
        {
            OleDbConnection db = GetConnection();
            db.Open();
            OleDbCommand dbcomm = db.CreateCommand();
            string Sql = string.Format("update GuestInfo set allownum={0},regstr='{1}' where id={2}", AllowNum,Regstr,Gid);
            dbcomm.CommandText = Sql;
            int i = dbcomm.ExecuteNonQuery();
            db.Close();
            return i; 
       }

        public int GetVideo(string VideoName)
        {
            OleDbConnection db = GetConnection();
            db.Open();
            OleDbCommand dbcomm = db.CreateCommand();
            string Sql = string.Format("select * from video where videoname='{0}'", VideoName);
            dbcomm.CommandText = Sql;
            DataSet ds = new DataSet();
            OleDbDataAdapter da = new OleDbDataAdapter(dbcomm);
            da.Fill(ds);
            int i = Convert.ToInt16(ds.Tables[0].Rows.Count);
            db.Close();
            return i;         
        }

        public int ChgPwd(string NewPassword)
        {
            OleDbConnection db = GetConnection();
            db.Open();
            OleDbCommand dbcomm = db.CreateCommand();
            string Sql = string.Format("update logusers set passwords='{0}' where UserName='admin'",NewPassword);
            dbcomm.CommandText = Sql;
            int i = dbcomm.ExecuteNonQuery();
            db.Close();
            return i;           
         }
    }
}
