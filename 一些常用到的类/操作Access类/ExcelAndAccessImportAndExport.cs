//从excel导入到数据库。

            OleDbConnection conExcel = new OleDbConnection();
            try
            {
                OpenFileDialog openFile = new OpenFileDialog();//打开文件对话框。
                openFile.Filter = ("Excel 文件(*.xls)|*.xls");//后缀名。
                if (openFile.ShowDialog() == DialogResult.OK)
                {
                    string filename = openFile.FileName;
                    int index = filename.LastIndexOf("\\");//截取文件的名字
                    filename = filename.Substring(index + 1);
                    conExcel.ConnectionString = "Provider=Microsoft.Jet.Oledb.4.0;Data Source=" + Application.StartupPath + "\\Appdata.mdb";
　                  //将excel导入access
                    //distinct :删除excel重复的行.
                    //[excel名].[sheet名] 已有的excel的表要加$
                    //where not in : 插入不重复的记录。
                    string sql = "insert into 用户表 select distinct * from [Excel 8.0;database=" + filename + "].[用户表$] where 记录编号   not IN (select 记录编号 from 用户表)";
                    OleDbCommand com = new OleDbCommand(sql, conExcel);
                    conExcel.Open();
                    com.ExecuteNonQuery();
                    MessageBox.Show("导入数据成功","导入数据", MessageBoxButtons.OK, MessageBoxIcon.Information );
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.ToString());
            }
            finally
            {
                conExcel.Close();
            }

//导出到excel操作。
			OleDbConnection conExcel = new OleDbConnection();
            try
            {
                SaveFileDialog saveFile = new SaveFileDialog();
                saveFile.Filter = ("Excel 文件(*.xls)|*.xls");//指定文件后缀名为Excel 文件。
                if (saveFile.ShowDialog() == DialogResult.OK)
                {
                     string filename = saveFile.FileName;
                     if (System.IO.File.Exists(filename))
                     {
                         System.IO.File.Delete(filename);//如果文件存在删除文件。
                      }
                    int index = filename.LastIndexOf("\\");//获取最后一个\的索引
                    filename = filename.Substring(index + 1);//获取excel名称(新建表的路径相对于SaveFileDialog的路径)
                    //select * into 建立 新的表。
                    //[[Excel 8.0;database= excel名].[sheet名] 如果是新建sheet表不能加$,如果向sheet里插入数据要加$.　
                    //sheet最多存储65535条数据。
                    string sql = "select top 65535 * into   [Excel 8.0;database=" + filename + "].[用户表] from 用户表";
　                  conExcel.ConnectionString = "Provider=Microsoft.Jet.Oledb.4.0;Data Source=" + Application.StartupPath + "\\Appdata.mdb";//将数据库放到debug目录下。
                    OleDbCommand com = new OleDbCommand(sql, conExcel);
                    conExcel.Open();
                    com.ExecuteNonQuery();
                    MessageBox.Show("导出数据成功","导出数据", MessageBoxButtons.OK, MessageBoxIcon.Information );
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.ToString());
            }
            finally 
            {
                conExcel.Close();
            }

 
