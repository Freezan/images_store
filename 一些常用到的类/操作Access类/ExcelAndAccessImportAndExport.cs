//��excel���뵽���ݿ⡣

            OleDbConnection conExcel = new OleDbConnection();
            try
            {
                OpenFileDialog openFile = new OpenFileDialog();//���ļ��Ի���
                openFile.Filter = ("Excel �ļ�(*.xls)|*.xls");//��׺����
                if (openFile.ShowDialog() == DialogResult.OK)
                {
                    string filename = openFile.FileName;
                    int index = filename.LastIndexOf("\\");//��ȡ�ļ�������
                    filename = filename.Substring(index + 1);
                    conExcel.ConnectionString = "Provider=Microsoft.Jet.Oledb.4.0;Data Source=" + Application.StartupPath + "\\Appdata.mdb";
��                  //��excel����access
                    //distinct :ɾ��excel�ظ�����.
                    //[excel��].[sheet��] ���е�excel�ı�Ҫ��$
                    //where not in : ���벻�ظ��ļ�¼��
                    string sql = "insert into �û��� select distinct * from [Excel 8.0;database=" + filename + "].[�û���$] where ��¼���   not IN (select ��¼��� from �û���)";
                    OleDbCommand com = new OleDbCommand(sql, conExcel);
                    conExcel.Open();
                    com.ExecuteNonQuery();
                    MessageBox.Show("�������ݳɹ�","��������", MessageBoxButtons.OK, MessageBoxIcon.Information );
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

//������excel������
			OleDbConnection conExcel = new OleDbConnection();
            try
            {
                SaveFileDialog saveFile = new SaveFileDialog();
                saveFile.Filter = ("Excel �ļ�(*.xls)|*.xls");//ָ���ļ���׺��ΪExcel �ļ���
                if (saveFile.ShowDialog() == DialogResult.OK)
                {
                     string filename = saveFile.FileName;
                     if (System.IO.File.Exists(filename))
                     {
                         System.IO.File.Delete(filename);//����ļ�����ɾ���ļ���
                      }
                    int index = filename.LastIndexOf("\\");//��ȡ���һ��\������
                    filename = filename.Substring(index + 1);//��ȡexcel����(�½����·�������SaveFileDialog��·��)
                    //select * into ���� �µı�
                    //[[Excel 8.0;database= excel��].[sheet��] ������½�sheet���ܼ�$,�����sheet���������Ҫ��$.��
                    //sheet���洢65535�����ݡ�
                    string sql = "select top 65535 * into   [Excel 8.0;database=" + filename + "].[�û���] from �û���";
��                  conExcel.ConnectionString = "Provider=Microsoft.Jet.Oledb.4.0;Data Source=" + Application.StartupPath + "\\Appdata.mdb";//�����ݿ�ŵ�debugĿ¼�¡�
                    OleDbCommand com = new OleDbCommand(sql, conExcel);
                    conExcel.Open();
                    com.ExecuteNonQuery();
                    MessageBox.Show("�������ݳɹ�","��������", MessageBoxButtons.OK, MessageBoxIcon.Information );
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

 
