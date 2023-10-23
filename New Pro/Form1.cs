﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace New_Pro
{
    public partial class Form1 : Form
    {


        public Form1()
        {
            InitializeComponent();
        }

        private void MyIntervalFunction(object obj)
        {
            // Source database connection string
            string sourceConnectionString;
            sourceConnectionString = @"Data Source=211.46.110.210,5398;Initial Catalog=DZICUBE;User ID=dzicube;Password=icbVL110%!2!)";
            // Destination database connection string
            string destinationConnectionString;
            destinationConnectionString = @"Data Source=101.250.201.10;Initial Catalog=simbiz;User ID=sa;Password=tlaqlwm2174";


            SqlConnection sourceConnection = null;
            SqlConnection destinationConnection = null;



            try
            {
                // Establish a connection to the source database
                sourceConnection = new SqlConnection(sourceConnectionString);
                sourceConnection.Open();

                #region Get data from THE ZONE DB

                // Select data from the source table
                string selectQuery = "select co_cd, in_dt, in_sq, ";
                selectQuery += "isu_dt , isu_sq ";
                selectQuery += "from AUTODOCU_SIMPLE ";
                selectQuery += "where isu_dt + convert(nvarchar(10), isu_sq) <> '000000000' ";
                selectQuery += "And logic_cd in ('2','3') ";

                SqlCommand selectCommand = new SqlCommand(selectQuery, sourceConnection);
                SqlDataAdapter adapter = new SqlDataAdapter(selectCommand);
                DataTable sourceData = new DataTable();
                adapter.Fill(sourceData);
                #endregion


                // Establish a connection to the destination database
                destinationConnection = new SqlConnection(destinationConnectionString);
                destinationConnection.Open();

                // Loop through the retrieved data and update the destination table
                foreach (DataRow row in sourceData.Rows)
                {
                    #region Update if_status = 'A' in IF_AUTODOCU_SIMPLE of Innotech DB

                    // Check  exists in the IF_AUTODOCU_SIMPLE table
                    string checkExistQuery = "SELECT COUNT(1) FROM IF_AUTODOCU_SIMPLE WHERE co_cd = @Value1 ";
                    checkExistQuery += "AND  in_dt = @Value2 ";
                    checkExistQuery += "AND  in_sq = @Value3 ";
                    checkExistQuery += "AND if_status <> 'A' ";
                    SqlCommand checkExistCommand = new SqlCommand(checkExistQuery, destinationConnection);
                    checkExistCommand.Parameters.AddWithValue("@Value1", row["co_cd"]);
                    checkExistCommand.Parameters.AddWithValue("@Value2", row["in_dt"]);
                    checkExistCommand.Parameters.AddWithValue("@Value3", row["in_sq"]);
                    int existingRecordCount = (int)checkExistCommand.ExecuteScalar();

                    if (existingRecordCount > 0)
                    {
                        // Update data in the IF_AUTODOCU_SIMPLE table
                        string updateQuery = "UPDATE IF_AUTODOCU_SIMPLE SET if_status = 'A' WHERE co_cd = @Value1 ";
                        updateQuery += "AND  in_dt = @Value2 ";
                        updateQuery += "AND  in_sq = @Value3 ";

                        SqlCommand updateCommand = new SqlCommand(updateQuery, destinationConnection);
                        updateCommand.Parameters.AddWithValue("@Value1", row["co_cd"]);
                        updateCommand.Parameters.AddWithValue("@Value2", row["in_dt"]);
                        updateCommand.Parameters.AddWithValue("@Value3", row["in_sq"]);

                        updateCommand.ExecuteNonQuery();
                        #endregion


                        //updated progress_status in tb_pur_handle_stock_in_detail in the destination database
                        #region Update progress_status ='completed' in tb_pur_handle_stock_in_detail

                        // Check  exists in the tb_sl_sale_detai table
                        string checkExistQuery1 = "SELECT COUNT(1) FROM IF_AUTODOCU_SIMPLE WHERE logic_cd = '2' ";
                        checkExistQuery1 += "AND in_dt = @Value2 ";
                        checkExistQuery1 += "AND  in_sq = @Value3 ";
                        checkExistQuery1 += "AND if_status = 'A' ";
                        SqlCommand checkExistCommand1 = new SqlCommand(checkExistQuery1, destinationConnection);
                        checkExistCommand1.Parameters.AddWithValue("@Value2", row["in_dt"]);
                        checkExistCommand1.Parameters.AddWithValue("@Value3", row["in_sq"]);
                        int existingRecordCount1 = (int)checkExistCommand1.ExecuteScalar();

                        // Check  exists in the destination table
                        string checkExistQuery2 = "SELECT COUNT(1) FROM tb_pur_handle_stock_in_detail WHERE progress_status = 'interface' ";
                        checkExistQuery2 += "AND in_dt = @Value2 ";
                        checkExistQuery2 += "AND  in_sq = @Value3 ";
   
                        SqlCommand checkExistCommand2 = new SqlCommand(checkExistQuery2, destinationConnection);
                        checkExistCommand2.Parameters.AddWithValue("@Value2", row["in_dt"]);
                        checkExistCommand2.Parameters.AddWithValue("@Value3", row["in_sq"]);
                        int existingRecordCount2 = (int)checkExistCommand2.ExecuteScalar();

                        if (existingRecordCount1 > 0&& existingRecordCount2 > 0)
                        {
                            string insertQuery = "UPDATE tb_pur_handle_stock_in_detail  SET progress_status = 'completed'  ";
                            insertQuery += "WHERE in_dt = @Value2 ";
                            insertQuery += "AND  in_sq = @Value3 ";
                            using (SqlCommand insertCommand = new SqlCommand(insertQuery, destinationConnection))
                            {
                                insertCommand.Parameters.AddWithValue("@Value2", row["in_dt"]);
                                insertCommand.Parameters.AddWithValue("@Value3", row["in_sq"]);
                                insertCommand.ExecuteNonQuery();
                            }
                        }
             
                        #endregion

                        #region Update progress_status = completed in tb_sl_sale_detail

                        // Check  exists in the tb_sl_sale_detai table
                        string checkExistQuery3 = "SELECT COUNT(1) FROM IF_AUTODOCU_SIMPLE WHERE isu_doc LIKE '국내매출%' AND logic_cd = '3' ";
                        checkExistQuery3 += "AND in_dt = @Value2 ";
                        checkExistQuery3 += "AND  in_sq = @Value3 ";
                        checkExistQuery3 += "AND if_status = 'A' ";
                        SqlCommand checkExistCommand3 = new SqlCommand(checkExistQuery3, destinationConnection);
                        checkExistCommand3.Parameters.AddWithValue("@Value2", row["in_dt"]);
                        checkExistCommand3.Parameters.AddWithValue("@Value3", row["in_sq"]);
                        int existingRecordCount3 = (int)checkExistCommand3.ExecuteScalar();

                        string checkExistQuery4 = "SELECT COUNT(1) FROM tb_sl_sale_detail WHERE progress_status = 'interface' ";
                        checkExistQuery4 += "AND in_dt = @Value2 ";
                        checkExistQuery4 += "AND  in_sq = @Value3 ";

                        SqlCommand checkExistCommand4 = new SqlCommand(checkExistQuery4, destinationConnection);
                        checkExistCommand4.Parameters.AddWithValue("@Value2", row["in_dt"]);
                        checkExistCommand4.Parameters.AddWithValue("@Value3", row["in_sq"]);
                        int existingRecordCount4 = (int)checkExistCommand4.ExecuteScalar();


                        if (existingRecordCount3 > 0 && existingRecordCount4 > 0)
                        {

                            string insertQuery = "UPDATE tb_sl_sale_detail  SET progress_status = 'completed'  ";
                            insertQuery += "WHERE in_dt = @Value2 ";
                            insertQuery += "AND  in_sq = @Value3 ";
                            using (SqlCommand insertCommand = new SqlCommand(insertQuery, destinationConnection))
                            {
                                insertCommand.Parameters.AddWithValue("@Value2", row["in_dt"]);
                                insertCommand.Parameters.AddWithValue("@Value3", row["in_sq"]);
                                insertCommand.ExecuteNonQuery();
                            }
                        }
             
                        #endregion

                        #region Update progress_status = completed in tb_sl_bl_export_detail

                        // Check  exists in the tb_sl_sale_detai table
                        string checkExistQuery5 = "SELECT COUNT(1) FROM IF_AUTODOCU_SIMPLE WHERE isu_doc LIKE '수출매출%' AND logic_cd = '3' ";
                        checkExistQuery5 += "AND in_dt = @Value2 ";
                        checkExistQuery5 += "AND  in_sq = @Value3 ";
                        checkExistQuery5 += "AND if_status = 'A' ";
                        SqlCommand checkExistCommand5 = new SqlCommand(checkExistQuery5, destinationConnection);
                        checkExistCommand5.Parameters.AddWithValue("@Value2", row["in_dt"]);
                        checkExistCommand5.Parameters.AddWithValue("@Value3", row["in_sq"]);
                        int existingRecordCount5 = (int)checkExistCommand5.ExecuteScalar();

                        string checkExistQuery6 = "SELECT COUNT(1) FROM tb_sl_bl_export_detail WHERE progress_status = 'interface' ";
                        checkExistQuery6 += "AND in_dt = @Value2 ";
                        checkExistQuery6 += "AND  in_sq = @Value3 ";

                        SqlCommand checkExistCommand6 = new SqlCommand(checkExistQuery6, destinationConnection);
                        checkExistCommand6.Parameters.AddWithValue("@Value2", row["in_dt"]);
                        checkExistCommand6.Parameters.AddWithValue("@Value3", row["in_sq"]);
                        int existingRecordCount6 = (int)checkExistCommand6.ExecuteScalar();

                        if (existingRecordCount5 > 0 && existingRecordCount6 > 0)
                        {

                            string insertQuery = "UPDATE tb_sl_bl_export_detail SET progress_status = 'completed'  ";
                            insertQuery += "WHERE in_dt = @Value2 ";
                            insertQuery += "AND  in_sq = @Value3 ";
                            using (SqlCommand insertCommand = new SqlCommand(insertQuery, destinationConnection))
                            {
                                insertCommand.Parameters.AddWithValue("@Value2", row["in_dt"]);
                                insertCommand.Parameters.AddWithValue("@Value3", row["in_sq"]);
                                insertCommand.ExecuteNonQuery();
                            }
                        }
          
                        #endregion


                    }
                    else
                    {
                        continue;
                    }

                }


                MessageBox.Show("Data update and insert completed.");

         
            }
            catch (Exception ex)
            {

                MessageBox.Show("An error occurred: " + ex.Message);
            }
            finally
            {
                // Close connections explicitly in the finally block
                sourceConnection.Close();
                destinationConnection.Close();

            }


        }

        private void Form1_Load_1(object sender, EventArgs e)
        {
            System.Threading.Timer timer = new System.Threading.Timer(new System.Threading.TimerCallback(MyIntervalFunction));
            timer.Change(0, 300000);
        }


    }
}