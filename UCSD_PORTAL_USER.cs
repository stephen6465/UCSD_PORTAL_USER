using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Serialization;
using System.Net;
using System.Web;
using System.Collections;
using System.IO;
using System.Text;
using System.Diagnostics;
using System.Data.SqlClient;
using System.Data.OleDb;

namespace UCSD_PORTAL_USER
{


    public partial class UCSD_PORTAL_USER : ServiceBase
    {

        private System.Timers.Timer timer;
        
        public UCSD_PORTAL_USER()
        {
            this.ServiceName = "UCSD_PORTAL_USER";
            this.AutoLog = false;

            InitializeComponent();

            eventLog1 = new System.Diagnostics.EventLog();
            if (!System.Diagnostics.EventLog.SourceExists("UCSD_USER_PORTAL"))
            {
                System.Diagnostics.EventLog.CreateEventSource(
                    "UCSD_USER_PORTAL", "UCSD_USER_PORTAL_LOG");
            }
            eventLog1.Source = "UCSD_USER_PORTAL";
            eventLog1.Log = "UCSD_USER_PORTAL_LOG";

        }
        internal static DataSet GetDataSet(String connectionString, String query)
        {
            try
            {
                using (SqlConnection conn = new SqlConnection(connectionString))
                {
                    try
                    {
                        SqlCommand cmd = conn.CreateCommand();
                        cmd.CommandText = query;
                        SqlDataAdapter da = new SqlDataAdapter(cmd);
                        DataSet ds = new DataSet();
                        da.Fill(ds);
                        return ds;
                    }
                    catch (Exception ex)
                    {
                        DataSet ds = new DataSet();
                        DataTable dt = new DataTable("errtable");
                        dt.Columns.Add("ErrorMessage", typeof(String));
                        DataRow dr = dt.NewRow();
                        dr["ErrorMessage"] = ex.ToString();
                        dt.Rows.Add(dr);
                        ds.Tables.Add(dt);
                        return ds;
                    }
                }
            }
            catch (Exception ex)
            {
                DataSet ds = new DataSet();
                DataTable dt = new DataTable("errtable");
                dt.Columns.Add("ErrorMessage", typeof(String));
                DataRow dr = dt.NewRow();
                dr["ErrorMessage"] = ex.ToString();
                dt.Rows.Add(dr);
                ds.Tables.Add(dt);
                return ds;
            }
        }
        public TimerPluginExecResult Execute()
        {
            TimerPluginExecResult tper = new TimerPluginExecResult();
            tper.Result = TimerPluginExecResult.ExecResultCode.Failure;

            if (CheckValue(settings.MVDataPath, "No MagView Data Path", tper)) return tper;
            if (CheckValue(settings.DBConnectionString, "No Connection String", tper)) return tper;
            if (CheckValue(settings.QueryString, "No Query String", tper)) return tper;
            if (CheckValue(settings.SSNField, "No SSN Field", tper)) return tper;
            if (CheckValue(settings.EmailField, "No Email Field", tper)) return tper;

            try
            {
                using (SqlConnection conn = new SqlConnection(settings.DBConnectionString))
                {
                    using (OleDbConnection mvdata = new OleDbConnection("Provider=VFPOLEDB.1;Data Source=" + settings.MVDataPath + ";Mode=ReadWrite|Share Deny None;Extended Properties=\"\";User ID=\"\";Mask Password=False;Cache Authentication=False;Encrypt Password=False;Collating Sequence=MACHINE;DSN="))
                    {
                        SqlDataReader dr = null;
                        mvdata.Open();
                        conn.Open();

                        OleDbCommand cmdInit = mvdata.CreateCommand();
                        cmdInit.CommandText = "set null off";
                        cmdInit.ExecuteNonQuery();

                        try
                        {
                            SqlCommand cmd = conn.CreateCommand();
                            cmd.CommandText = settings.QueryString;
                            dr = cmd.ExecuteReader();
                        }
                        catch (Exception ex)
                        {
                            tper.Result = TimerPluginExecResult.ExecResultCode.Failure;
                            tper.Message = "Failed during query execution:\r\n" + ex.ToString();
                            return tper;
                        }
                        while (dr.Read())
                        {
                            String ssn = "", email = "", active = "";
                            bool bactive = true;

                            ssn = TryGetField(settings.SSNField, dr);
                            email = TryGetField(settings.EmailField, dr);
                            active = TryGetField(settings.ActiveField, dr);
                            bactive = String.IsNullOrEmpty(active) || !(active.StartsWith("0") ||
                                active.StartsWith("N") || active.StartsWith("F"));
                            bactive &= !String.IsNullOrEmpty(ssn) && !String.IsNullOrEmpty(email);
                            if (bactive)
                            {
                                OleDbCommand ocmd = mvdata.CreateCommand();
                                ocmd.CommandText = "select puser.active, puser.id, puser.account from puserx join puser on puser.id = puserx.id "
                                    + " where puserx.fk_type='P' and puserx.fk=?";
                                ocmd.Parameters.Add("?fk", OleDbType.Char).Value = ssn;
                                bool rec_exists = false, rec_active = false, acct_diff = false, pat_exists = false, patemail_diff = false;
                                String exist_id = "";

                                OleDbDataReader odr = ocmd.ExecuteReader();
                                if (odr.Read())
                                {
                                    rec_exists = true;
                                    rec_active = Convert.ToBoolean(odr["active"].ToString());
                                    exist_id = odr["id"].ToString().Trim();
                                    String exist_acct = odr["account"].ToString().ToUpper().Trim();
                                    acct_diff = !exist_acct.Equals(ssn.ToUpper().Trim());
                                }
                                odr.Close();

                                ocmd = mvdata.CreateCommand();
                                ocmd.CommandText = "select patients.email_addr from patients where ssn=?";
                                ocmd.Parameters.Add("?ssn", OleDbType.Char).Value = ssn;
                                odr = ocmd.ExecuteReader();
                                if (odr.Read())
                                {
                                    pat_exists = true;
                                    String pat_email = odr["email_addr"].ToString().ToUpper().Trim();
                                    patemail_diff = !pat_email.Equals(email.ToUpper().Trim());
                                }
                                odr.Close();
                                if (pat_exists)
                                {
                                    if (!rec_exists)
                                    {
                                        ocmd = mvdata.CreateCommand();
                                        ocmd.CommandText = "insert into puser (id,account,acct_type,active) "
                                            + "values(?,?,'P',.t.)";
                                        String id = GetID(mvdata);
                                        ocmd.Parameters.Add("?id", OleDbType.Char).Value = id;
                                        ocmd.Parameters.Add("?account", OleDbType.Char).Value = ssn;
                                        ocmd.ExecuteNonQuery();
                                        ocmd = mvdata.CreateCommand();
                                        ocmd.CommandText = "insert into puserx (id,fk,fk_type) values(?,?,'P')";
                                        ocmd.Parameters.Add("?id", OleDbType.Char).Value = id;
                                        ocmd.Parameters.Add("?fk", OleDbType.Char).Value = ssn;
                                        ocmd.ExecuteNonQuery();
                                    }
                                    if (rec_exists && (!rec_active || acct_diff))
                                    {
                                        ocmd = mvdata.CreateCommand();
                                        ocmd.CommandText = "update puser set active=.t., account=? where id=?";
                                        ocmd.Parameters.Add("?account", OleDbType.Char).Value = ssn;
                                        ocmd.Parameters.Add("?id", OleDbType.Char).Value = exist_id;
                                        ocmd.ExecuteNonQuery();
                                    }
                                    if (patemail_diff)
                                    {
                                        ocmd = mvdata.CreateCommand();
                                        ocmd.CommandText = "update patients set email_addr=? where ssn=?";
                                        ocmd.Parameters.Add("?email_addr", OleDbType.Char).Value = email;
                                        ocmd.Parameters.Add("?ssn", OleDbType.Char).Value = ssn;
                                        ocmd.ExecuteNonQuery();
                                    }
                                }
                            }
                        }
                        tper.Result = TimerPluginExecResult.ExecResultCode.Success;
                    }
                }
            }
            catch (Exception ex)
            {
                tper.Result = TimerPluginExecResult.ExecResultCode.Failure;
                tper.Message = "Unknown failure:\r\n" + ex.ToString();
            }
            return tper;
        }

        private String GetID(OleDbConnection mvdata)
        {
            bool exists = true;
            String id = "";

            while (exists)
            {
                id = RndLongStr(9999999999);
                OleDbCommand ocmd = mvdata.CreateCommand();
                ocmd.CommandText = "select id from puser where id=?";
                ocmd.Parameters.Add("?id", OleDbType.Char).Value = id;
                OleDbDataReader odr = ocmd.ExecuteReader();
                exists = false;
                if (odr.Read()) exists = true;
                odr.Close();
            }
            return id;
        }

        private String RndLongStr(long max)
        {
            Random r = new Random();
            int i1 = r.Next(), i2 = r.Next();
            long l = (i1 << 32) | i2;
            while (l > max)
            {
                i1 = r.Next(); i2 = r.Next();
                l = (i1 << 32) | i2;
            }
            return l.ToString();
        }
        private String TryGetField(String field, SqlDataReader dr)
        {
            if (String.IsNullOrEmpty(field)) return "";
            try
            {
                String s = dr[field].ToString();
                return s.Trim();
            }
            catch (Exception) { return ""; }
        }
        protected override void OnStart(string[] args)
        {
            eventLog1.WriteEntry("IN OnStartup");

            UCSDSettings t = new UCSDSettings();
            eventLog1.WriteEntry("Created UCSD Setting object");
            if (!File.Exists("C:\\MVPORTAL_plugin\\MVPortal_PlugIn_config.XML"))
            {
                t.Serialize("C:\\MVPORTAL_plugin\\MVPortal_PlugIn_config.XML", t);
            }
            UCSDSettings t2 = t.Deserialize("C:\\MVPORTAL_plugin\\MVPortal_PlugIn_config.XML");
            eventLog1.WriteEntry("Created UCSD Setting object - from file");

            this.timer = new System.Timers.Timer(30000D);  // 30000 milliseconds = 30 seconds
            this.timer.AutoReset = true;
            this.timer.Elapsed += new System.Timers.ElapsedEventHandler(this.timer_Elapsed);
            this.timer.Start();

        }
        private void timer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
           // check if the current time is right then do work here so 1 compare time and then do work!
            
            
            
            // MyServiceApp.ServiceWork.Main(); // my separate static method for do work
        }

        protected override void OnStop()
        {

            this.timer.Stop();
            eventLog1.WriteEntry("Shutting Timer down");
            this.timer = null;
            eventLog1.WriteEntry("Shutting down now... Bye bye now!");
        
        }

        private void eventLog1_EntryWritten(object sender, EntryWrittenEventArgs e)
        {

        }




        
    }

    [Serializable()]
    [XmlRoot("UCSDSettings")]
    public class UCSDSettings
    {

        public UCSDSettings() { }

        [XmlElement("Server")]
        public String Server { get; set; }

        [XmlElement("IpAddress")]
        public String IpAddress { get; set; }

        [XmlElement("AuthURL")]
        public String AuthURL { get; set; }

        [XmlElement("UniqueId")]
        public String UniqueId { get; set; }

        [XmlElement("CommId")]
        public String CommId { get; set; }


        [XmlElement("DBName")]
        public String DBName { get; set; }

        public void Serialize(string file, UCSDSettings c)
        {
            System.Xml.Serialization.XmlSerializer xs
               = new System.Xml.Serialization.XmlSerializer(c.GetType());
            string path1 = Path.GetDirectoryName(file);
            if (!Directory.Exists(path1))
            {
                Directory.CreateDirectory(path1);
            }
            StreamWriter writer = File.CreateText(file);
            xs.Serialize(writer, c);
            writer.Flush();
            writer.Close();
        }
        public UCSDSettings Deserialize(string file)
        {
            System.Xml.Serialization.XmlSerializer xs
               = new System.Xml.Serialization.XmlSerializer(
                  typeof(UCSDSettings));
            StreamReader reader = File.OpenText(file);
            UCSDSettings c = (UCSDSettings)xs.Deserialize(reader);
            reader.Close();
            return c;
        }
    }



}
