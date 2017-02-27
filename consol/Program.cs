/*
 * Created by SharpDevelop.
 * User: Mansur
 * Date: 29.07.2015
 * Time: 9:01
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
//using System.Drawing;
using System.Text;
//using System.Windows.Forms;
using Borland.Data;
using Borland.Vcl;
using Borland.Data.Units;
using System.Data.SqlClient;
using System.Data.Common;
using System.Data.SqlServerCe;
using System.IO;

namespace consol
{
	class Program
	{
		public static DbConnection ibconn;
		public static SqlCeConnection sceconn;
		
		 public class LogFile
	   	 {
	        StreamWriter sw;
	 
	        public LogFile(string path)
	        {
	            sw = new StreamWriter(path);
	        }
	 
	        public void WriteLine(string str)
	        {
	            sw.WriteLine(str);
	            sw.Flush();
	        }
	    }
		
		public static string ConvertEncoding(string value, Encoding src, Encoding trg)
		{
			Decoder dec = src.GetDecoder();
			byte[] ba = trg.GetBytes(value);
			int len = dec.GetCharCount(ba, 0, ba.Length);
			char[] ca = new char[len];
			dec.GetChars(ba, 0, ba.Length, ca, 0);
			return new string(ca);
		}
		
		public static void DeleteObjects()
		{
			string tablename = "PROBJECT_DELETED";
			var lf = new LogFile(tablename+".log");
            string sql = "select cpaso_id from "+tablename;
            string cpaso_id, oradr_adress_id;
            
           	DbCommand ibcmd = ibconn.CreateCommand();
			DbCommand scecmd = sceconn.CreateCommand();
            ibcmd.CommandText = sql;
            ibconn.Open();
			sceconn.Open();
            DbDataReader rdr = ibcmd.ExecuteReader();
			//scecmd.CommandText = sql;
           
			while (rdr.Read ()) {
				cpaso_id = rdr.GetValue(0).ToString();
				sql = "SELECT cpaso_id, oradr_adress_id FROM PRobject WHERE cpaso_id="+cpaso_id;
				lf.WriteLine(sql);
				scecmd.CommandText = sql;
				DbDataReader scepr = scecmd.ExecuteReader();
				if(scepr.Read()){
					cpaso_id = scepr.GetValue(0).ToString();
					oradr_adress_id = scepr.GetValue(1).ToString();
					if(cpaso_id.Length>0){
						sql = "DELETE FROM PRobject WHERE cpaso_id='"+cpaso_id+"'";
						lf.WriteLine(sql);
						scecmd.CommandText = sql; 
						scecmd.ExecuteNonQuery();
						sql = "DELETE FROM OR_ADRESS WHERE oradr_adress_id='"+oradr_adress_id+"'";
						lf.WriteLine(sql);
						scecmd.CommandText = sql; 
						scecmd.ExecuteNonQuery();
					}
				}
			}
			Console.WriteLine(tablename+" TAMAM");
            rdr.Close();
            ibconn.Close();
			sceconn.Close();
		}
		
		public static void DeleteFrom(string table){
			sceconn.Open();
			DbCommand scecmd = sceconn.CreateCommand();
			var lf = new LogFile("delete from "+table.Replace("<","_").Replace(">","_")+".log");
			string sql = "DELETE FROM "+table;
			lf.WriteLine(sql);
			scecmd.CommandText = sql; 
			scecmd.ExecuteNonQuery();
			sceconn.Close();
			Console.WriteLine(sql+" TAMAM");
		}
		 
		public static void Main(string[] args)
		{
			Console.WriteLine("Выгружаем данные");
			
			ibconn = getConnection();
			sceconn = new SqlCeConnection();
			sceconn.ConnectionString = "Data Source = E://Mansur//C#//Database_1.sdf";
			DeleteFrom("probject");
			DeleteFrom("or_adress where oradr_adress_id<>2910");
			string fields;
			fields = "ORADR_ADRESS_ID, ORSTR_ID, ORADT_ID, ORART_ID, ORADR_POST_INDEX, ORADR_HOME, ORADR_NOTE_ADR, AF_ADDOBJ_ID, ADR_FULL_ADRESS, AF_ADDOBJ_GUID";
			//= "ORADR_ADRESS_ID, ORSTR_ID, ORADT_ID, ORART_ID, ORADR_POST_INDEX, ORADR_HOME, ORADR_NOTE_ADR, ADR_FULL_ADRESS, AF_ADDOBJ_GUID";
			ReadData("or_adress",fields,"ORADR_ADRESS_ID","1=1");
			fields = "CPASO_ID, CPPRT_ID, CPASO_NAME, CPASO_INVENTORY_NUMBER, CPSIT_ID, CPASO_SQUARE, ORMSU_ID_SQUARE, CPINF_ID, ORADR_ADRESS_ID, CPASO_START_DATE, CPCST_AMOUNT_1, ORMSU_ID_AMOUNT_1, CPCST_DATE_AMOUNT_1, CPCST_AMOUNT_2, ORMSU_ID_AMOUNT_2, CPCST_DATE_AMOUNT_2, CPOEF_VALUE_IZ, CPOEF_VALUE_PR, ORMSU_ID_SQUARE_P, ORMSU_ID_SQUARE_V, CPINF_SV_ID, CPOBS_ID, CPASC_SERIES, CPASC_CODE, CPASC_DATE, CPINF_PASS_ID, CPINF_CAD_ID, CPASO_MARK, CPINF_RT_ID, CPOBS_RT_ID, CPASC_RT_SERIES, CPASC_RT_CODE, CPASC_RT_DATE, CPASO_CADASTRE_CODE, ORMSU_ID_AM_CADASTRE, ORMSU_ID_SQUARE_BUILDING, CPASO_PASS_NUM, CPASO_PASS_DATE, CPASO_PASS_SUB, IE_GUID, CPLCA_ID, CPCST_AMOUNT_CADASTRE, CPASO_KOLVO";
			ReadData("PROBJECT",fields,"CPASO_ID","cpprt_id<>18");
			ReadData("PROBJECT",fields,"CPASO_ID","cpprt_id=18 and cpasc_date<='31.12.2016'");
			Set_af_addobj_id();
			//DeleteObjects();
			Console.Write("Press any key to continue . . . ");
			Console.ReadKey(true);
		}
		
		public static string res(string str){
			if(str=="") return "NULL";
			else return "'"+str+"'";
		}
		
		public static DbConnection getConnection()
        {
            DbConnection c = new TAdoDbxConnection();
			c.ConnectionString = "DriverName=Interbase;Database=192.168.1.250:C:\\Mansur\\zemotdel.gdb;RoleName=RoleName;User_Name=sysdba;Password=masterkey;lc_ctype=UTF-8;SQLDialect=3;MetaDataAssemblyLoader=Borland.Data.TDBXInterbaseMetaDataCommandFactory,Borland.Data.DbxReadOnlyMetaData,Version=11.0.5000.0,Culture=neutral,PublicKeyToken=91d62ebb5b0d1b1b;GetDriverFunc=getSQLDriverINTERBASE;LibraryName=dbxint30.dll;VendorLib=GDS32.DLL";
            return c;
        }
		
		public static string Prepare(DbDataReader rdr,int f)
		{
			string element = rdr.GetValue (f).ToString ();
			if(rdr.GetName(f)=="CPCST_AMOUNT_1" || rdr.GetName(f)=="CPCST_AMOUNT_2" || rdr.GetName(f)=="CPOEF_VALUE_IZ"
			   || rdr.GetName(f)=="CPCST_AMOUNT_CADASTRE" || rdr.GetName(f)=="CPOEF_VALUE_PR" || rdr.GetName(f)=="CPASO_SQUARE"
			  )
				element = element.Replace(",",".");
			if((rdr.GetName(f)=="CPCST_DATE_AMOUNT_1" || rdr.GetName(f)=="CPCST_DATE_AMOUNT_2" || 
			    rdr.GetName(f)=="CPASC_DATE" || rdr.GetName(f)=="CPASC_RT_DATE" || rdr.GetName(f)=="CPASO_PASS_DATE")
			    && element.Length>6
			  )
				//element = element.Replace(",",".");
				//Console.WriteLine(rdr.GetName (f)+"="+element);
				element = element.Substring(3,3)+element.Substring(0,3)+element.Substring(6,element.Length-6);
			return element;
		}

		public static void ReadData(string tablename, string felden, string id, string qayda)
        {
			var lf = new LogFile(tablename+"_"+qayda.Replace('<','_').Replace('>','_')+".log");
            string sql = "select "+ felden+ " from "+tablename+" WHERE "+qayda;
            DbCommand ibcmd = ibconn.CreateCommand();
			DbCommand scecmd = sceconn.CreateCommand();
            ibcmd.CommandText = sql;
			//scecmd.CommandText = sql;
            ibconn.Open();
			sceconn.Open();
			DbDataReader rdr = ibcmd.ExecuteReader();
			//DbDataReader scerdr;
			int sceid;
			//MessageBox.Show(myreader.FieldCount.ToString());

			int f;
			int fc = rdr.FieldCount;
			string update;
			string element;
			while (rdr.Read ()) {
				string id_val =rdr.GetValue (rdr.GetOrdinal (id)).ToString ();
				scecmd.CommandText = "SELECT count(*) FROM "+tablename+ " WHERE " +id + "=" +id_val;
				sceid = (int)scecmd.ExecuteScalar ();
				update = "";
				if (sceid > 0) {
					update = "UPDATE " + tablename + " SET ";
					for (f = 0; f < fc; f++) {
						if (f > 0){
							update += ",";
						}
						//Console.WriteLine(rdr.GetName(f) +" = "+rdr.col);
						element = Prepare(rdr,f);
						update += rdr.GetName (f) + "=" + res(element);
					}
					update += " WHERE " + id + "=" + id_val;
				} else {
					update = "INSERT INTO "+ tablename+"(" + felden + ") VALUES(";
					for (f = 0; f < fc; f++) {
						if (f > 0)
							update += ",";
						element = Prepare(rdr,f);
						update += res(element);
					}
					update += ")";
				}
				
				sql = ConvertEncoding(update,Encoding.UTF8,Encoding.GetEncoding(1251));//Encoding.ASCII.GetBytes(update).ToString();
				lf.WriteLine(sql);
				scecmd.CommandText = sql;
				try{
					scecmd.ExecuteNonQuery();
				}
				catch(Exception e){
					Console.WriteLine("Error ON " +sql);
				}
			}
			Console.WriteLine(tablename+" TAMAM");
            rdr.Close();
            ibconn.Close();
			sceconn.Close();
        }
		
		public static void Set_af_addobj_id()
		{
			DbCommand scecmd = sceconn.CreateCommand();
			var lf = new LogFile("af_addobj.log");
			scecmd.CommandText = "select distinct af_addobj_guid "+
				"from or_adress where af_addobj_id is null and af_addobj_guid is not null";
				
			sceconn.Open();
			DbDataReader rdr = scecmd.ExecuteReader();
			int af_addobj_id;
			string af_addobj_guid;
			while(rdr.Read())
			{
				af_addobj_guid = rdr.GetValue(0).ToString();
				scecmd.CommandText = "SELECT id from af_addobj WHERE ie_guid='"+af_addobj_guid+"'";
				lf.WriteLine(scecmd.CommandText);
				af_addobj_id = (int) scecmd.ExecuteScalar();
				scecmd.CommandText = "UPDATE or_adress SET af_addobj_id="+af_addobj_id.ToString()+
					" WHERE af_addobj_guid='"+af_addobj_guid+"'";
				lf.WriteLine(scecmd.CommandText);
				scecmd.ExecuteNonQuery();
			}
			rdr.Close();
			sceconn.Close();
		}
	}
}