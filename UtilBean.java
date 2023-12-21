package com.etrm.fms.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Vector;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

//Coded By          : Harsh Patel
//Code Reviewed by	:  
//CR Date			: 22/09/2022 
//Status	  		: Developing
public class UtilBean 
{
	Connection conn;
	Statement stmtement,stmtement1,stmtement2,stmtement3;
	ResultSet resultset,resultset1,resultset2,resultset3;
	String query = "";
	String query1="";
	
	NumberFormat nf = new DecimalFormat("###########0.00");
	NumberFormat nf2 = new DecimalFormat("###########0.0000");
	
	DateUtil dateUtil = new DateUtil();
	
	private boolean init() 
	{
		boolean returnty = false;
		try
		{
			Context initContext = new InitialContext();
			if(initContext == null ) 
			{
			  throw new Exception("Boom - No Context");
			}
			Context envContext  = (Context)initContext.lookup("java:/comp/env");
			DataSource ds = (DataSource)envContext.lookup(RuntimeConf.security_database);
			if (ds != null) 
			{
				conn = ds.getConnection();  	
				if(conn != null) 
				{
					returnty = true;
				}
				else
				{
					returnty = false;
				}
			}
			else
			{
				returnty = false;
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return returnty;	
	}
	
	public void getCountryMaster()
	{
		try
		{
			if(init())
			{
				COUNTRY_CODE.clear();
				COUNTRY_NM.clear();
				ISO_CODE.clear();
				
				query = "SELECT COUNTRY_CODE,COUNTRY_NM,ISO_CODE "
						+ "FROM FMS_COUNTRY_MST "
						+ "ORDER BY COUNTRY_NM ";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				while(resultset.next())
				{
					COUNTRY_CODE.add(resultset.getString(1)==null?"":resultset.getString(1));
					COUNTRY_NM.add(resultset.getString(2)==null?"":resultset.getString(2));
					ISO_CODE.add(resultset.getString(3)==null?"":resultset.getString(3));
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void getStateMaster()
	{
		try
		{
			if(init())
			{
				TIN.clear();
				STATE_CODE.clear();
				STATE_NM.clear();
				
				query = "SELECT TIN,STATE_CODE,STATE_NM "
						+ "FROM FMS_STATE_MST "
						+ "ORDER BY STATE_NM";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				while(resultset.next())
				{
					TIN.add(resultset.getString(1)==null?"":resultset.getString(1));
					STATE_CODE.add(resultset.getString(2)==null?"":resultset.getString(2));
					STATE_NM.add(resultset.getString(3)==null?"":resultset.getString(3));
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public String getStateName(String cd)
	{
		String nm="";
		try
		{
			if(init())
			{
				query = "SELECT STATE_NM "
						+ "FROM FMS_STATE_MST "
						+ "WHERE TIN='"+cd+"' "
						+ "ORDER BY STATE_NM";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					nm= resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return nm;
	}
	
	public String getSectorName(String cd,String comp_cd)
	{
		String nm="";
		try
		{
			if(init())
			{
				query = "SELECT SECTOR_NAME "
						+ "FROM FMS_SECTOR_MST "
						+ "WHERE SECTOR_CD='"+cd+"' AND COMPANY_CD='"+comp_cd+"' ";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					nm= resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return nm;
	}
	
	public void getCounterpartyMaster(String company_cd)
	{
		try
		{
			if(init())
			{
				COUNTERPARTY_CD.clear();
				COUNTERPARTY_NM.clear();
				COUNTERPARTY_ABBR.clear();
				
				query = "SELECT COUNTERPARTY_CD,COUNTERPARTY_NM,COUNTERPARTY_ABBR "
						+ "FROM FMS_COUNTERPARTY_MST A "
						+ "WHERE COMPANY_CD='"+company_cd+"' AND EFF_DT=(SELECT MAX(EFF_DT) FROM FMS_COUNTERPARTY_MST B "
						+ "WHERE A.COUNTERPARTY_CD=B.COUNTERPARTY_CD AND A.COMPANY_CD=B.COMPANY_CD) "
						+ "ORDER BY COUNTERPARTY_NM";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				while(resultset.next())
				{
					COUNTERPARTY_CD.add(resultset.getString(1)==null?"":resultset.getString(1));
					COUNTERPARTY_NM.add(resultset.getString(2)==null?"":resultset.getString(2));
					COUNTERPARTY_ABBR.add(resultset.getString(3)==null?"":resultset.getString(3));
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void getActiveCounterpartyMaster(String company_cd)
	{
		try
		{
			if(init())
			{
				COUNTERPARTY_CD.clear();
				COUNTERPARTY_NM.clear();
				COUNTERPARTY_ABBR.clear();
				
				query = "SELECT COUNTERPARTY_CD,COUNTERPARTY_NM,COUNTERPARTY_ABBR "
						+ "FROM FMS_COUNTERPARTY_MST A "
						+ "WHERE COMPANY_CD='"+company_cd+"' AND STATUS='Y' "
						+ "AND EFF_DT=(SELECT MAX(EFF_DT) FROM FMS_COUNTERPARTY_MST B "
						+ "WHERE A.COUNTERPARTY_CD=B.COUNTERPARTY_CD AND A.COMPANY_CD=B.COMPANY_CD) "
						+ "ORDER BY COUNTERPARTY_NM";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				while(resultset.next())
				{
					COUNTERPARTY_CD.add(resultset.getString(1)==null?"":resultset.getString(1));
					COUNTERPARTY_NM.add(resultset.getString(2)==null?"":resultset.getString(2));
					COUNTERPARTY_ABBR.add(resultset.getString(3)==null?"":resultset.getString(3));
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public String getCounterpartyName(String cd, String comp_cd)
	{
		String name="";
		try
		{
			if(init())
			{
				query = "SELECT COUNTERPARTY_NM "
						+ "FROM FMS_COUNTERPARTY_MST A "
						+ "WHERE COUNTERPARTY_CD='"+cd+"' AND COMPANY_CD='"+comp_cd+"' "
						+ "AND EFF_DT=(SELECT MAX(EFF_DT) FROM FMS_COUNTERPARTY_MST B WHERE A.COUNTERPARTY_CD=B.COUNTERPARTY_CD "
						+ "AND A.COMPANY_CD=B.COMPANY_CD) ";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					name = resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return name;
	}
	
	public String getCounterpartySAPcode(String cd, String comp_cd)
	{
		String name="";
		try
		{
			if(init())
			{
				query = "SELECT SAP_CODE "
						+ "FROM FMS_COUNTERPARTY_MST A "
						+ "WHERE COUNTERPARTY_CD='"+cd+"' AND COMPANY_CD='"+comp_cd+"' "
						+ "AND EFF_DT=(SELECT MAX(EFF_DT) FROM FMS_COUNTERPARTY_MST B WHERE A.COUNTERPARTY_CD=B.COUNTERPARTY_CD "
						+ "AND A.COMPANY_CD=B.COMPANY_CD) ";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					name = resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return name;
	}
	
	public String getGasExchangeSAPcode(String cd, String comp_cd)
	{
		String name="";
		try
		{
			if(init())
			{
				query = "SELECT SAP_CODE "
						+ "FROM FMS_COMPANY_EXCHG_MST A "
						+ "WHERE COUNTERPARTY_CD='"+cd+"' AND COMPANY_CD='"+comp_cd+"' "
						+ "AND EFF_DT=(SELECT MAX(EFF_DT) FROM FMS_COMPANY_EXCHG_MST B WHERE A.COUNTERPARTY_CD=B.COUNTERPARTY_CD "
						+ "AND A.COMPANY_CD=B.COMPANY_CD) ";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					name = resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return name;
	}
	
	public String getCompanyName(String comp_cd)
	{
		String name="";
		try
		{
			if(init())
			{
				query = "SELECT COMPANY_NM "
						+ "FROM FMS_COMPANY_OWNER_MST A "
						+ "WHERE COMPANY_CD='"+comp_cd+"' "
						+ "AND EFF_DT=(SELECT MAX(EFF_DT) FROM FMS_COMPANY_OWNER_MST B WHERE A.COMPANY_CD=B.COMPANY_CD) ";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					name = resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return name;
	}
	
	public String getCompanyAbbr(String comp_cd)
	{
		String name="";
		try
		{
			if(init())
			{
				query = "SELECT COMPANY_ABBR "
						+ "FROM FMS_COMPANY_OWNER_MST A "
						+ "WHERE COMPANY_CD='"+comp_cd+"' "
						+ "AND EFF_DT=(SELECT MAX(EFF_DT) FROM FMS_COMPANY_OWNER_MST B WHERE A.COMPANY_CD=B.COMPANY_CD) ";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					name = resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return name;
	}
	
	public String getCompanySAPcode(String comp_cd)
	{
		String name="";
		try
		{
			if(init())
			{
				query = "SELECT SAP_CODE "
						+ "FROM FMS_COMPANY_OWNER_MST A "
						+ "WHERE COMPANY_CD='"+comp_cd+"' "
						+ "AND EFF_DT=(SELECT MAX(EFF_DT) FROM FMS_COMPANY_OWNER_MST B WHERE A.COMPANY_CD=B.COMPANY_CD) ";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					name = resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return name;
	}
	
	public String getGasExchangeName(String cd,String comp_cd)
	{
		String name="";
		try
		{
			if(init())
			{
				query = "SELECT COUNTERPARTY_NM "
						+ "FROM FMS_COMPANY_EXCHG_MST A "
						+ "WHERE COMPANY_CD='"+comp_cd+"' AND COUNTERPARTY_CD='"+cd+"' "
						+ "AND EFF_DT=(SELECT MAX(EFF_DT) FROM FMS_COMPANY_EXCHG_MST B "
						+ "WHERE A.COMPANY_CD=B.COMPANY_CD AND A.COUNTERPARTY_CD=B.COUNTERPARTY_CD) ";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					name = resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return name;
	}
	
	public String getGasExchangeCd(String abbr,String comp_cd)
	{
		String name="";
		try
		{
			if(init())
			{
				query = "SELECT COUNTERPARTY_CD "
						+ "FROM FMS_COMPANY_EXCHG_MST A "
						+ "WHERE COMPANY_CD='"+comp_cd+"' AND COUNTERPARTY_ABBR='"+abbr+"' "
						+ "AND EFF_DT=(SELECT MAX(EFF_DT) FROM FMS_COMPANY_EXCHG_MST B "
						+ "WHERE A.COMPANY_CD=B.COMPANY_CD AND A.COUNTERPARTY_CD=B.COUNTERPARTY_CD) ";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					name = resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return name;
	}
	
	public String getGasExchangeAbbr(String cd,String comp_cd)
	{
		String abbr="";
		try
		{
			if(init())
			{
				query = "SELECT COUNTERPARTY_ABBR "
						+ "FROM FMS_COMPANY_EXCHG_MST A "
						+ "WHERE COMPANY_CD='"+comp_cd+"' AND COUNTERPARTY_CD='"+cd+"' "
						+ "AND EFF_DT=(SELECT MAX(EFF_DT) FROM FMS_COMPANY_EXCHG_MST B "
						+ "WHERE A.COMPANY_CD=B.COMPANY_CD AND A.COUNTERPARTY_CD=B.COUNTERPARTY_CD) ";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					abbr = resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return abbr;
	}
	
	public String getCounterpartyABBR(String cd, String comp_cd)
	{
		String abbr="";
		try
		{
			if(init())
			{
				query = "SELECT COUNTERPARTY_ABBR "
						+ "FROM FMS_COUNTERPARTY_MST A "
						+ "WHERE COUNTERPARTY_CD='"+cd+"' AND COMPANY_CD='"+comp_cd+"' "
						+ "AND EFF_DT=(SELECT MAX(EFF_DT) FROM FMS_COUNTERPARTY_MST B WHERE A.COUNTERPARTY_CD=B.COUNTERPARTY_CD "
						+ "AND A.COMPANY_CD=B.COMPANY_CD) ";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					abbr = resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return abbr;
	}
	
	public String getEmpName(String cd)
	{
		String name="";
		try
		{
			if(init())
			{
				query = "SELECT EMP_NM "
						+ "FROM FMS_EMP_MST "
						+ "WHERE EMP_CD='"+cd+"' ";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					name = resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return name;
	}
	
	public String getUserName(String cd)
	{
		String name="";
		try
		{
			if(init())
			{
				query = "SELECT EMP_UID "
						+ "FROM FMS_EMP_MST "
						+ "WHERE EMP_CD='"+cd+"' ";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					name = resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return name;
	}
	
	public String getEnergyUnitNm(String cd)
	{
		String name="";
		try
		{
			if(init())
			{
				query = "SELECT ENERGY_UNIT_ABR "
						+ "FROM FMS_ENERGY_UNIT "
						+ "WHERE ENERGY_UNIT_CD='"+cd+"' ";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					name = resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return name;
	}
	
	//effective - counterparty effective date validity 
	//clearance - KYC | IGX
	//Active | Deactive check
	public void getEffectiveTraderCounterpartyList(String clearance, String comp_cd)
	{
		try
		{
			if(init())
			{
				COUNTERPARTY_CD.clear();
				COUNTERPARTY_NM.clear();
				COUNTERPARTY_ABBR.clear();
				
				query = "SELECT COUNTERPARTY_CD,COUNTERPARTY_NM,COUNTERPARTY_ABBR "
						+ "FROM FMS_COUNTERPARTY_MST A "
						+ "WHERE COMPANY_CD='"+comp_cd+"' AND EFF_DT=(SELECT MAX(B.EFF_DT) FROM FMS_COUNTERPARTY_MST B WHERE A.COUNTERPARTY_CD=B.COUNTERPARTY_CD "
						+ "AND B.EFF_DT<=TO_DATE(TO_CHAR(SYSDATE,'DD/MM/YYYY'),'DD/MM/YYYY') AND A.COMPANY_CD=B.COMPANY_CD) "
						+ "AND TRADER='Y' AND STATUS='Y' ";
				if(clearance.equals("KYC")) {
					query+="AND KYC='Y' ";
				}
				else if(clearance.equals("IGX")) {
					query+="AND IGX='Y' ";
				}
					query+= "ORDER BY COUNTERPARTY_NM";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				while(resultset.next())
				{
					COUNTERPARTY_CD.add(resultset.getString(1)==null?"":resultset.getString(1));
					COUNTERPARTY_NM.add(resultset.getString(2)==null?"":resultset.getString(2));
					COUNTERPARTY_ABBR.add(resultset.getString(3)==null?"":resultset.getString(3));
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	//effective - counterparty effective date validity 
	//clearance - KYC | IGX
	//Active | Deactive check
	public void getEffectiveCustomerCounterpartyList(String clearance, String comp_cd)
	{
		try
		{
			if(init())
			{
				COUNTERPARTY_CD.clear();
				COUNTERPARTY_NM.clear();
				COUNTERPARTY_ABBR.clear();
				
				query = "SELECT COUNTERPARTY_CD,COUNTERPARTY_NM,COUNTERPARTY_ABBR "
						+ "FROM FMS_COUNTERPARTY_MST A "
						+ "WHERE COMPANY_CD='"+comp_cd+"' AND EFF_DT=(SELECT MAX(B.EFF_DT) FROM FMS_COUNTERPARTY_MST B WHERE A.COUNTERPARTY_CD=B.COUNTERPARTY_CD "
						+ "AND B.EFF_DT<=TO_DATE(TO_CHAR(SYSDATE,'DD/MM/YYYY'),'DD/MM/YYYY') AND A.COMPANY_CD=B.COMPANY_CD) "
						+ "AND CUSTOMER='Y' AND STATUS='Y' ";
				if(clearance.equals("KYC")) {
					query+="AND KYC='Y' ";
				}
				else if(clearance.equals("IGX")) {
					query+="AND IGX='Y' ";
				}
					query+= "ORDER BY COUNTERPARTY_NM";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				while(resultset.next())
				{
					COUNTERPARTY_CD.add(resultset.getString(1)==null?"":resultset.getString(1));
					COUNTERPARTY_NM.add(resultset.getString(2)==null?"":resultset.getString(2));
					COUNTERPARTY_ABBR.add(resultset.getString(3)==null?"":resultset.getString(3));
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void getEffectiveCustomerCounterpartyList(String comp_cd)
	{
		try
		{
			if(init())
			{
				COUNTERPARTY_CD.clear();
				COUNTERPARTY_NM.clear();
				COUNTERPARTY_ABBR.clear();
				
				query = "SELECT COUNTERPARTY_CD,COUNTERPARTY_NM,COUNTERPARTY_ABBR "
						+ "FROM FMS_COUNTERPARTY_MST A "
						+ "WHERE COMPANY_CD='"+comp_cd+"' AND EFF_DT=(SELECT MAX(B.EFF_DT) FROM FMS_COUNTERPARTY_MST B WHERE A.COUNTERPARTY_CD=B.COUNTERPARTY_CD "
						+ "AND B.EFF_DT<=TO_DATE(TO_CHAR(SYSDATE,'DD/MM/YYYY'),'DD/MM/YYYY') AND A.COMPANY_CD=B.COMPANY_CD) "
						+ "AND CUSTOMER='Y' AND STATUS='Y' ";
				query+= "ORDER BY COUNTERPARTY_NM";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				while(resultset.next())
				{
					COUNTERPARTY_CD.add(resultset.getString(1)==null?"":resultset.getString(1));
					COUNTERPARTY_NM.add(resultset.getString(2)==null?"":resultset.getString(2));
					COUNTERPARTY_ABBR.add(resultset.getString(3)==null?"":resultset.getString(3));
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	public void getEffectiveTraderCounterpartyList(String comp_cd)
	{
		try
		{
			if(init())
			{
				COUNTERPARTY_CD.clear();
				COUNTERPARTY_NM.clear();
				COUNTERPARTY_ABBR.clear();
				
				query = "SELECT COUNTERPARTY_CD,COUNTERPARTY_NM,COUNTERPARTY_ABBR "
						+ "FROM FMS_COUNTERPARTY_MST A "
						+ "WHERE COMPANY_CD='"+comp_cd+"' AND EFF_DT=(SELECT MAX(B.EFF_DT) FROM FMS_COUNTERPARTY_MST B WHERE A.COUNTERPARTY_CD=B.COUNTERPARTY_CD "
						+ "AND B.EFF_DT<=TO_DATE(TO_CHAR(SYSDATE,'DD/MM/YYYY'),'DD/MM/YYYY') AND A.COMPANY_CD=B.COMPANY_CD) "
						+ "AND TRADER='Y' AND STATUS='Y' ";
				query+= "ORDER BY COUNTERPARTY_NM";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				while(resultset.next())
				{
					COUNTERPARTY_CD.add(resultset.getString(1)==null?"":resultset.getString(1));
					COUNTERPARTY_NM.add(resultset.getString(2)==null?"":resultset.getString(2));
					COUNTERPARTY_ABBR.add(resultset.getString(3)==null?"":resultset.getString(3));
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	//effective - counterparty effective date validity 
	//clearance - KYC | IGX
	//Active | Deactive check
	public void getEffectiveTransporterCounterpartyList(String clearance, String comp_cd)
	{
		try
		{
			if(init())
			{
				COUNTERPARTY_CD.clear();
				COUNTERPARTY_NM.clear();
				COUNTERPARTY_ABBR.clear();
				
				query = "SELECT COUNTERPARTY_CD,COUNTERPARTY_NM,COUNTERPARTY_ABBR "
						+ "FROM FMS_COUNTERPARTY_MST A "
						+ "WHERE COMPANY_CD='"+comp_cd+"' AND EFF_DT=(SELECT MAX(B.EFF_DT) FROM FMS_COUNTERPARTY_MST B WHERE A.COUNTERPARTY_CD=B.COUNTERPARTY_CD "
						+ "AND B.EFF_DT<=TO_DATE(TO_CHAR(SYSDATE,'DD/MM/YYYY'),'DD/MM/YYYY') AND A.COMPANY_CD=B.COMPANY_CD) "
						+ "AND TRANSPORTER='Y' AND STATUS='Y' ";
				if(clearance.equals("KYC")) {
					query+="AND KYC='Y' ";
				}
				else if(clearance.equals("IGX")) {
					query+="AND IGX='Y' ";
				}
					query+= "ORDER BY COUNTERPARTY_NM";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				while(resultset.next())
				{
					COUNTERPARTY_CD.add(resultset.getString(1)==null?"":resultset.getString(1));
					COUNTERPARTY_NM.add(resultset.getString(2)==null?"":resultset.getString(2));
					COUNTERPARTY_ABBR.add(resultset.getString(3)==null?"":resultset.getString(3));
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	//effective - counterparty effective date validity 
	//Active | Deactive check
	public void getEffectiveCompanyOwnerList()
	{
		try
		{
			if(init())
			{
				COUNTERPARTY_CD.clear();
				COUNTERPARTY_NM.clear();
				COUNTERPARTY_ABBR.clear();
				
				query = "SELECT COMPANY_CD,COMPANY_NM,COMPANY_ABBR "
						+ "FROM FMS_COMPANY_OWNER_MST A "
						+ "WHERE EFF_DT=(SELECT MAX(B.EFF_DT) FROM FMS_COMPANY_OWNER_MST B WHERE A.COMPANY_CD=B.COMPANY_CD "
						+ "AND B.EFF_DT<=TO_DATE(TO_CHAR(SYSDATE,'DD/MM/YYYY'),'DD/MM/YYYY')) "
						+ "AND STATUS='Y' ";
				query+= "ORDER BY COMPANY_NM";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				while(resultset.next())
				{
					COUNTERPARTY_CD.add(resultset.getString(1)==null?"":resultset.getString(1));
					COUNTERPARTY_NM.add(resultset.getString(2)==null?"":resultset.getString(2));
					COUNTERPARTY_ABBR.add(resultset.getString(3)==null?"":resultset.getString(3));
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	//effective - counterparty plant effective date validity
	public void getEffectiveCounterpartyPlantList(String counterparty_cd, String entity, String comp_cd)
	{
		try
		{
			if(init())
			{
				CD.clear();
				PLANT_NM.clear();
				PLANT_ABBR.clear();
				PLANT_SEQ_NO.clear();
				
				query = "SELECT COUNTERPARTY_CD,PLANT_NAME,PLANT_ABBR,SEQ_NO "
						+ "FROM FMS_COUNTERPARTY_PLANT_DTL A "
						+ "WHERE COUNTERPARTY_CD='"+counterparty_cd+"' AND ENTITY='"+entity+"' AND COMPANY_CD='"+comp_cd+"' "
						+ "AND EFF_DT=(SELECT MAX(B.EFF_DT) FROM FMS_COUNTERPARTY_PLANT_DTL B WHERE A.COUNTERPARTY_CD=B.COUNTERPARTY_CD "
						+ "AND A.SEQ_NO=B.SEQ_NO AND A.COMPANY_CD=B.COMPANY_CD AND B.EFF_DT<=TO_DATE(TO_CHAR(SYSDATE,'DD/MM/YYYY'),'DD/MM/YYYY') AND A.ENTITY=B.ENTITY) "
						+ "AND STATUS='Y' ";
				query+= "ORDER BY PLANT_NAME";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				while(resultset.next())
				{
					CD.add(resultset.getString(1)==null?"":resultset.getString(1));
					PLANT_NM.add(resultset.getString(2)==null?"":resultset.getString(2));
					PLANT_ABBR.add(resultset.getString(3)==null?"":resultset.getString(3));
					PLANT_SEQ_NO.add(resultset.getString(4)==null?"0":resultset.getString(4));
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
		
	//effective - counterparty plant effective date validity
	public void getEffectiveTraderPlantList(String counterparty_cd, String comp_cd)
	{
		try
		{
			if(init())
			{
				TRD_CD.clear();
				TRD_PLANT_NM.clear();
				TRD_PLANT_ABBR.clear();
				TRD_PLANT_SEQ_NO.clear();
				
				query = "SELECT COUNTERPARTY_CD,PLANT_NAME,PLANT_ABBR,SEQ_NO "
						+ "FROM FMS_COUNTERPARTY_PLANT_DTL A "
						+ "WHERE COUNTERPARTY_CD='"+counterparty_cd+"' AND ENTITY='T' AND COMPANY_CD='"+comp_cd+"' "
						+ "AND EFF_DT=(SELECT MAX(B.EFF_DT) FROM FMS_COUNTERPARTY_PLANT_DTL B WHERE A.COUNTERPARTY_CD=B.COUNTERPARTY_CD "
						+ "AND A.SEQ_NO=B.SEQ_NO AND A.COMPANY_CD=B.COMPANY_CD AND B.EFF_DT<=TO_DATE(TO_CHAR(SYSDATE,'DD/MM/YYYY'),'DD/MM/YYYY') AND A.ENTITY=B.ENTITY) "
						+ "AND STATUS='Y' ";
				query+= "ORDER BY PLANT_NAME";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				while(resultset.next())
				{
					TRD_CD.add(resultset.getString(1)==null?"":resultset.getString(1));
					TRD_PLANT_NM.add(resultset.getString(2)==null?"":resultset.getString(2));
					TRD_PLANT_ABBR.add(resultset.getString(3)==null?"":resultset.getString(3));
					TRD_PLANT_SEQ_NO.add(resultset.getString(4)==null?"0":resultset.getString(4));
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	//effective - counterparty plant effective date validity
	public void getEffectiveCustomerPlantList(String counterparty_cd, String comp_cd)
	{
		try
		{
			if(init())
			{
				TRD_CD.clear();
				TRD_PLANT_NM.clear();
				TRD_PLANT_ABBR.clear();
				TRD_PLANT_SEQ_NO.clear();
				
				query = "SELECT COUNTERPARTY_CD,PLANT_NAME,PLANT_ABBR,SEQ_NO "
						+ "FROM FMS_COUNTERPARTY_PLANT_DTL A "
						+ "WHERE COUNTERPARTY_CD='"+counterparty_cd+"' AND ENTITY='C' AND COMPANY_CD='"+comp_cd+"' "
						+ "AND EFF_DT=(SELECT MAX(B.EFF_DT) FROM FMS_COUNTERPARTY_PLANT_DTL B WHERE A.COUNTERPARTY_CD=B.COUNTERPARTY_CD "
						+ "AND A.SEQ_NO=B.SEQ_NO AND A.COMPANY_CD=B.COMPANY_CD AND B.EFF_DT<=TO_DATE(TO_CHAR(SYSDATE,'DD/MM/YYYY'),'DD/MM/YYYY') AND A.ENTITY=B.ENTITY) "
						+ "AND STATUS='Y' ";
				query+= "ORDER BY PLANT_NAME";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				while(resultset.next())
				{
					TRD_CD.add(resultset.getString(1)==null?"":resultset.getString(1));
					TRD_PLANT_NM.add(resultset.getString(2)==null?"":resultset.getString(2));
					TRD_PLANT_ABBR.add(resultset.getString(3)==null?"":resultset.getString(3));
					TRD_PLANT_SEQ_NO.add(resultset.getString(4)==null?"0":resultset.getString(4));
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void getEffectiveCustomerPlantList(String comp_cd)
	{
		try
		{
			if(init())
			{
				TRD_CD.clear();
				TRD_PLANT_NM.clear();
				TRD_PLANT_ABBR.clear();
				TRD_PLANT_SEQ_NO.clear();
				
				query = "SELECT COUNTERPARTY_CD,PLANT_NAME,PLANT_ABBR,SEQ_NO "
						+ "FROM FMS_COUNTERPARTY_PLANT_DTL A "
						+ "WHERE ENTITY='C' AND COMPANY_CD='"+comp_cd+"' "
						+ "AND EFF_DT=(SELECT MAX(B.EFF_DT) FROM FMS_COUNTERPARTY_PLANT_DTL B WHERE A.COUNTERPARTY_CD=B.COUNTERPARTY_CD "
						+ "AND A.SEQ_NO=B.SEQ_NO AND A.COMPANY_CD=B.COMPANY_CD AND B.EFF_DT<=TO_DATE(TO_CHAR(SYSDATE,'DD/MM/YYYY'),'DD/MM/YYYY') AND A.ENTITY=B.ENTITY) "
						+ "AND STATUS='Y' ";
				query+= "ORDER BY PLANT_NAME";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				while(resultset.next())
				{
					TRD_CD.add(resultset.getString(1)==null?"":resultset.getString(1));
					TRD_PLANT_NM.add(resultset.getString(2)==null?"":resultset.getString(2));
					TRD_PLANT_ABBR.add(resultset.getString(3)==null?"":resultset.getString(3));
					TRD_PLANT_SEQ_NO.add(resultset.getString(4)==null?"0":resultset.getString(4));
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	//effective - counterparty plant effective date validity
	public void getEffectiveTransporterPlantList(String comp_cd)
	{
		try
		{
			if(init())
			{
				TRANS_CD.clear();
				TRANS_PLANT_NM.clear();
				TRANS_PLANT_ABBR.clear();
				TRANS_PLANT_SEQ_NO.clear();
				
				query = "SELECT COUNTERPARTY_CD,PLANT_NAME,PLANT_ABBR,SEQ_NO "
						+ "FROM FMS_COUNTERPARTY_PLANT_DTL A "
						+ "WHERE ENTITY='R' AND COMPANY_CD='"+comp_cd+"' "
						+ "AND EFF_DT=(SELECT MAX(b.EFF_DT) FROM FMS_COUNTERPARTY_PLANT_DTL B WHERE A.COUNTERPARTY_CD=B.COUNTERPARTY_CD "
						+ "AND A.SEQ_NO=B.SEQ_NO AND A.COMPANY_CD=B.COMPANY_CD AND B.EFF_DT<=TO_DATE(TO_CHAR(SYSDATE,'DD/MM/YYYY'),'DD/MM/YYYY') AND A.ENTITY=B.ENTITY) "
						+ "AND STATUS='Y' ";
				query+= "ORDER BY COUNTERPARTY_CD,PLANT_NAME";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				while(resultset.next())
				{
					TRANS_CD.add(resultset.getString(1)==null?"":resultset.getString(1));
					TRANS_PLANT_NM.add(resultset.getString(2)==null?"":resultset.getString(2));
					TRANS_PLANT_ABBR.add(resultset.getString(3)==null?"":resultset.getString(3));
					TRANS_PLANT_SEQ_NO.add(resultset.getString(4)==null?"0":resultset.getString(4));
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	//effective - counterparty business plant effective date validity
	public void getEffectiveCounterpartyBusinessPlantList(String counterparty_cd, String entity, String comp_cd)
	{
		try
		{
			if(init())
			{
				CD.clear();
				PLANT_NM.clear();
				PLANT_ABBR.clear();
				PLANT_SEQ_NO.clear();
				
				query = "SELECT COUNTERPARTY_CD,PLANT_NAME,PLANT_ABBR,SEQ_NO "
						+ "FROM FMS_COUNTERPARTY_BU_DTL A "
						+ "WHERE COUNTERPARTY_CD='"+counterparty_cd+"' AND ENTITY='"+entity+"' AND COMPANY_CD='"+comp_cd+"' "
						+ "AND EFF_DT=(SELECT MAX(B.EFF_DT) FROM FMS_COUNTERPARTY_BU_DTL B WHERE A.COUNTERPARTY_CD=B.COUNTERPARTY_CD "
						+ "AND A.SEQ_NO=B.SEQ_NO AND A.COMPANY_CD=B.COMPANY_CD AND B.EFF_DT<=TO_DATE(TO_CHAR(SYSDATE,'DD/MM/YYYY'),'DD/MM/YYYY') AND A.ENTITY=B.ENTITY) "
						+ "AND STATUS='Y' ";
				query+= "ORDER BY PLANT_NAME";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				while(resultset.next())
				{
					CD.add(resultset.getString(1)==null?"":resultset.getString(1));
					PLANT_NM.add(resultset.getString(2)==null?"":resultset.getString(2));
					PLANT_ABBR.add(resultset.getString(3)==null?"":resultset.getString(3));
					PLANT_SEQ_NO.add(resultset.getString(4)==null?"0":resultset.getString(4));
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	//effective - counterparty business plant effective date validity
	public void getEffectiveBusinessPlantList(String comp_cd)
	{
		try
		{
			if(init())
			{
				BU_CD.clear();
				BU_PLANT_NM.clear();
				BU_PLANT_ABBR.clear();
				BU_PLANT_SEQ_NO.clear();
				
				query = "SELECT COUNTERPARTY_CD,PLANT_NAME,PLANT_ABBR,SEQ_NO "
						+ "FROM FMS_COUNTERPARTY_PLANT_DTL A "
						+ "WHERE ENTITY='B' AND COMPANY_CD='"+comp_cd+"' "
						+ "AND EFF_DT=(SELECT MAX(b.EFF_DT) FROM FMS_COUNTERPARTY_PLANT_DTL B WHERE A.COUNTERPARTY_CD=B.COUNTERPARTY_CD "
						+ "AND A.SEQ_NO=B.SEQ_NO AND A.COMPANY_CD=B.COMPANY_CD AND B.EFF_DT<=TO_DATE(TO_CHAR(SYSDATE,'DD/MM/YYYY'),'DD/MM/YYYY') AND A.ENTITY=B.ENTITY) "
						+ "AND STATUS='Y' ";
				query+= "ORDER BY PLANT_NAME";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				while(resultset.next())
				{
					BU_CD.add(resultset.getString(1)==null?"":resultset.getString(1));
					BU_PLANT_NM.add(resultset.getString(2)==null?"":resultset.getString(2));
					BU_PLANT_ABBR.add(resultset.getString(3)==null?"":resultset.getString(3));
					BU_PLANT_SEQ_NO.add(resultset.getString(4)==null?"0":resultset.getString(4));
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public String getMinEffectiveDateOfCounterparty(String counterparty_cd, String comp_cd)
	{
		String date="";
		try
		{
			if(init())
			{
				query = "SELECT TO_CHAR(EFF_DT,'DD/MM/YYYY') "
						+ "FROM FMS_COUNTERPARTY_MST A "
						+ "WHERE COUNTERPARTY_CD='"+counterparty_cd+"' AND COMPANY_CD='"+comp_cd+"' "
						+ "AND EFF_DT=(SELECT MIN(B.EFF_DT) FROM FMS_COUNTERPARTY_MST B WHERE A.COUNTERPARTY_CD=B.COUNTERPARTY_CD "
						+ "AND B.EFF_DT<=TO_DATE(TO_CHAR(SYSDATE,'DD/MM/YYYY'),'DD/MM/YYYY') AND A.COMPANY_CD=B.COMPANY_CD) ";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					date = resultset.getString(1)==null?"0":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return date;
	}
	
	public String getCounterpartyPlantABBR(String counterparty_cd, String comp_cd, String plant_seq, String entity)
	{
		String abbr="";
		try
		{
			if(init())
			{
				query = "SELECT PLANT_ABBR "
						+ "FROM FMS_COUNTERPARTY_PLANT_DTL A "
						+ "WHERE COUNTERPARTY_CD='"+counterparty_cd+"' AND ENTITY='"+entity+"' AND COMPANY_CD='"+comp_cd+"' AND SEQ_NO='"+plant_seq+"' "
						+ "AND EFF_DT=(SELECT MAX(B.EFF_DT) FROM FMS_COUNTERPARTY_PLANT_DTL B WHERE A.COUNTERPARTY_CD=B.COUNTERPARTY_CD "
						+ "AND A.SEQ_NO=B.SEQ_NO AND A.COMPANY_CD=B.COMPANY_CD AND B.EFF_DT<=TO_DATE(TO_CHAR(SYSDATE,'DD/MM/YYYY'),'DD/MM/YYYY') AND A.ENTITY=B.ENTITY) ";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					abbr=resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return abbr;
	}

	public String getCounterpartyBuPlantABBR(String counterparty_cd, String comp_cd, String plant_seq, String entity)
	{
		String abbr="";
		try
		{
			if(init())
			{
				query = "SELECT PLANT_ABBR "
						+ "FROM FMS_COUNTERPARTY_BU_DTL A "
						+ "WHERE COUNTERPARTY_CD='"+counterparty_cd+"' AND ENTITY='"+entity+"' AND COMPANY_CD='"+comp_cd+"' AND SEQ_NO='"+plant_seq+"' "
						+ "AND EFF_DT=(SELECT MAX(B.EFF_DT) FROM FMS_COUNTERPARTY_BU_DTL B WHERE A.COUNTERPARTY_CD=B.COUNTERPARTY_CD "
						+ "AND A.SEQ_NO=B.SEQ_NO AND A.COMPANY_CD=B.COMPANY_CD AND B.EFF_DT<=TO_DATE(TO_CHAR(SYSDATE,'DD/MM/YYYY'),'DD/MM/YYYY') AND A.ENTITY=B.ENTITY) ";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					abbr=resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return abbr;
	}

	public String getCounterpartyPlantName(String counterparty_cd, String comp_cd, String plant_seq, String entity)
	{
		String abbr="";
		try
		{
			if(init())
			{
				query = "SELECT PLANT_NAME "
						+ "FROM FMS_COUNTERPARTY_PLANT_DTL A "
						+ "WHERE COUNTERPARTY_CD='"+counterparty_cd+"' AND ENTITY='"+entity+"' AND COMPANY_CD='"+comp_cd+"' AND SEQ_NO='"+plant_seq+"' "
						+ "AND EFF_DT=(SELECT MAX(B.EFF_DT) FROM FMS_COUNTERPARTY_PLANT_DTL B WHERE A.COUNTERPARTY_CD=B.COUNTERPARTY_CD "
						+ "AND A.SEQ_NO=B.SEQ_NO AND A.COMPANY_CD=B.COMPANY_CD AND B.EFF_DT<=TO_DATE(TO_CHAR(SYSDATE,'DD/MM/YYYY'),'DD/MM/YYYY') AND A.ENTITY=B.ENTITY) ";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					abbr=resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return abbr;
	}
	
	public String getFreqNm(String freq)
	{
		String freq_nm="";
		try
		{
			if(freq.equals("1"))
			{
				freq_nm="1st-Fortnight";
			}
			else if(freq.equals("2"))
			{
				freq_nm="2nd-Fortnight";
			}
			else if(freq.equals("3"))
			{
				freq_nm="1st-Weekly";
			}
			else if(freq.equals("4"))
			{
				freq_nm="2nd-Weekly";
			}
			else if(freq.equals("5"))
			{
				freq_nm="3rd-Weekly";
			}
			else if(freq.equals("6"))
			{
				freq_nm="4th-Weekly";
			}
			else if(freq.equals("7"))
			{
				freq_nm="Monthly";
			}
			else if(freq.equals("8"))
			{
				freq_nm="Other";
			}
			else if(freq.equals("9"))
			{
				freq_nm="5th-Weekly";
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return freq_nm;
	}
	
	public String getRateUnitNm(String cd)
	{
		String name="";
		try
		{
			if(init())
			{
				query = "SELECT RATE_UNIT_ABR "
						+ "FROM FMS_RATE_UNIT "
						+ "WHERE RATE_UNIT_CD='"+cd+"' ";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					name = resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return name;
	}
	
	public String RateNumberFormat(double rate, String rate_unit)
	{
		String number="";
		try
		{
			if(rate_unit.equals("1"))
			{
				number = nf.format(rate);
			}
			else if(rate_unit.equals("2"))
			{
				number = nf2.format(rate);
			}
			else
			{
				number = nf.format(rate);
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return number;
	}
	
	//effective - counterparty effective date validity 
	//Active | Deactive check
	public void getEffectiveTransporterCounterpartyList(String comp_cd)
	{
		try
		{
			if(init())
			{
				COUNTERPARTY_CD.clear();
				COUNTERPARTY_NM.clear();
				COUNTERPARTY_ABBR.clear();
				
				query = "SELECT COUNTERPARTY_CD,COUNTERPARTY_NM,COUNTERPARTY_ABBR "
						+ "FROM FMS_COUNTERPARTY_MST A "
						+ "WHERE COMPANY_CD='"+comp_cd+"' AND EFF_DT=(SELECT MAX(B.EFF_DT) FROM FMS_COUNTERPARTY_MST B WHERE A.COUNTERPARTY_CD=B.COUNTERPARTY_CD "
						+ "AND B.EFF_DT<=TO_DATE(TO_CHAR(SYSDATE,'DD/MM/YYYY'),'DD/MM/YYYY') AND A.COMPANY_CD=B.COMPANY_CD) "
						+ "AND TRANSPORTER='Y' AND STATUS='Y' ";
					query+= "ORDER BY COUNTERPARTY_NM";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				while(resultset.next())
				{
					COUNTERPARTY_CD.add(resultset.getString(1)==null?"":resultset.getString(1));
					COUNTERPARTY_NM.add(resultset.getString(2)==null?"":resultset.getString(2));
					COUNTERPARTY_ABBR.add(resultset.getString(3)==null?"":resultset.getString(3));
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public String DueDateCalculation(String inv_date,String days,String type,String exclude_sat,String comp_cd,String state_tin)
	{
		String date="";
		String day_nms="";
		
		if(state_tin.length()==1)
		{
			state_tin="0"+state_tin;
		}
		if(exclude_sat.equals("Y"))
		{
			day_nms="'SAT','SUN'";
		}
		else
		{
			day_nms="'SUN'";
		}
		try
		{
			if(init())
			{
				if(type.equals("C")) //CALENDAR DAYS
				{
					if(!days.equals(""))
					{
						int day=Integer.parseInt(days);
						for(int i=1; i <=day; i++)
		    			{
							query="SELECT TO_CHAR(TO_DATE('"+inv_date+"','DD/MM/YYYY')+"+i+",'DD/MM/YYYY') "
									+ "FROM DUAL";
							stmtement = conn.createStatement();
							resultset = stmtement.executeQuery(query);
							if(resultset.next())
							{
								date = resultset.getString(1)==null?"":resultset.getString(1);
								
								int count=0;
								query1 = "SELECT COUNT(*) "
										+ "FROM DUAL "
										+ "WHERE TO_CHAR(TO_DATE('"+date+"','DD/MM/YYYY'),'DY') IN ("+day_nms+") ";
								stmtement1 = conn.createStatement();
								resultset1=stmtement1.executeQuery(query1);
								if(resultset1.next())
								{
									count = resultset1.getInt(1);
									if(count == 1)
									{
										day = day + 1; 
									}
								}
								stmtement1.close();
								resultset1.close();
							}
							stmtement.close();
							resultset.close();
		    			}
					}
				}
				else if(type.equals("B")) //BUSINESS DAY
				{
					if(!days.equals(""))
					{
						int day=Integer.parseInt(days);
						for(int i=1; i <=day; i++)
		    			{
							query="SELECT TO_CHAR(TO_DATE('"+inv_date+"','DD/MM/YYYY')+"+i+",'DD/MM/YYYY') "
									+ "FROM DUAL";
							stmtement = conn.createStatement();
							resultset = stmtement.executeQuery(query);
							if(resultset.next())
							{
								date = resultset.getString(1)==null?"":resultset.getString(1);
								
								int count=0;
								
								query1="SELECT COUNT(*) "
										+ "FROM FMS_HOLIDAY_DTL "
										+ "WHERE COMPANY_CD='"+comp_cd+"' AND HOLIDAY_DT=TO_DATE('"+date+"','DD/MM/YYYY') "
										+ "AND FLAG='Y' AND STATE_TIN='"+state_tin+"'";
								stmtement1 = conn.createStatement();
								resultset1=stmtement1.executeQuery(query1);
								if(resultset1.next())
								{
									count = resultset1.getInt(1);
									if(count == 1)
									{
										day = day + 1; 
									}
								}
								stmtement1.close();
								resultset1.close();
								if(count==0)
								{
									query1 = "SELECT COUNT(*) "
											+ "FROM DUAL "
											+ "WHERE TO_CHAR(TO_DATE('"+date+"','DD/MM/YYYY'),'DY') IN ("+day_nms+") ";
									stmtement1 = conn.createStatement();
									resultset1=stmtement1.executeQuery(query1);
									if(resultset1.next())
									{
										count = resultset1.getInt(1);
										if(count == 1)
										{
											day = day + 1; 
										}
									}
									stmtement1.close();
									resultset1.close();
								}
							}
							stmtement.close();
							resultset.close();
		    			}
					}
				}
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return date;
	}
		
	public String getContVariableDCQ(String comp_cd,String counterparty_cd, String agmt_no, String cont_no, String cont_type, String date)
	{
		String dcq="";
		try
		{
			if(init())
			{
				query="SELECT DCQ "
						+ "FROM FMS_SUPPLY_CONT_DCQ_DTL "
						+ "WHERE COMPANY_CD='"+comp_cd+"' AND COUNTERPARTY_CD='"+counterparty_cd+"' "
						+ "AND AGMT_NO='"+agmt_no+"' AND CONT_NO='"+cont_no+"' "
						+ "AND CONTRACT_TYPE='"+cont_type+"' AND STATUS='Y' "
						+ "AND FROM_DT<=TO_DATE('"+date+"','DD/MM/YYYY') AND TO_DT>=TO_DATE('"+date+"','DD/MM/YYYY') ";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					dcq=resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return dcq;
	}
	
	public String getDisplayDealMapping(String agmt, String agmt_rev, String cont, String cont_rev, String cont_type)
	{
		String dealMapping="";
		try
		{
			if(cont_type.equals("S"))
			{
				dealMapping=cont_type+""+agmt+"-"+cont;
			}
			else
			{
				dealMapping=cont_type+""+cont;
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return dealMapping;
	}
	
	public String getToMailReceipentList(String comp_cd, String menu_nm, String module_nm, String rpt_freq, String gen_type)
    {
    	String recipient_list="";
    	try
    	{
    		if(init())
			{
	    		String report_freq="";
	    		String recipient_cd = "";
	    		
	    		int support_flag = 0;
	    		query="SELECT COUNT(*) FROM "
	    				+ "FMS_EMAIL_SUPPORT_MST "
	    				+ "WHERE UPPER(MODULE_NAME)='"+module_nm.toUpperCase()+"' AND "
	    				+ "UPPER(MENU_NAME) LIKE '"+menu_nm.toUpperCase()+"' AND UPPER(REPORT_FREQ)='"+rpt_freq.toUpperCase()+"' AND "
	    				+ "UPPER(GENERATION_TYPE)='"+gen_type.toUpperCase()+"' AND SUPPORT_FLAG='Y'";
	    		stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					support_flag=resultset.getInt(1);
				}
				stmtement.close();
				resultset.close();
	    		
	    		if(support_flag > 0)
	    		{
					query="SELECT REPORT_FREQ,RECIPIENTS_CD "
							+ "FROM FMS_EMAIL_RECIPIENT_MST "
							+ "WHERE UPPER(MENU_NAME) LIKE '"+menu_nm.toUpperCase()+"' AND COMPANY_CD='"+comp_cd+"' "
							+ "AND UPPER(MODULE_NAME)='"+module_nm.toUpperCase()+"' AND UPPER(REPORT_FREQ)='"+rpt_freq.toUpperCase()+"' "
							+ "AND UPPER(GENERATION_TYPE)='"+gen_type.toUpperCase()+"'";
					stmtement1 = conn.createStatement();
					resultset1 = stmtement1.executeQuery(query);
					if(resultset1.next())
					{
						report_freq = resultset1.getString(1)==null?"":resultset1.getString(1);
						recipient_cd = resultset1.getString(2)==null?"0":resultset1.getString(2);
					}
					stmtement1.close();
					resultset1.close();
					
					int count=0;
					query="SELECT COUNT(*) "
							+ "FROM FMS_EMAIL_RECIPIENT_DTL A "
		    				+ "WHERE COMPANY_CD='"+comp_cd+"' AND RECIPIENTS_CD = (SELECT RECIPIENTS_CD FROM FMS_EMAIL_RECIPIENT_MST B "
		    				+ "WHERE A.COMPANY_CD=B.COMPANY_CD AND RECIPIENTS_CD='"+recipient_cd+"' AND STOP_FLAG='N') "
		    				+ "AND UPPER(ENABLE_DISABLE)='Y'";
					stmtement2 = conn.createStatement();
					resultset2 = stmtement2.executeQuery(query);
					if(resultset2.next())
					{
						count = resultset2.getInt(1);
					}
					stmtement2.close();
					resultset2.close();
					
					if(count > 0)
					{
						int temp_count=0;
						query="SELECT TO_EMAIL "
								+ "FROM FMS_EMAIL_RECIPIENT_DTL A "
			    				+ "WHERE COMPANY_CD='"+comp_cd+"' AND RECIPIENTS_CD = (SELECT RECIPIENTS_CD FROM FMS_EMAIL_RECIPIENT_MST B "
			    				+ "WHERE A.COMPANY_CD=B.COMPANY_CD AND RECIPIENTS_CD='"+recipient_cd+"' AND STOP_FLAG='N') "
			    				+ "AND UPPER(ENABLE_DISABLE)='Y'";
						stmtement = conn.createStatement();
						resultset = stmtement.executeQuery(query);
						while(resultset.next())
						{
			    			temp_count=temp_count+1;
			    			if(temp_count == count)
			    			{
			    				recipient_list += resultset.getString(1)==null?"":resultset.getString(1);
			    			}
			    			else
			    			{
			    				recipient_list += resultset.getString(1)==null?"":resultset.getString(1)+",";
			    			}
			    		}
						stmtement.close();
						resultset.close();
					}
	    		}
	    		conn.close();
			}
    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	}
    	return recipient_list;
    }
	
	public String getCcMailReceipentList(String comp_cd,String menu_nm, String module_nm, String rpt_freq, String gen_type)
    {
    	String recipient_list="";
    	try
    	{
    		if(init())
			{
	    		String report_freq="";
	    		String recipient_cd = "";
	    		
	    		int support_flag = 0;
	    		query="SELECT COUNT(*) FROM "
	    				+ "FMS_EMAIL_SUPPORT_MST "
	    				+ "WHERE UPPER(MODULE_NAME)='"+module_nm.toUpperCase()+"' AND "
	    				+ "UPPER(MENU_NAME) LIKE '"+menu_nm.toUpperCase()+"' AND UPPER(REPORT_FREQ)='"+rpt_freq.toUpperCase()+"' AND "
	    				+ "UPPER(GENERATION_TYPE)='"+gen_type.toUpperCase()+"' AND SUPPORT_FLAG='Y'";
	    		stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					support_flag=resultset.getInt(1);
				}
				stmtement.close();
				resultset.close();
	    		
	    		if(support_flag > 0)
	    		{
					query="SELECT REPORT_FREQ,RECIPIENTS_CD "
							+ "FROM FMS_EMAIL_RECIPIENT_MST "
							+ "WHERE UPPER(MENU_NAME) LIKE '"+menu_nm.toUpperCase()+"' AND COMPANY_CD='"+comp_cd+"' "
							+ "AND UPPER(MODULE_NAME)='"+module_nm.toUpperCase()+"' AND UPPER(REPORT_FREQ)='"+rpt_freq.toUpperCase()+"' "
							+ "AND UPPER(GENERATION_TYPE)='"+gen_type.toUpperCase()+"'";
					stmtement1 = conn.createStatement();
					resultset1 = stmtement1.executeQuery(query);
					if(resultset1.next())
					{
						report_freq = resultset1.getString(1)==null?"":resultset1.getString(1);
						recipient_cd = resultset1.getString(2)==null?"0":resultset1.getString(2);
					}
					stmtement1.close();
					resultset1.close();
					
					int count=0;
					query="SELECT COUNT(*) "
							+ "FROM FMS_EMAIL_RECIPIENT_DTL A "
		    				+ "WHERE COMPANY_CD='"+comp_cd+"' AND RECIPIENTS_CD = (SELECT RECIPIENTS_CD FROM FMS_EMAIL_RECIPIENT_MST B "
		    				+ "WHERE A.COMPANY_CD=B.COMPANY_CD AND RECIPIENTS_CD='"+recipient_cd+"' AND STOP_FLAG='N') "
		    				+ "AND UPPER(ENABLE_DISABLE)='Y'";
					stmtement2 = conn.createStatement();
					resultset2 = stmtement2.executeQuery(query);
					if(resultset2.next())
					{
						count = resultset2.getInt(1);
					}
					stmtement2.close();
					resultset2.close();
					
					if(count > 0)
					{
						int temp_count=0;
						query="SELECT CC_EMAIL "
								+ "FROM FMS_EMAIL_RECIPIENT_DTL A "
			    				+ "WHERE COMPANY_CD='"+comp_cd+"' AND RECIPIENTS_CD = (SELECT RECIPIENTS_CD FROM FMS_EMAIL_RECIPIENT_MST B "
			    				+ "WHERE A.COMPANY_CD=B.COMPANY_CD AND RECIPIENTS_CD='"+recipient_cd+"' AND STOP_FLAG='N') "
			    				+ "AND UPPER(ENABLE_DISABLE)='Y'";
						stmtement = conn.createStatement();
						resultset = stmtement.executeQuery(query);
						while(resultset.next())
						{
			    			temp_count=temp_count+1;
			    			if(temp_count == count)
			    			{
			    				recipient_list += resultset.getString(1)==null?"":resultset.getString(1);
			    			}
			    			else
			    			{
			    				recipient_list += resultset.getString(1)==null?"":resultset.getString(1)+",";
			    			}
			    		}
						stmtement.close();
						resultset.close();
					}
	    		}
	    		conn.close();
			}
    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	}
    	return recipient_list;
    }
	
	public String getEntityTaxStructureDtl(String comp_cd, String counterparty_cd, String entity, String seq, String bu, String date)
    {
    	String structure_dtl="";
    	try
    	{
    		if(init())
			{
    			query="SELECT TAX_STRUCT_DTL "
						+ "FROM FMS_ENTITY_TAX_STRUCT_DTL A "
						+ "WHERE ENTITY='"+entity+"' AND COMPANY_CD='"+comp_cd+"' AND COUNTERPARTY_CD='"+counterparty_cd+"' "
						+ "AND PLANT_SEQ_NO='"+seq+"' AND BU_UNIT='"+bu+"' "
						+ "AND EFF_DT=(SELECT MAX(B.EFF_DT) FROM FMS_ENTITY_TAX_STRUCT_DTL B "
						+ "WHERE A.ENTITY=B.ENTITY AND A.COMPANY_CD=B.COMPANY_CD AND A.COUNTERPARTY_CD=B.COUNTERPARTY_CD "
						+ "AND A.PLANT_SEQ_NO=B.PLANT_SEQ_NO AND A.BU_UNIT=B.BU_UNIT AND B.EFF_DT<=TO_DATE('"+date+"','DD/MM/YYYY'))";
    			stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					structure_dtl=resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	}
    	return structure_dtl;
    }
	
	public String getEntityBuTaxStructureDtl(String comp_cd, String counterparty_cd, String entity, String seq, String bu, String date, String inv_type)
    {
    	String structure_dtl="";
    	try
    	{
    		if(init())
			{
    			query="SELECT TAX_STRUCT_DTL "
						+ "FROM FMS_ENTITY_BU_SVC_TAX_DTL A "
						+ "WHERE ENTITY='"+entity+"' AND COMPANY_CD='"+comp_cd+"' AND COUNTERPARTY_CD='"+counterparty_cd+"' "
						+ "AND PLANT_SEQ_NO='"+seq+"' AND BU_UNIT='"+bu+"' AND INVOICE_TYPE='"+inv_type+"' "
						+ "AND EFF_DT=(SELECT MAX(B.EFF_DT) FROM FMS_ENTITY_BU_SVC_TAX_DTL B "
						+ "WHERE A.ENTITY=B.ENTITY AND A.COMPANY_CD=B.COMPANY_CD AND A.COUNTERPARTY_CD=B.COUNTERPARTY_CD "
						+ "AND A.PLANT_SEQ_NO=B.PLANT_SEQ_NO AND A.BU_UNIT=B.BU_UNIT AND A.INVOICE_TYPE=B.INVOICE_TYPE "
						+ "AND B.EFF_DT<=TO_DATE('"+date+"','DD/MM/YYYY'))";
    			stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					structure_dtl=resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	}
    	return structure_dtl;
    }
	
	public String getFirstDtOfBillingCycle(String billing_frq,String days,String date)
	{
		String period_start_dt="";
		String period_end_dt="";
		try
		{
			String[] split=date.split("/");
			String month=split[1];
			String year=split[2];
			
			if(billing_frq.equals("F"))
			{	
				int count=dateUtil.getDays(date, "15/"+month+"/"+year);
				int count1=dateUtil.getDays(date, ""+dateUtil.getLastDateOfMonth(month, year));
				
				if(count <= 1)
				{
					period_start_dt= "01/"+month+"/"+year;
				}
				else if(count1 <= 1)
				{
					period_start_dt= "16/"+month+"/"+year;
				}
			}
			else if(billing_frq.equals("W"))
			{
				int count=dateUtil.getDays(date, "07/"+month+"/"+year);
				int count1=dateUtil.getDays(date, "14/"+month+"/"+year);
				int count2=dateUtil.getDays(date, "21/"+month+"/"+year);
				int count3=dateUtil.getDays(date, "28/"+month+"/"+year);
				int count4=dateUtil.getDays(date, ""+dateUtil.getLastDateOfMonth(month, year));
				
				if(count <= 1)
				{
					period_start_dt= "01/"+month+"/"+year;
				}
				else if(count1 <= 1)
				{
					period_start_dt= "08/"+month+"/"+year;
				}
				else if(count2 <= 1)
				{
					period_start_dt= "15/"+month+"/"+year;
				}
				else if(count3 <= 1)
				{
					period_start_dt= "22/"+month+"/"+year;
				}
				else if(count4 <= 1)
				{
					if(month.equals("02"))
					{
						int noofdays=dateUtil.getDays(""+dateUtil.getLastDateOfMonth(month, year), ""+dateUtil.getFirstDateOfMonth(month, year));
						if(noofdays==29)
						{
							period_start_dt="29/"+month+"/"+year;
						}
						/*else
						{
							period_start_dt=""+dateUtil.getLastDateOfMonth(month, year);
						}*/
					}
					else
					{
						period_start_dt="29/"+month+"/"+year;
					}
				}
			}
			else if(billing_frq.equals("M"))
			{
				int count=dateUtil.getDays(date, ""+dateUtil.getLastDateOfMonth(month, year));
				
				if(count <= 1)
				{
					period_start_dt= "01/"+month+"/"+year;
				}
			}
			
			if(period_start_dt.equals(""))
			{
				period_start_dt=date;
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return period_start_dt;
	}
	
	public String getSalesContractName(String contract_type)
	{
		String contract_nm="";
		try
		{
			if(contract_type.equals("S"))
			{
				contract_nm="SN";
			}
			else if(contract_type.equals("L"))
			{
				contract_nm="LOA";
			}
			else if(contract_type.equals("X"))
			{
				contract_nm="IGX";
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return contract_nm;
	}
	
	public String getTranspoterCustomerCode(String comp_cd, String transporter_cd,String customer_cd,String customer_plant_seq,String date)
	{
		String customer_code="";
    	try
    	{
    		if(init())
			{
    			query="SELECT CUSTOMER_CODE "
					+ "FROM FMS_TRANSPORTER_CUST_CD A "
					+ "WHERE COMPANY_CD='"+comp_cd+"' AND TRANSPORTER_CD='"+transporter_cd+"' "
					+ "AND COUNTERPARTY_CD='"+customer_cd+"' AND PLANT_SEQ='"+customer_plant_seq+"' AND STATUS='Y' "
					+ "AND EFF_DT=(SELECT MAX(EFF_DT) FROM FMS_TRANSPORTER_CUST_CD B WHERE A.COMPANY_CD=B.COMPANY_CD "
					+ "AND A.TRANSPORTER_CD=B.TRANSPORTER_CD AND A.COUNTERPARTY_CD=B.COUNTERPARTY_CD AND A.PLANT_SEQ=B.PLANT_SEQ "
					+ "AND EFF_DT<=TO_DATE('"+date+"','DD/MM/YYYY') AND A.STATUS=B.STATUS)";
    			stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					customer_code=resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	}
    	return customer_code;
	}
	
	public String getCounterpartyBuABBR(String counterparty_cd, String comp_cd, String plant_seq, String entity)
	{
		String abbr="";
		try
		{
			if(init())
			{
				query = "SELECT PLANT_ABBR "
						+ "FROM FMS_COUNTERPARTY_BU_DTL A "
						+ "WHERE COUNTERPARTY_CD='"+counterparty_cd+"' AND ENTITY='"+entity+"' AND COMPANY_CD='"+comp_cd+"' AND SEQ_NO='"+plant_seq+"' "
						+ "AND EFF_DT=(SELECT MAX(B.EFF_DT) FROM FMS_COUNTERPARTY_BU_DTL B WHERE A.COUNTERPARTY_CD=B.COUNTERPARTY_CD "
						+ "AND A.SEQ_NO=B.SEQ_NO AND A.COMPANY_CD=B.COMPANY_CD AND B.EFF_DT<=TO_DATE(TO_CHAR(SYSDATE,'DD/MM/YYYY'),'DD/MM/YYYY') AND A.ENTITY=B.ENTITY) ";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					abbr=resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return abbr;
	}
	
	public String getTaxSAPcode(String tax_struct_cd,String comp_cd)
	{
		String name="";
		try
		{
			if(init())
			{
				query = "SELECT SAP_TAX_CODE "
						+ "FROM FMS_TAX_STRUCTURE "
						+ "WHERE COMPANY_CD='"+comp_cd+"' AND TAX_STR_CD='"+tax_struct_cd+"'";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					name = resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return name;
	}
	
	public String getTaxSAPcode(String tax_struct_cd, String tax_code, String comp_cd)
	{
		String name="";
		try
		{
			if(init())
			{
				query = "SELECT SAP_TAX_CODE "
						+ "FROM FMS_TAX_STRUCTURE_DTL "
						+ "WHERE COMPANY_CD='"+comp_cd+"' AND TAX_CODE='"+tax_code+"' "
						+ "AND TAX_STR_CD='"+tax_struct_cd+"'";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					name = resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return name;
	}
	
	public String getTaxGLcode(String tax_struct_cd,String comp_cd)
	{
		String name="";
		try
		{
			if(init())
			{
				query = "SELECT SAP_GL "
						+ "FROM FMS_TAX_STRUCTURE "
						+ "WHERE COMPANY_CD='"+comp_cd+"' AND TAX_STR_CD='"+tax_struct_cd+"'";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					name = resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return name;
	}
	
	public String getTaxGLcode(String tax_struct_cd, String tax_code, String comp_cd)
	{
		String name="";
		try
		{
			if(init())
			{
				query = "SELECT SAP_GL "
						+ "FROM FMS_TAX_STRUCTURE_DTL "
						+ "WHERE COMPANY_CD='"+comp_cd+"' AND TAX_CODE='"+tax_code+"' "
						+ "AND TAX_STR_CD='"+tax_struct_cd+"'";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					name = resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return name;
	}
	
	public String getBankName(String cd,String comp_cd)
	{
		String name="";
		try
		{
			if(init())
			{
				query="SELECT BANK_NAME "
						+ "FROM FMS_BANK_MST A "
						+ "WHERE COMPANY_CD='"+comp_cd+"' AND BANK_CD='"+cd+"' "
						+ "AND EFF_DT=(SELECT MAX(EFF_DT) FROM FMS_BANK_MST B "
						+ "WHERE A.COMPANY_CD=B.COMPANY_CD AND A.BANK_CD=B.BANK_CD) ";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					name = resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return name;
	}
	
	public String PrePaddingZero(String code, int numLen)
	{
		String final_code="";
		try
		{
			final_code=code;
			int number = numLen-code.length();
			for(int i=0;i<number;i++)
			{
				final_code="0"+final_code;
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return final_code;
	}
	
	public String getFromMailId(String comp_cd)
	{
		String key_value="";
		try
		{
			if(init())
			{
				query = "SELECT KEY_VALUE "
						+ "FROM FMS_SRV_SETTING "
						+ "WHERE COMPANY_CD='"+comp_cd+"' AND KEY_NM='EMAIL_FROM' ";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					key_value=resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return key_value;
	}
	
	public String getGLdesc(String comp_cd,String gl_code)
	{
		String gl_desc="";
		try
		{
			if(init())
			{
				query = "SELECT GL_DECR "
						+ "FROM FMS_SAP_ACCOUNT_GL_MST "
						+ "WHERE COMPANY_CD='"+comp_cd+"' AND GL_CODE='"+gl_code+"' ";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					gl_desc=resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return gl_desc;
	}
	
	public String RemovePrePaddingZero(String code)
	{
		String final_code="";
		try
		{
			int number = code.length();
			int count=0;
			for(int i=0;i<number;i++)
			{
				String temp = ""+code.charAt(i);
				if(temp.equals("0"))
				{
					count++;
				}
				else
				{
					break;
				}
			}
			
			final_code=code.substring(count, number);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return final_code;
	}
	
	public String getBankABBR(String cd,String comp_cd)
	{
		String abbr="";
		try
		{
			if(init())
			{
				query="SELECT BANK_ABBR "
						+ "FROM FMS_BANK_MST A "
						+ "WHERE COMPANY_CD='"+comp_cd+"' AND BANK_CD='"+cd+"' "
						+ "AND EFF_DT=(SELECT MAX(EFF_DT) FROM FMS_BANK_MST B "
						+ "WHERE A.COMPANY_CD=B.COMPANY_CD AND A.BANK_CD=B.BANK_CD) ";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					abbr = resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return abbr;
	}
	
	public String  getBillingFreqNm(String freq)
	{
		String nm="";
		try
		{
			if(freq.equals("1") || freq.equals("2"))
			{
				if(freq.equals("1"))
				{
					nm="1st-Fortnight";
				}
				else if(freq.equals("2"))
				{
					nm="2nd-Fortnight";
				}
			}
			else if(freq.equals("3") || freq.equals("4") || freq.equals("5") || freq.equals("6") || freq.equals("9"))
			{
				if(freq.equals("3"))
				{
					nm="1st-Weekly";
				}
				else if(freq.equals("4")) 
				{
					nm="2nd-Weekly";
				}
				else if(freq.equals("5")) 
				{
					nm="3rd-Weekly";
				} 
				else if(freq.equals("6")) 
				{
					nm="4th-Weekly";
				} 
				else if(freq.equals("9"))
				{
					nm="5th-Weekly";
				}
			}
			else if(freq.equals("7"))
			{
				nm="Monthly";
			}
			else if(freq.equals("8"))
			{
				nm="Other";
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return nm;
	}
	
	public String getInvoicePrefix(String comp_cd)
	{
		String inv_prefix="";
		try
		{
			if(init())
			{
				query="SELECT INVOICE_PREFIX "
						+ "FROM FMS_COMPANY_OWNER_MST A "
						+ "WHERE COMPANY_CD='"+comp_cd+"' AND EFF_DT=(SELECT MAX(EFF_DT) FROM FMS_COMPANY_OWNER_MST B "
						+ "WHERE A.COMPANY_CD=B.COMPANY_CD)";
				stmtement = conn.createStatement();
				resultset = stmtement.executeQuery(query);
				if(resultset.next())
				{
					inv_prefix = resultset.getString(1)==null?"":resultset.getString(1);
				}
				stmtement.close();
				resultset.close();
				conn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return inv_prefix;
	}
	
	Vector COUNTRY_CODE = new Vector();
	Vector COUNTRY_NM = new Vector();
	Vector ISO_CODE = new Vector();
	
	Vector TIN = new Vector();
	Vector STATE_CODE = new Vector();
	Vector STATE_NM = new Vector();
	
	Vector COUNTERPARTY_CD = new Vector();
	Vector COUNTERPARTY_NM = new Vector();
	Vector COUNTERPARTY_ABBR = new Vector();
	Vector TRD_CD = new Vector();
	Vector TRD_PLANT_NM = new Vector();
	Vector TRD_PLANT_ABBR = new Vector();
	Vector TRD_PLANT_SEQ_NO = new Vector();
	Vector TRANS_CD = new Vector();
	Vector TRANS_PLANT_NM = new Vector();
	Vector TRANS_PLANT_ABBR = new Vector();
	Vector TRANS_PLANT_SEQ_NO = new Vector();
	Vector BU_CD = new Vector();
	Vector BU_PLANT_NM = new Vector();
	Vector BU_PLANT_ABBR = new Vector();
	Vector BU_PLANT_SEQ_NO = new Vector();
	Vector CD = new Vector();
	Vector PLANT_NM = new Vector();
	Vector PLANT_ABBR = new Vector();
	Vector PLANT_SEQ_NO = new Vector();
	
	public Vector getCOUNTRY_CODE() {return COUNTRY_CODE;}
	public Vector getCOUNTRY_NM() {return COUNTRY_NM;}
	public Vector getISO_CODE() {return ISO_CODE;}
	
	public Vector getTIN() {return TIN;}
	public Vector getSTATE_CODE() {return STATE_CODE;}
	public Vector getSTATE_NM() {return STATE_NM;}
	
	public Vector getCOUNTERPARTY_CD() {return COUNTERPARTY_CD;}
	public Vector getCOUNTERPARTY_NM() {return COUNTERPARTY_NM;}
	public Vector getCOUNTERPARTY_ABBR() {return COUNTERPARTY_ABBR;}
	public Vector getTRD_CD() {return TRD_CD;}
	public Vector getTRD_PLANT_NM() {return TRD_PLANT_NM;}
	public Vector getTRD_PLANT_ABBR() {return TRD_PLANT_ABBR;}
	public Vector getTRD_PLANT_SEQ_NO() {return TRD_PLANT_SEQ_NO;}
	public Vector getTRANS_CD() {return TRANS_CD;}
	public Vector getTRANS_PLANT_NM() {return TRANS_PLANT_NM;}
	public Vector getTRANS_PLANT_ABBR() {return TRANS_PLANT_ABBR;}
	public Vector getTRANS_PLANT_SEQ_NO() {return TRANS_PLANT_SEQ_NO;}
	public Vector getBU_CD() {return BU_CD;}
	public Vector getBU_PLANT_NM() {return BU_PLANT_NM;}
	public Vector getBU_PLANT_ABBR() {return BU_PLANT_ABBR;}
	public Vector getBU_PLANT_SEQ_NO() {return BU_PLANT_SEQ_NO;}
	public Vector getCD() {return CD;}
	public Vector getPLANT_NM() {return PLANT_NM;}
	public Vector getPLANT_ABBR() {return PLANT_ABBR;}
	public Vector getPLANT_SEQ_NO() {return PLANT_SEQ_NO;}
}
