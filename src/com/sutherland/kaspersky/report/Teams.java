/**
 * 
 */
package com.sutherland.kaspersky.report;


import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import com.sutherland.helios.api.report.frontend.ReportFrontEndGroups;
import com.sutherland.helios.database.connection.DatabaseConnection;
import com.sutherland.helios.database.connection.SQL.ConnectionFactory;
import com.sutherland.helios.exceptions.DatabaseConnectionCreationException;
import com.sutherland.helios.exceptions.ExceptionFormatter;
import com.sutherland.helios.exceptions.ReportSetupException;
import com.sutherland.helios.logging.LogIDFactory;
import com.sutherland.helios.report.Report;
import com.sutherland.helios.report.parameters.groups.ReportParameterGroups;
import com.sutherland.kaspersky.datasources.DatabaseConfigs;


/**
 * The list of teams within the Hughes support desk
 * 
 * @author Jason Diamond
 *
 */
public final class Teams extends Report 
{	
	private KasperskyRoster roster;
	private DatabaseConnection dbConnection;
	private final String dbPropFile = DatabaseConfigs.KASP_DEV_DB;
	private final static Logger logger = Logger.getLogger(Teams.class);

	public static String uiGetReportName()
	{
		return "Teams";
	}
	
	public static String uiGetReportDesc()
	{
		return "A list of teams in the roster.";
	}
	
	public final static LinkedHashMap<String, String> uiSupportedReportFrontEnds = ReportFrontEndGroups.STACK_RANK_FRONTENDS;
	
	public final static LinkedHashMap<String, ArrayList<String>> uiReportParameters = ReportParameterGroups.ROSTER_REPORT_PARAMETERS;
	
	/**
	 * Build the Roster report.
	 *
	 * @throws ReportSetupException	If a connection to the database could not be established.
	 */
	public Teams() throws ReportSetupException 
	{
		super();
	}
	
	/* (non-Javadoc)
	 * @see helios.Report#setupReport()
	 */
	@Override
	protected boolean setupReport() 
	{
		boolean retval = false;

		try
		{
			reportName = Teams.uiGetReportName();
			reportDesc = Teams.uiGetReportDesc();
			
			for(Entry<String, ArrayList<String>> reportType : uiReportParameters.entrySet())
			{
				for(String paramName :  reportType.getValue())
				{
					getParameters().addSupportedParameter(paramName);
				}
			}
			
			retval = true;
		}
		catch (Exception e)
		{
			setErrorMessage("Error setting up report");
			
			logErrorMessage(getErrorMessage());
			logErrorMessage( ExceptionFormatter.asString(e));
		}

		return retval;
	}
	
	@Override
	protected boolean setupLogger() 
	{
		logID = LogIDFactory.getLogID().toString();

		if (MDC.get(LOG_ID_PREFIX) == null) 
		{
			MDC.put(LOG_ID_PREFIX, LOG_ID_PREFIX + logID);
		}

		return (logger != null);
	}

	/** 
	 * Attempt to establish connections to all required datasources. A report by definition has at least one, and possibly many.
	 * 
	 * @return	True if the connection was established, false otherwise.
	 */
	protected boolean setupDataSourceConnections()
	{
		boolean retval = false;

		try 
		{
			ConnectionFactory factory = new ConnectionFactory();
			
			factory.load(dbPropFile);
			
			dbConnection = factory.getConnection();
		}
		catch (DatabaseConnectionCreationException e) 
		{
			setErrorMessage("DatabaseConnectionCreationException on attempt to access database");
			
			logErrorMessage(getErrorMessage());
			logErrorMessage( ExceptionFormatter.asString(e));
		} 
		finally
		{
			if(dbConnection != null)
			{
				retval = true;
			}
		}

		return retval;
	}

	/**
	 * Close the report, any sub reports, and any database connections.
	 * 
	 * @see report.Report#close()
	 */
	@Override
	public void close()
	{
		if(roster != null)
		{
			roster.close();
		}

		if(dbConnection != null)
		{
			dbConnection.close();
		}
		
		super.close();
		
		if (!isChildReport) 
		{
			MDC.getContext().clear();
		}
	}
	
	@Override
	public ArrayList<String> getReportSchema() 
	{
		ArrayList<String> retval = new ArrayList<String>();

		retval.add("Team");

		return retval;
	}

	@Override
	protected ArrayList<String[]> loadData() throws Exception 
	{
		ArrayList<String[]> retval = new ArrayList<String[]>();

		for(String[] row : dbConnection.runQuery("Select distinct team from lmi_kaspersky_roster where parent_id = '10982630' "))
		{
			retval.add(row);
		}

		return retval;
	}

	/* (non-Javadoc)
	 * @see helios.Report#validateParameters()
	 */
	@Override
	public boolean validateParameters() 
	{
		return true;
	}
	
	@Override
	protected void logErrorMessage(String message) 
	{
		logger.log(Level.ERROR, message);
	}

	@Override
	protected void logInfoMessage(String message) 
	{
		logger.log(Level.INFO, message);
	}

	@Override
	protected void logWarnMessage(String message) 
	{
		logger.log(Level.WARN, message);
	}

	@Override
	public String getUnits() 
	{
		return null;
	}
}
