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
import com.sutherland.helios.data.formatting.NumberFormatter;
import com.sutherland.helios.database.connection.SQL.ConnectionFactory;
import com.sutherland.helios.database.connection.SQL.RemoteConnection;
import com.sutherland.helios.date.parsing.DateParser;
import com.sutherland.helios.exceptions.DatabaseConnectionCreationException;
import com.sutherland.helios.exceptions.ExceptionFormatter;
import com.sutherland.helios.exceptions.ReportSetupException;
import com.sutherland.helios.logging.LogIDFactory;
import com.sutherland.helios.report.Report;
import com.sutherland.helios.report.parameters.groups.ReportParameterGroups;
import com.sutherland.kaspersky.datasources.DatabaseConfigs;

/**
 * @author Jason Diamond
 *
 */
public final class LMIDSATCases extends Report 
{
	private RemoteConnection dbConnection;

	private final String dbPropFile = DatabaseConfigs.KASP_DEV_DB;
	private KasperskyRoster roster;
	private final static Logger logger = Logger.getLogger(LMIDSATCases.class);

	public static String uiGetReportName()
	{
		return "LMI DSAT Cases";
	}

	public static String uiGetReportDesc()
	{
		return "Customer dissatisfaction survey details for LMI surveys.";
	}

	public final static LinkedHashMap<String, String> uiSupportedReportFrontEnds = ReportFrontEndGroups.STACK_RANK_FRONTENDS;

	public final static LinkedHashMap<String, ArrayList<String>> uiReportParameters = ReportParameterGroups.DASHBOARD_REPORT_PARAMETERS;

	/**
	 * Build the report object.
	 * 
	 * @throws ReportSetupException		If a failure occurs during creation of the report or its resources.
	 */
	public LMIDSATCases() throws ReportSetupException 
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
			reportName = LMIDSATCases.uiGetReportName();
			reportDesc = LMIDSATCases.uiGetReportDesc(); 

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

	/* (non-Javadoc)
	 * @see helios.Report#setupDataSourceConnections()
	 */
	@Override
	protected boolean setupDataSourceConnections()
	{
		boolean retval = false;

		try 
		{
			ConnectionFactory factory = new ConnectionFactory();

			factory.load(dbPropFile);

			dbConnection = factory.getConnection();
		}
		catch(DatabaseConnectionCreationException e )
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

	/* (non-Javadoc)
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
			MDC.remove(LOG_ID_PREFIX);
		}
	}

	@Override
	public ArrayList<String> getReportSchema() 
	{
		ArrayList<String> retval = new ArrayList<String>();

		retval.add("Date Grain");
		retval.add("SessionID");
		retval.add("Customer");
		retval.add("Technician");
		retval.add("Q1");
		retval.add("Q2");
		retval.add("Q3");
		retval.add("Q4");
		retval.add("Q5");
		retval.add("CSAT (%)");


		return retval;
	}

	/* (non-Javadoc)
	 * @see helios.Report#runReport(java.lang.String, java.lang.String)
	 */
	@Override
	protected ArrayList<String[]> runReport() throws Exception
	{
		ArrayList<String[]> retval = new ArrayList<String[]>();

		String query = "SELECT Date,Session_ID,Customer_Name,Technician_Name,Technician_ID,Q1,Q2,Q3,Q4,Q5 FROM LMI_10982630_Customer_Survey WHERE Date >= '" + 
				getParameters().getStartDate() + 
				"' AND Date < '" + 
				getParameters().getEndDate() + 
				"'";

		roster = new KasperskyRoster();
		roster.setChildReport(true);
		roster.getParameters().setAgentNames(getParameters().getAgentNames());
		roster.getParameters().setTeamNames(getParameters().getTeamNames());
		roster.load();

		String tID, q1, q2, q3, q4, q5;
		int maxPoints, surveyPoints;
		for(String[] row:  dbConnection.runQuery(query))
		{
			maxPoints = 0;
			surveyPoints = 0;
			
			tID = row[4];
			if(roster.hasUser(tID) )
			{
				//questions can be blank, surveys can be blank
				
				//for each q, if the value is defined add the normalized values
				

				
				q1 = row[5];
				q2 = row[6];
				q3 = row[7];
				q4 = row[8];
				q5 = row[9];
				
				if( !q1.equals("") )
				{
					surveyPoints += Integer.parseInt(q1);
					maxPoints += 10;
				}
				
				if( !q2.equals("") )
				{
					surveyPoints += Integer.parseInt(q2);
					maxPoints += 10;
				}
				
				if( !q3.equals("") )
				{
					surveyPoints += Integer.parseInt(q3);
					maxPoints += 10;
				}
				
				if( !q4.equals("") )
				{
					surveyPoints += Integer.parseInt(q4);
					maxPoints += 10;
				}
				
				if( !q5.equals("") )
				{
					surveyPoints += Integer.parseInt(q5);
					maxPoints += 10;
				}
				
				//throw out blank surveys
				if(maxPoints > 0)
				{
					double csat = (double)surveyPoints/(double)maxPoints;
					
					if( csat < .85)
					{
						//add row if dsat < 85%
						
						int dateFormat = Integer.parseInt(getParameters().getDateFormat());
						String creationDate = DateParser.convertToString(DateParser.convertSQLDateToGregorian(row[0]), dateFormat );	
						
						
						retval.add
						(
							new String[]
							{
									creationDate,
									row[1],
									row[2],
									row[3],
									q1,
									q2,
									q3,
									q4,
									q5,
									"" + NumberFormatter.convertToPercentage(csat, 4)
							}
						);
					}
				}
			}
		}

		for( Entry<String, String> queryStats  : dbConnection.getStatistics().entrySet())
		{
			logInfoMessage( "Query " + queryStats.getKey() + ": " + queryStats.getValue());
		}

		return retval;
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
}
