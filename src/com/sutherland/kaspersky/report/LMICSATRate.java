/**
 * 
 */
package com.sutherland.kaspersky.report;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import com.sutherland.helios.api.report.frontend.ReportFrontEndGroups;
import com.sutherland.helios.data.Aggregation;
import com.sutherland.helios.data.attributes.DataAttributes;
import com.sutherland.helios.data.formatting.NumberFormatter;
import com.sutherland.helios.data.granularity.user.UserGrains;
import com.sutherland.helios.data.units.DataUnits;
import com.sutherland.helios.database.connection.SQL.ConnectionFactory;
import com.sutherland.helios.database.connection.SQL.RemoteConnection;
import com.sutherland.helios.date.formatting.DateFormatter;
import com.sutherland.helios.date.parsing.DateParser;
import com.sutherland.helios.exceptions.DatabaseConnectionCreationException;
import com.sutherland.helios.exceptions.ExceptionFormatter;
import com.sutherland.helios.exceptions.ReportSetupException;
import com.sutherland.helios.logging.LogIDFactory;
import com.sutherland.helios.report.Report;
import com.sutherland.helios.report.parameters.groups.ReportParameterGroups;
import com.sutherland.helios.statistics.Statistics;
import com.sutherland.kaspersky.datasources.DatabaseConfigs;

/**
 * @author Jason Diamond
 *
 */
public final class LMICSATRate extends Report  implements DataAttributes
{
	private RemoteConnection dbConnection;
	private final String dbPropFile = DatabaseConfigs.KASP_DEV_DB;
	private KasperskyRoster roster;
	private final static Logger logger = Logger.getLogger(LMICSATRate.class);
	private final static GregorianCalendar SCORING_CHANGE_DATE = DateParser.convertSQLDateToGregorian("2015-01-29 11:30:00");
	
	public static String uiGetReportName()
	{
		return "LMI CSAT Rate";
	}
	
	public static String uiGetReportDesc()
	{
		return "CSAT rate for LMI surveys.";
	}
	
	public final static LinkedHashMap<String, String> uiSupportedReportFrontEnds = ReportFrontEndGroups.BASIC_METRIC_FRONTENDS;
	
	public final static LinkedHashMap<String, ArrayList<String>> uiReportParameters = ReportParameterGroups.BASIC_METRIC_REPORT_PARAMETERS;
	
	/**
	 * Build the report object.
	 * 
	 * @throws ReportSetupException		If a failure occurs during creation of the report or its resources.
	 */
	public LMICSATRate() throws ReportSetupException
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
			reportName = LMICSATRate.uiGetReportName();
			reportDesc = LMICSATRate.uiGetReportDesc();
			
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
		
		if(isTimeTrendReport())
		{
			retval.add("Date Grain");
		}
		else if(isStackReport())
		{
			retval.add("User Grain");
		}
		
		retval.add("CSAT Rate (%)");
		
		return retval;
	}

	/* (non-Javadoc)
	 * @see helios.Report#runReport(java.lang.String, java.lang.String)
	 */
	@Override
	protected ArrayList<String[]> loadData() throws Exception
	{
		
		ArrayList<String[]> retval = new ArrayList<String[]>();

		String query = "SELECT Date,Technician_ID,Q1,Q2,Q3,Q4 FROM LMI_10982630_Customer_Survey WHERE Date >= '" + 
				getParameters().getStartDate() + 
				"' AND Date < '" + 
				getParameters().getEndDate() + 
				"'";


		Aggregation reportData = new Aggregation();

		String tID, q1, q2, q3, q4, reportGrain;
		int maxPoints, surveyPoints;
		
		roster = new KasperskyRoster();
		roster.setChildReport(true);
		roster.getParameters().setAgentNames(getParameters().getAgentNames());
		roster.getParameters().setTeamNames(getParameters().getTeamNames());
		roster.load();
				
		//don't assign time grain just yet. in case this is a non-time report, because the timegrain param is not guaranteed to be set 
		int timeGrain, userGrain, dateFormat;
		double csatRate;
		GregorianCalendar surveyDate;
		
		for(String[] row:  dbConnection.runQuery(query))
		{
			maxPoints = 0;
			surveyPoints = 0;

			tID = row[1];
			if(roster.hasUser(tID) )
			{
				q1 = row[2];
				q2 = row[3];
				q3 = row[4];
				q4 = row[5];
				surveyDate = DateParser.convertSQLDateToGregorian(row[0]);

				if( !q1.equals("") )
				{
					surveyPoints += Integer.parseInt(q1);
					if(surveyDate.before(SCORING_CHANGE_DATE))
					{
						maxPoints += 10;
					}
					else
					{
						maxPoints += 5;
					}
				}

				if( !q2.equals("") )
				{
					surveyPoints += Integer.parseInt(q2);
					if(surveyDate.before(SCORING_CHANGE_DATE))
					{
						maxPoints += 10;
					}
					else
					{
						maxPoints += 5;
					}
				}

				if( !q3.equals("") )
				{
					surveyPoints += Integer.parseInt(q3);
					maxPoints += 10;
				}

				if( !q4.equals("") )
				{
					surveyPoints += Integer.parseInt(q4);
					if(surveyDate.before(SCORING_CHANGE_DATE))
					{
						maxPoints += 10;
					}
					else
					{
						maxPoints += 5;
					}
				}

				//throw out blank surveys
				csatRate = 0.0;
				if(maxPoints > 0)
				{
					csatRate = (double)surveyPoints/(double)maxPoints;
					
					if(isTimeTrendReport())
					{
						timeGrain = Integer.parseInt(getParameters().getTimeGrain());
						dateFormat = Integer.parseInt(getParameters().getDateFormat());
						reportGrain = DateFormatter.getFormattedDate(surveyDate, timeGrain, dateFormat);
					}
					else //if(isStackReport())
					{
						userGrain = Integer.parseInt(getParameters().getUserGrain());
						reportGrain = UserGrains.getUserGrain(userGrain, roster.getUser(tID));
					}
					
					reportData.addDatum(reportGrain);
					reportData.getDatum(reportGrain).addAttribute(SAT_SURVEYS_ATTR);
					reportData.getDatum(reportGrain).addData(SAT_SURVEYS_ATTR, csatRate);
				}
			}
		}
		
		for( Entry<String, String> queryStats  : dbConnection.getStatistics().entrySet())
		{
			logInfoMessage( "Query " + queryStats.getKey() + ": " + queryStats.getValue());
		}

		double finalCSATRate;
		for(String grain : reportData.getDatumIDList())
		{
			finalCSATRate = Statistics.getAverage(reportData.getDatum(grain).getAttributeData(SAT_SURVEYS_ATTR));
			retval.add(new String[]{grain, "" + NumberFormatter.convertToPercentage(finalCSATRate, 4) });
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

	@Override
	public String getUnits() 
	{
		return DataUnits.CSAT_RATE;
	}
}
