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
import com.sutherland.kaspersky.datasources.DatabaseConfigs;

/**
 * @author Jason Diamond
 *
 */
public final class NetPromoterScore extends Report  implements DataAttributes
{
	private RemoteConnection dbConnection;
	private final String dbPropFile = DatabaseConfigs.KASP_DEV_DB;
	private KasperskyRoster roster;
	private final static Logger logger = Logger.getLogger(NetPromoterScore.class);
	private final static String NET_PROM_ATTR = "netPromoter";
	private final static String NON_NET_PROM_ATTR = "nonNetProm";
	private final static String VIABLE_SURVEYS_ATTR = "viableSurveys";
	
	public static String uiGetReportName()
	{
		return "Net Promoter Score";
	}
	
	public static String uiGetReportDesc()
	{
		return "How likely a customer would recommend the client.";
	}
	
	public final static LinkedHashMap<String, String> uiSupportedReportFrontEnds = ReportFrontEndGroups.BASIC_METRIC_FRONTENDS;
	
	public final static LinkedHashMap<String, ArrayList<String>> uiReportParameters = ReportParameterGroups.BASIC_METRIC_REPORT_PARAMETERS;
	
	/**
	 * Build the report object.
	 * 
	 * @throws ReportSetupException		If a failure occurs during creation of the report or its resources.
	 */
	public NetPromoterScore() throws ReportSetupException
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
			reportName = NetPromoterScore.uiGetReportName();
			reportDesc = NetPromoterScore.uiGetReportDesc();
			
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
		
		retval.add("Net Promoter Score (%)");
		
		return retval;
	}

	/* (non-Javadoc)
	 * @see helios.Report#runReport(java.lang.String, java.lang.String)
	 */
	@Override
	protected ArrayList<String[]> loadData() throws Exception
	{
		
		ArrayList<String[]> retval = new ArrayList<String[]>();

		String query = "SELECT Date,Technician_ID,Session_ID,Q3 FROM LMI_10982630_Customer_Survey WHERE Date >= '" + 
				getParameters().getStartDate() + 
				"' AND Date < '" + 
				getParameters().getEndDate() + 
				"'";


		Aggregation reportData = new Aggregation();

		String tID, q3, reportGrain;
		int surveyAnswer;
		
		roster = new KasperskyRoster();
		roster.setChildReport(true);
		roster.getParameters().setAgentNames(getParameters().getAgentNames());
		roster.getParameters().setTeamNames(getParameters().getTeamNames());
		roster.load();
				
		//don't assign time grain just yet. in case this is a non-time report, because the timegrain param is not guaranteed to be set 
		int timeGrain, userGrain, dateFormat;
		
		for(String[] row:  dbConnection.runQuery(query))
		{
			tID = row[1];
			if(roster.hasUser(tID) )
			{
				q3 = row[3];

				if( !q3.equals("") )
				{
					surveyAnswer = Integer.parseInt(q3);
					
					if(isTimeTrendReport())
					{
						timeGrain = Integer.parseInt(getParameters().getTimeGrain());
						dateFormat = Integer.parseInt(getParameters().getDateFormat());
						reportGrain = DateFormatter.getFormattedDate( DateParser.convertSQLDateToGregorian(row[0]), timeGrain, dateFormat);
					}
					else //if(isStackReport())
					{
						userGrain = Integer.parseInt(getParameters().getUserGrain());
						reportGrain = UserGrains.getUserGrain(userGrain, roster.getUser(tID));
					}
					
					reportData.addDatum(reportGrain);
					reportData.getDatum(reportGrain).addAttribute(VIABLE_SURVEYS_ATTR);
					reportData.getDatum(reportGrain).addAttribute(NET_PROM_ATTR);
					reportData.getDatum(reportGrain).addAttribute(NON_NET_PROM_ATTR);
					
					reportData.getDatum(reportGrain).addData(VIABLE_SURVEYS_ATTR, row[2]);
					
					//NPS is calculated by (% of promoters (9-10) - % of detractors (0-6)).
					
					if(surveyAnswer == 9 || surveyAnswer == 10)
					{
						reportData.getDatum(reportGrain).addData(NET_PROM_ATTR, row[2]);
					}
					else if(surveyAnswer <= 6)
					{
						reportData.getDatum(reportGrain).addData(NON_NET_PROM_ATTR, row[2]);
					}
				}
			}
		}
		
		for( Entry<String, String> queryStats  : dbConnection.getStatistics().entrySet())
		{
			logInfoMessage( "Query " + queryStats.getKey() + ": " + queryStats.getValue());
		}

		double nps, surveyCount, promotedSurveys, nonPromotedSurveys;
		for(String grain : reportData.getDatumIDList())
		{
			surveyCount = reportData.getDatum(grain).getAttributeData(VIABLE_SURVEYS_ATTR).size();
			
			if(surveyCount > 0)
			{
				//a grain could contain zero surveys
				if(reportData.getDatum(grain).getAttributeNameList().contains(NET_PROM_ATTR))
				{
					promotedSurveys = reportData.getDatum(grain).getAttributeData(NET_PROM_ATTR).size();
				}
				else
				{
					promotedSurveys = 0;
				}

				if(reportData.getDatum(grain).getAttributeNameList().contains(NON_NET_PROM_ATTR))
				{
					nonPromotedSurveys = reportData.getDatum(grain).getAttributeData(NON_NET_PROM_ATTR).size();
				}
				else
				{
					nonPromotedSurveys = 0;
				}
				
				nps = (promotedSurveys/surveyCount) - (nonPromotedSurveys/surveyCount);
				retval.add(new String[]{grain, "" + NumberFormatter.convertToPercentage(nps, 4) });
			}
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
		return DataUnits.NET_PROMOTER_SCORE;
	}
}
