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
import com.sutherland.helios.data.Datum;
import com.sutherland.helios.database.connection.SQL.ConnectionFactory;
import com.sutherland.helios.exceptions.DatabaseConnectionCreationException;
import com.sutherland.helios.exceptions.ExceptionFormatter;
import com.sutherland.helios.exceptions.ReportSetupException;
import com.sutherland.helios.logging.LogIDFactory;
import com.sutherland.helios.report.parameters.groups.ReportParameterGroups;
import com.sutherland.helios.roster.Roster;
import com.sutherland.kaspersky.datasources.DatabaseConfigs;
import com.sutherland.kaspersky.report.roster.Attributes;


/**
 * The roster containing the agents for whom we care about performance. This backend is not only used by the Roster report, but is used by other reports requiring 
 * an end-all list of users to report on. AgentName to CMS Name mappings are also loaded and can be referenced by implementing reports without additional database queries.
 * 
 * 
 * @author Jason Diamond
 *
 */
public final class KasperskyRoster extends Roster implements Attributes
{
	
	private final String dbPropFile = DatabaseConfigs.KASP_DEV_DB;

	private final static String PROGRAM_NAME = "Kaspersky";

	private final static String ORGUNIT_NAME = "CAN01";
	
	private final static Logger logger = Logger.getLogger(KasperskyRoster.class);

	public static String uiGetReportName()
	{
		return "Kaspersky Roster";
	}
	
	public static String uiGetReportDesc()
	{
		return "A list of users in the roster.";
	}
	
	public final static LinkedHashMap<String, String> uiSupportedReportFrontEnds = ReportFrontEndGroups.STACK_RANK_FRONTENDS;
	
	public final static LinkedHashMap<String, ArrayList<String>> uiReportParameters = ReportParameterGroups.ROSTER_REPORT_PARAMETERS;
	
	/**
	 * Build the Roster report.
	 *
	 * @throws ReportSetupException	If a connection to the database could not be established.
	 */
	public KasperskyRoster() throws ReportSetupException 
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
			reportName = KasperskyRoster.uiGetReportName();
			reportDesc = KasperskyRoster.uiGetReportDesc();
			
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
			
			logErrorMessage( getErrorMessage());
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
		catch(DatabaseConnectionCreationException e )
		{
			setErrorMessage("DatabaseConnectionCreationException on attempt to access database");
					
			logErrorMessage( getErrorMessage());
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
		if(dbConnection != null)
		{
			dbConnection.close();
		}

		super.close();
		
		if (!isChildReport) 
		{
			//MDC.getContext().clear();
			MDC.remove(LOG_ID_PREFIX);
		}
	}
	
	@Override
	public ArrayList<String> getReportSchema() 
	{
		ArrayList<String> retval = new ArrayList<String>();
		
		retval.add("Node ID");
		retval.add("Parent ID");
		retval.add("Team");
		retval.add("Name");
		retval.add("Email");
		retval.add("Description");
		retval.add("Status");
		retval.add("Type");
		
		return retval;
	}

	/**
	 * Build the roster from the database. Also build the PARAM -> USER mappings for other reports to reference.
	 * 
	 */
	public void load()
	{
		logInfoMessage("Loading Roster");

		clearUsers();

		String nodeID;
		String parentID;
		String name;
		String team;
		String email;
		String description;
		String status;
		String type;
		String fullName;

		Datum newUser = null;
		
		String query = 	"SELECT NODE_ID,PARENT_ID,TEAM,NAME,EMAIL,DESCRIPTION,STATUS,TYPE FROM LMI_KASPERSKY_ROSTER where parent_id = '10982630' and type != 'Administrator' and type != 'AdministratorLink'  and type != 'MasterAdministrator' ";
		
		for(String[] row:  dbConnection.runQuery(query))
		{
			try
			{					
				nodeID = row[0].trim();
				parentID = row[1].trim();
				name = row[3].trim();
				team = row[2].trim();
				email = row[4].trim();
				description = row[5].trim();
				status = row[6].trim();
				type = row[7].trim();
				
				fullName = name;
				
				type = row[6].trim();

				if( !hasUser(nodeID) ) 
				{
					newUser = new Datum(nodeID);

					newUser.addAttribute(NODE_ID_ATTR);
					newUser.addAttribute(PARENT_ID_ATTR);
					newUser.addAttribute(TEAMNAME_ATTR);
					newUser.addAttribute(NAME_ATTR);
					newUser.addAttribute(STATUS_ATTR); 
					newUser.addAttribute(TYPE_ATTR);
					newUser.addAttribute(EMAIL_ATTR);
					newUser.addAttribute(FULLNAME_ATTR);
					newUser.addAttribute(ACTIVE_ATTR);
					newUser.addAttribute(PROGRAMNAME_ATTR);
					newUser.addAttribute(ORGUNIT_ATTR);
					
					newUser.setAttributeAsUnique(NODE_ID_ATTR);

					newUser.addData(NODE_ID_ATTR, nodeID);
					newUser.addData(PARENT_ID_ATTR, parentID);
					newUser.addData(NAME_ATTR, name);
					newUser.addData(TEAMNAME_ATTR, team);
					newUser.addData(EMAIL_ATTR, email);
					newUser.addData(DESCRIPTION_ATTR, description);
					newUser.addData(STATUS_ATTR, status);
					newUser.addData(TYPE_ATTR, type);
					
					
					newUser.addData(FULLNAME_ATTR, fullName);
					newUser.addData(PROGRAMNAME_ATTR, PROGRAM_NAME);
					newUser.addData(ORGUNIT_ATTR, ORGUNIT_NAME);
					
					if(shouldIncludeUser(newUser) || includeAllUsers)
					{
						addUser(nodeID, newUser);
					}
				}
			}
			catch(NullPointerException e)
			{
				logErrorMessage(  "Error adding user for line beginning with " + row[0]);
				logErrorMessage( ExceptionFormatter.asString(e));
			}
		}
		
		for( Entry<String, String> queryEntry : dbConnection.getStatistics().entrySet())
		{
			logInfoMessage( "Query: " + queryEntry.getKey() + " => " + queryEntry.getValue());
		}
		
		logInfoMessage( "Loaded " + getSize() + " users into roster");
	}
	
	/**
	 * Accessor for a specified User's name, for human readability.
	 * 
	 * @param userID	String to query the Users by. Can be Employee ID or User ID.
	 * 
	 * @return	The User discovered.
	 */
	public String getFullName(String userID)
	{
		Datum user = getUser(userID);

		String fullName = null;

		try
		{
			fullName = user.getAttributeData(NAME_ATTR).get(0);
		}
		catch(NullPointerException e)
		{
			logErrorMessage( "Could not determine full name for parameter: " + userID);
		}

		return fullName;
	}

	/**
	 * Convert the userlist into something more user-readable.
	 * 
	 * @return	The roster. 
	 * @throws Exception 
	 * 
	 * @see report.Report#runReport()
	 */
	@Override
	protected ArrayList<String[]> runReport() throws Exception 
	{
		clearUsers();
		load();

		ArrayList<String[]> retval = new ArrayList<String[]>();
		Datum thisUser;
		for(String userID : getUserIDs())
		{
			thisUser = getUser(userID);
			
			retval.add
			(
					new String[]
					{
							thisUser.getAttributeData(NODE_ID_ATTR).get(0),
							thisUser.getAttributeData(PARENT_ID_ATTR).get(0),
							thisUser.getAttributeData(TEAMNAME_ATTR).get(0),
							thisUser.getAttributeData(NAME_ATTR).get(0),
							thisUser.getAttributeData(EMAIL_ATTR).get(0),
							thisUser.getAttributeData(DESCRIPTION_ATTR).get(0),
							thisUser.getAttributeData(STATUS_ATTR).get(0),
							thisUser.getAttributeData(TYPE_ATTR).get(0),
					}
			);
		}
				
		return retval;
	}

	@Override
	public boolean isActiveUser(String userID) 
	{
		boolean retval = false;
		
		try
		{
			retval = getUser(userID).getAttributeData(STATUS_ATTR).get(0).equalsIgnoreCase("disabled");
		}
		catch(Exception e)
		{}
		
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
