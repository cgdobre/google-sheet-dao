/** 
 * @author cgdobre@gmail.com
 * 
 * with credits to the guy whose blog i had copied the inital version of the source code from but can not find any more.
 */

package ro.progsquad.google.sheet.dao;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.gdata.client.spreadsheet.FeedURLFactory;
import com.google.gdata.client.spreadsheet.ListQuery;
import com.google.gdata.client.spreadsheet.SpreadsheetService;
import com.google.gdata.data.spreadsheet.ListEntry;
import com.google.gdata.data.spreadsheet.ListFeed;
import com.google.gdata.data.spreadsheet.SpreadsheetEntry;
import com.google.gdata.data.spreadsheet.SpreadsheetFeed;
import com.google.gdata.data.spreadsheet.WorksheetEntry;
import com.google.gdata.util.ServiceException;

public class GoogleSheetDAO {

	/** Our view of Google Spreadsheets as an authenticated Google user. */
	private final SpreadsheetService service = new SpreadsheetService("google-sheet-dao");

	/** A factory that generates the appropriate feed URLs. */
	final private FeedURLFactory factory = FeedURLFactory.getDefault();

	/** The Spreadsheet we are accessing **/
	private WorksheetEntry worksheet = null;

	private GoogleSheetDAO() {
	}

	/** a cache of all already opened Spreadsheets **/
	private static Map<String, GoogleSheetDAO> worksheetDAOs = new HashMap<String, GoogleSheetDAO>();
	
	/**
	 * Logs in to Google and fins your worksheet. 
	 * Then returns a GoogleSheetDAO instance that allows you to manipulate the worksheet.
	 * 
	 * @param userName
	 * 			  OAuth Service account email address to authenticate (e.g. somecode@developer.gserviceaccount.com)
	 *            Get it from https://console.developers.google.com
	 * @param p12key
	 * 			  OAuth Service account p12 key of specified username
	 *            Get it from https://console.developers.google.com
	 * @param workbookName
	 * 			  The name of the Google Drive Workbook.
	 * 			  Make sure it is shared with the userName from the Google Drive interface!
	 * @param sheetName
	 * 			  The name of the Spreadsheet inside the specified Workbook
	 * 
	 * @return The DAO for your sheet
	 * 
	 * @throws IOException
	 * 			  when an error occurs in communication with the Google Spreadsheets service.
	 * @throws ServiceException
	 * 			  when the request causes an error in the Google Spreadsheets service.
	 * @throws GeneralSecurityException
	 * 			  when an authentication error occurs
	 */
	public static GoogleSheetDAO getInstance(final String userName, final File p12key, 
											 final String workbookName, final String sheetName) 
			throws IOException, ServiceException, GeneralSecurityException {
		
		if (worksheetDAOs.containsKey(workbookName + ":" + sheetName)) {
			return worksheetDAOs.get(sheetName);
		}

		GoogleSheetDAO newDao = new GoogleSheetDAO();
		newDao.login(userName, p12key);
		newDao.loadSheet(workbookName, sheetName);

		worksheetDAOs.put(workbookName + ":" + sheetName, newDao);
		return newDao;
	}
	
	/*
	 * Log in to Google, under a Google Spreadsheets account.
	 */
	private void login(final String username, final File p12key) throws GeneralSecurityException, IOException {
		// Authenticate
		final String[] SCOPESArray = {
				"https://spreadsheets.google.com/feeds",
				"https://spreadsheets.google.com/feeds/spreadsheets/private/full",
				"https://docs.google.com/feeds" 
				};
		final List<String> SCOPES = Arrays.asList(SCOPESArray);
		
		final GoogleCredential credential = new GoogleCredential.Builder()
				.setTransport(GoogleNetHttpTransport.newTrustedTransport())
				.setJsonFactory(new JacksonFactory())
				.setServiceAccountId(username).setServiceAccountScopes(SCOPES)
				.setServiceAccountPrivateKeyFromP12File(p12key).build();

		service.setOAuth2Credentials(credential);
	}

	/*
	 * Uses the user's credentials to get a list of spreadsheets. Then loads a sheet.
	 */
	private void loadSheet(final String sheetId, final String worksheetName) throws IOException, ServiceException {
		// list spreadsheets
		final SpreadsheetFeed feed = service.getFeed(factory.getSpreadsheetsFeedUrl(), SpreadsheetFeed.class);
		final List<SpreadsheetEntry> spreadsheets = feed.getEntries();

		if (spreadsheets.size() == 0) {
			throw new ServiceException("No Spreadsheets found. Did you share the Workbook with the OAuth Service account email ?");
		}

		// find spreadsheet by id
		for (SpreadsheetEntry sheet : spreadsheets) {
			if (StringUtils.equals(sheet.getTitle().getPlainText(), sheetId)) {
				final Iterator<WorksheetEntry> worksheetIterator = sheet.getWorksheets().iterator();
				while (worksheetIterator.hasNext()) {
					final WorksheetEntry worksheetEntry = worksheetIterator.next();
					if (StringUtils.equals(worksheetEntry.getTitle().getPlainText(), worksheetName)) {
						worksheet = worksheetEntry;
						break;
					}
				}
				break;
			}
		}

		if (worksheet == null) {
			throw new ServiceException("No sheet found with id " + sheetId + " and worksheet " + worksheetName
					+ ". Check your spelling and verify if you shared the Workbook with the OAuth Service account email.");
		}
	}

	/**
	 * Adds a new list entry.
	 * 
	 * @throws ServiceException
	 *             when the request causes an error in the Google Spreadsheets service.
	 * @throws IOException
	 *             when an error occurs in communication with the Google Spreadsheets service.
	 */
	public void addNewEntry(final Map<String, String> nameValuePairs) throws IOException, ServiceException {
		final ListEntry newEntry = new ListEntry();

		for (final String tag : nameValuePairs.keySet()) {
			newEntry.getCustomElements().setValueLocal(tag, nameValuePairs.get(tag));
		}

		service.insert(worksheet.getListFeedUrl(), newEntry);
	}

	/**
	 * Performs a full database-like query on the rows.
	 * 
	 * @param structuredQuery
	 *             a query like: name = "Bob" and phone != "555-1212"
	 * @throws ServiceException
	 *             when the request causes an error in the Google Spreadsheets service.
	 * @throws IOException
	 *             when an error occurs in communication with the Google Spreadsheets service.
	 */
	public List<ListEntry> query(String structuredQuery) throws IOException, ServiceException {
		ListQuery query = new ListQuery(worksheet.getListFeedUrl());
		query.setSpreadsheetQuery(structuredQuery);

		List<ListEntry> result = null;

		try {
			result = service.query(query, ListFeed.class).getEntries();
		} catch (Exception e) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			result = service.query(query, ListFeed.class).getEntries();
		}

		return result;
	}

	/**
	 * Updates an existing entry.
	 * 
	 * @param entry
	 *            the entry to update
	 * @param nameValuePairs
	 *            the name value pairs, such as "name=Rosa" to change the row's name field to Rosa
	 * @throws ServiceException
	 *             when the request causes an error in the Google Spreadsheets service.
	 * @throws IOException
	 *             when an error occurs in communication with the Google Spreadsheets service.
	 */
	public void update(final ListEntry entry, final Map<String, String> nameValuePairs) throws IOException, ServiceException {
		if (entry == null || nameValuePairs == null) {
			return;
		}

		for (final String tag : nameValuePairs.keySet()) {
			entry.getCustomElements().setValueLocal(tag, nameValuePairs.get(tag));
		}

		entry.update(); // This updates the existing entry.
	}
	
	public List<ListEntry> getRows() throws IOException, ServiceException {
		ListFeed feed = service.getFeed(worksheet.getListFeedUrl(), ListFeed.class);
		return feed.getEntries();
	}
}
