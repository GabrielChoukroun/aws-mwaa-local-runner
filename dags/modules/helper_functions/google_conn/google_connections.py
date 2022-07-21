import base64
import json
import mimetypes
import os
import pickle
import socket
import time
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
# from cns_recon import GAUTH_CREDS, LOGGER, GDAUTH_CREDS
# from cns_recon.helper_funcs.slack_wrapper import SlackBot
from googleapiclient import discovery
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaInMemoryUpload
from airflow.models import Variable

# ALERTS = SlackBot()
GAUTH_CREDS = None
gpickle = 'ctoken.pickle'

class GoogleSheets():

    def __init__(self, spreadsheetId):
        """
        Initialize a Google Sheets Service object

        Parameters
        ------------------------
        spreadsheetId: str
            Unique spreadsheet identifier
        GAUTH: Google OAuth2 Credentials object
            Needed to authorize Sheets API requests
        """
        GAUTH_CREDS = pickle.loads(base64.b64decode(Variable.get(gpickle)))

        self.spreadsheetId = spreadsheetId
        self.service = discovery.build('sheets', 'v4', credentials=GAUTH_CREDS, cache_discovery=False)
        self.retries = 0

    def alertChannel(self, e):
        """
        Send an alert for any failed executions

        Parameters
        ------------------------
        e: Error
        """
        # ALERTS.postMessage(f"""
        # Project: `Recon` ```Module: Google Sheets \nError: {e} \nPlease check Logs for more information.```""")
        print(f"""
        Project: `Recon` ```Module: Google Sheets \nError: {e} \nPlease check Logs for more information.```""")

    def append(self, range, data, majorDimension="ROWS", valueInputOption="USER_ENTERED"):
        """
        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/append

        Parameters
        ------------------------
        ranges: str
            "Example!A1:Z1"
        data: list
            [["0"], ["1"]...]
        majorDimensions: str
            "ROWS" (default) / "COLUMNS"
        valueInputOption: str
            "USER_ENTERED"(default) / "RAW"
        """
        try:
            response = self.service.spreadsheets().values().append(
                spreadsheetId = self.spreadsheetId,
                range = range,
                body = {
                    "majorDimension": majorDimension,
                    "values": data
                },
                valueInputOption = valueInputOption
            ).execute()
        
        except HttpError as e:
            # LOGGER.error(e)
            print(e)
            self.alertChannel(e)


    def batchGet(self, ranges, valueRenderOption="UNFORMATTED_VALUE"):
        """
        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/batchGet

        Parameters
        ------------------------
        ranges: list
            ["Example!A1:Z1", ...]
        valueRenderOption: str
            "FORMATTED_VALUE"(default) / "UNFORMATTED_VALUE" / "FORMULA"
        """
        try:
            response = self.service.spreadsheets().values().batchGet(
                spreadsheetId = self.spreadsheetId,
                ranges = ranges,
                valueRenderOption = valueRenderOption
            ).execute()

            return response.get('valueRanges', [])
        except (HttpError, socket.timeout) as e:
            # LOGGER.error(e)
            print(e)
            if self.retries<5:
                # LOGGER.warning("Read operation for batchGet timed out, retrying...")
                print("Read operation for batchGet timed out, retrying...")
                self.retries+=1
                time.sleep(60 * (self.retries / 2))
                return self.batchGet(ranges, valueRenderOption)
            else:
                self.retries=0
                self.alertChannel("BatchGet method resulted in read timeout error after 5 retries.")
                raise e

    def batchUpdate(self, data):
        """
        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/batchUpdate

        Parameters
        ------------------------
        requestBody: dict
            requestBody
        """
        try:
            response = self.service.spreadsheets().values().batchUpdate(
                spreadsheetId = self.spreadsheetId,
                body = data
            ).execute()

        except (HttpError, socket.timeout) as e:
            # LOGGER.error(e)
            print(e)
            if self.retries<5:
                # LOGGER.warning("Read operation for BatchUpdate timed out, retrying...")
                print("Read operation for BatchUpdate timed out, retrying...")
                self.retries+=1
                time.sleep(60 * (self.retries / 2))
                return self.batchUpdate(data)
            else:
                self.retries=0
                self.alertChannel("BatchUpdate method resulted in read timeout error after 5 retries.")
                raise e

    def batchSheetUpdate(self, data):
        """
        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/batchUpdate

        Parameters
        ------------------------
        requestBody: dict
            requestBody
        """
        response = self.service.spreadsheets().batchUpdate(
            spreadsheetId = self.spreadsheetId,
            body = data
        ).execute()

    def batchClear(self, range):
        """
        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/batchClear

        Parameters
        ------------------------
        range: list
            ["Example!A1:Z1", ...]
        """
        try:
            response = self.service.spreadsheets().values().batchClear(
                spreadsheetId = self.spreadsheetId,
                body = {
                    'ranges': range
                }
            ).execute()

        except (HttpError, socket.timeout) as e:
            # LOGGER.error(e)
            print(e)
            if self.retries<5:
                # LOGGER.warning("Read operation for batchClear timed out, retrying...")
                print("Read operation for batchClear timed out, retrying...")
                self.retries+=1
                time.sleep(60 * (self.retries / 2))
                return self.batchClear(range)
            else:
                self.retries=0
                self.alertChannel("BatchClear method resulted in read timeout error after 5 retries.")
                raise e

    def batchGetSheetId(self):
        """
        Get Sheet IDs for all sheets

        Returns
        ------------------------
        Pandas DataFrame
        """
        try:
            response = self.service.spreadsheets().get(
                spreadsheetId = self.spreadsheetId,
                fields = "sheets(properties(sheetId,title))"
            ).execute()
            return pd.DataFrame(response['sheets'])['properties'].apply(pd.Series)
        except (HttpError, socket.timeout) as e:
            # LOGGER.error(e)
            print(e)
            if self.retries<5:
                # LOGGER.warning("Read operation for Get Id timed out, retrying...")
                print("Read operation for Get Id timed out, retrying...")
                self.retries+=1
                time.sleep(60 * (self.retries / 2))
                return self.batchGetSheetId()
            else:
                self.retries=0
                self.alertChannel("Get Id method resulted in read timeout error after 5 retries.")
                raise e

    def addNewSheet(self, sheetName):
        """
        Add new sheet to existing spreadsheet
        
        Parameters
        ------------------------
        sheetName: str
            "Name of the sheet"
        """
        try:
            self.batchSheetUpdate(
                data = {
                    'requests': [
                        {
                            'addSheet':{
                                'properties':{
                                    'title': f'{sheetName}'
                                }
                            } 
                        }
                    ]
                }
            )
        except (HttpError, socket.timeout) as e:
            # LOGGER.error(e)
            print(e)
            if type(e)==HttpError:
                if 'already exists' in json.loads(e.args[1].decode())['error']['message']:
                    return None

            if self.retries<5:
                # LOGGER.warning("Read operation for AddNewSheet timed out, retrying...")
                print("Read operation for AddNewSheet timed out, retrying...")
                self.retries+=1
                time.sleep(60 * (self.retries / 2))
                return self.addNewSheet(sheetName)
            else:
                self.retries=0
                self.alertChannel("AddNewSheet method resulted in read timeout error after 5 retries.")
                raise e

    def alterDimensions(self, sheetName, dimension, startIndex, endIndex):
        """
        Delete {dimension} [ROWS/COLUMNS] from specified {sheetName}
        
        Parameters
        ------------------------
        startIndex: int
            index of starting dimension
        endIndex: int
            index of ending dimension
        """
        try:
            sheetIds = self.batchGetSheetId()

            data = {
                "requests": [
                    {
                        "deleteDimension": {
                            "range": {
                                "sheetId": int(sheetIds.loc[sheetIds.title == sheetName]['sheetId'].values[0]),
                                "dimension": dimension,
                                "startIndex": startIndex,
                                "endIndex": endIndex
                            }
                        }
                    }
                ]
            }
            self.batchSheetUpdate(data)
        except (HttpError, socket.timeout) as e:
            # LOGGER.error(e)
            print(e)
            if self.retries<5:
                # LOGGER.warning("Encountered Error during alterDimensions, retrying...")
                print("Encountered Error during alterDimensions, retrying...")
                self.retries+=1
                time.sleep(60 * (self.retries / 2))
                return self.alterDimensions(sheetName, dimension, startIndex, endIndex)
            else:
                self.retries=0
                self.alertChannel("alterDimensions method resulted in error after 5 retries.")
                raise e

    def duplicateSheet(self, sourceName, destinationName, s_startRowIndex=0, s_endRowIndex=1000, s_startColIndex=0, s_endColIndex=50, d_startRowIndex=0, d_endRowIndex=1000, d_startColIndex=0, d_endColIndex=50):
        try:
            self.addNewSheet(destinationName)
            
            sheetIds = self.batchGetSheetId()
            
            data = {
                "requests": [
                    {
                        "copyPaste": {
                            "source": {
                                "sheetId": int(sheetIds.loc[sheetIds.title == sourceName]['sheetId'].values[0]),
                                "startRowIndex": s_startRowIndex,
                                "endRowIndex": s_endRowIndex,
                                "startColumnIndex": s_startColIndex,
                                "endColumnIndex": s_endColIndex
                            },
                            "destination": {
                                "sheetId": int(sheetIds.loc[sheetIds.title == destinationName]['sheetId'].values[0]),
                                "startRowIndex": d_startRowIndex,
                                "endRowIndex": d_endRowIndex,
                                "startColumnIndex": d_startColIndex,
                                "endColumnIndex": d_endColIndex
                            },
                            "pasteType": "PASTE_NORMAL",
                            "pasteOrientation": "NORMAL"
                        }
                    }
                ]
            }
            self.batchSheetUpdate(data)
            return int(sheetIds.loc[sheetIds.title == destinationName]['sheetId'].values[0])
        except (HttpError, socket.timeout) as e:
            # LOGGER.error(e)
            print(e)
            if self.retries<5:
                # LOGGER.warning("Read operation for duplicateSheet timed out, retrying...")
                print("Read operation for duplicateSheet timed out, retrying...")

                self.retries+=1
                time.sleep(60 * (self.retries / 2))
                return self.duplicateSheet(sourceName, destinationName, s_startRowIndex, s_endRowIndex, s_startColIndex, s_endColIndex, d_startRowIndex, d_endRowIndex, d_startColIndex, d_endColIndex)
            else:
                self.retries=0
                self.alertChannel("duplicateSheet method resulted in read timeout error after 5 retries.")
                raise e

class GoogleAppsScript():
    def __init__(self, scriptId):
        GAUTH_CREDS = pickle.loads(base64.b64decode(Variable.get(gpickle)))
        AS_CREDS = GAUTH_CREDS
        self.SCRIPT_ID = scriptId
        self.service = discovery.build('script', 'v1', credentials=AS_CREDS)
        self.retries = 0

    def alertChannel(self, e):
        """
        Send an alert for any failed executions

        Parameters
        ------------------------
        e: Error
        """
        # ALERTS.postMessage(f"""
        # Project: `Recon` ```Module: Google Sheets \nError: {e} \nPlease check Logs for more information.```""")
        print(f"""
        Project: `Recon` ```Module: Google Sheets \nError: {e} \nPlease check Logs for more information.```""")

    def executeFunction(self, functionName):
        request = {"function": functionName}
        try:
            response = self.service.scripts().run(body=request, scriptId=self.SCRIPT_ID).execute()
            if 'error' in response:
                print(response)
                raise Exception
        except (HttpError, socket.timeout) as e:
            # LOGGER.error(e)
            print(e)
            if self.retries<5:
                # LOGGER.warning("Read operation for executeFunction timed out, retrying...")
                print("Read operation for executeFunction timed out, retrying...")
                self.retries+=1
                time.sleep(60 * (self.retries / 2))
                return self.executeFunction(functionName)
            else:
                self.retries=0
                self.alertChannel("executeFunction method resulted in read timeout error after 5 retries.")
                raise e

class GoogleGmail():
    def __init__(self):
        GAUTH_CREDS = pickle.loads(base64.b64decode(Variable.get(gpickle)))
        self.service = build('gmail', 'v1', credentials=GAUTH_CREDS)

    def alertChannel(self, e):
        # ALERTS.postMessage(f"""
        # Project: `Automations` ```Module: Google Sheets \nError: {e} \nPlease check Logs for more information.```""")
        print(f"""
        Project: `Automations` ```Module: Google Sheets \nError: {e} \nPlease check Logs for more information.```""")

    def CreateMessage(self, sender, to, subject, message_text, cc=None, html=False):
        """Create a message for an email.

          Args:
            sender: Email address of the sender.
            to: Email address of the receiver.
            subject: The subject of the email message.
            message_text: The text of the email message.

          Returns:
            An object containing a base64url encoded email object.
        """
        message = MIMEText(message_text)
        if html: message= MIMEText(message_text, 'html')
        message['to'] = to
        message['from'] = sender
        message['subject'] = subject
        if cc is not None:
            message['cc'] = cc
        return {'raw': base64.urlsafe_b64encode(message.as_string().encode()).decode()}

    def CreateMessageWithAttachment(self, sender, to, subject, message_text, file, cc=None):
        message = MIMEMultipart()
        message['to'] = to
        message['from'] = sender
        message['subject'] = subject
        if cc is not None:
            message['cc'] = cc

        msg = MIMEText(message_text)
        message.attach(msg)

        content_type, encoding = mimetypes.guess_type(file)

        if content_type is None or encoding is not None:
            content_type = 'application/octet-stream'
        main_type, sub_type = content_type.split('/', 1)
        if main_type == 'text':
            fp = open(file, 'r')
            msg = MIMEText(fp.read(), _subtype=sub_type)
            fp.close()
        elif main_type == 'image':
            fp = open(file, 'rb')
            msg = MIMEImage(fp.read(), _subtype=sub_type)
            fp.close()
        elif main_type == 'audio':
            fp = open(file, 'rb')
            msg = MIMEAudio(fp.read(), _subtype=sub_type)
            fp.close()
        else:
            fp = open(file, 'rb')
            msg = MIMEBase(main_type, sub_type)
            msg.set_payload(fp.read())
            fp.close()
        filename = os.path.basename(file)
        msg.add_header('Content-Disposition', 'attachment', filename=filename)
        message.attach(msg)

        return {'raw': base64.urlsafe_b64encode(message.as_string().encode()).decode()}

    def sendMail(self, user_id, message):
        try:
            message = (self.service.users().messages().send(userId=user_id, body=message).execute())
        except HttpError as e:
            # LOGGER.error(e)
            print(e)
            self.alertChannel(e)

class GoogleDrive():
    def __init__(self):
        GAUTH_CREDS = pickle.loads(base64.b64decode(Variable.get(gpickle)))

        self.service =build('drive', 'v3', credentials=GAUTH_CREDS, cache_discovery=False)

    def fileUpload(self, fileName, filePathOrBuffer, parents, mimetype, buffer=False):
        """
        Uploads a file to given parent ID
        ------------------------
        fileName: str
            file name with extension
        filePath: str
            file path including fileName
        parents: list
            parentIds to upload to
        mimetype: str
            mimeType of the file
        """
        fileMetaData = {
            'name': fileName,
            'parents': parents
        }
        media = MediaInMemoryUpload(filePathOrBuffer, mimetype) if buffer else MediaFileUpload(filePathOrBuffer, mimetype=mimetype)
        self.service.files().create(
            body = fileMetaData,
            media_body = media,
            fields = 'id'
        ).execute()

    def fileDownload(self, file_id, file_path, mimetype):
        """
        Download a file from a fileID in Drive
        ------------------------
        fileId: str
            file name with extension
        filePath: str
            file path including fileName
        mimetype: str
            mimeType of the file
        application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
        """
        # Call the Drive v3 API
        results = self.service.files().export_media(
            fileId=file_id,
            mimeType=mimetype).execute()
        with open(file_path, "wb") as f:
            f.write(results)
            f.close()

    def fileFinder(self, query, pageSize=20):
        """
        Find a list of file in the drive according to a query
        -----------------------

        query str: Query string to get a list of files
        ----Example-----
        Files with the name "hello"	name = 'hello'
        Files with a name containing the words "hello" and "goodbye"	name contains 'hello' and name contains 'goodbye'
        Folders that are Google apps or have the folder MIME type	mimeType = 'application/vnd.google-apps.folder'
        Documentation:https://developers.google.com/drive/api/v3/search-files

        pageSize : Number of maximum files to return

        return: { 'files' [{'id': [ID1], 'name':[NAME1]},
                            'id': [ID1], 'name':[NAME1]},....]
                }
        """
        resource = self.service.files()
        result = resource.list(q=query, pageSize=pageSize, fields="files(id, name)").execute()
        return result

    def listAllFilesInFolder(self, topFolderId):
        """
        Find list of all files present in provided folder of id {topFolderId}
        """
        items = []
        pageToken = ""
        while pageToken is not None:
            response = self.service.files().list(q="'" + topFolderId + "' in parents", pageSize=1000, pageToken=pageToken, fields="nextPageToken, files(id, name)").execute()
            items.extend(response.get('files', []))
            pageToken = response.get('nextPageToken')

        return items

    def copySheet(self, id_to_copy, name, folder_id):
        newfile = {'name': name, 'parents': folder_id}
        response = self.service.files().copy(fileId=id_to_copy, body=newfile).execute()
        return response

    def get_last_modified_user(self, file_id:str):
        """
        Returns the last modified user for a given file based on file id
        :param file_id: File Identifier
        :return: Modified UserName
        """
        response = self.service.files().get(fileId=file_id, fields="*").execute()
        if response.get('lastModifyingUser') and dict(response.get('lastModifyingUser')).get('displayName'):
            return response['lastModifyingUser']['displayName']
        return 'UNKNOWN'

