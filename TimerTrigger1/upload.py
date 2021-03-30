#will be stored to "f5mdata" folder in iec2019065@iiita.ac.in Gdrive



#pydrive
import pickle #token.pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from apiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials


#global variable
service = None #only call service_object if it's None

#pydrive service object
def service_object():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.

    # If modifying these scopes, delete the file token.json.
    SCOPES = ['https://www.googleapis.com/auth/drive']

    #token.pickle resides in overall function directory instead of individual function apps
    with open('token.pickle', 'rb') as token:
        creds = pickle.load(token)

    #refreshes with refresh_token if creds is expired
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())

    
    service = build('drive', 'v3', credentials=creds)

    return service


def upload(file_name):
    
    #service_object
    global service
    if service == None :
        service = service_object()

    #folder_id (will be stored to "f5mdata" folder in iec2019065@iiita.ac.in Gdrive)
    folder_object = service.files().list(q="mimeType='application/vnd.google-apps.folder'  and name='f5mdata'",
                                         spaces='drive',
                                         fields='files(name,id)').execute()
    
    folder_id = folder_object.get('files')[0]['id']

    #upload
    file_metadata = {
    'name': [file_name],
    'parents': [folder_id]}
    media = MediaFileUpload(file_name, mimetype='text/parquet', resumable=True)
    service.files().create(body=file_metadata,media_body=media, fields='id').execute()
    

