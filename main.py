import xml.etree.ElementTree as ET
import requests
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaFileUpload


SCOPES = ['https://www.googleapis.com/auth/drive.file']
# CSVPATH = '/home/drondavu/apps/DNxmlFeed/feed.csv'
CSVPATH = 'feed.csv'

def getXML():
	xmlUrl = 'https://www.dronenerds.com/dynnexdrones.xml'

	r = requests.get(xmlUrl)

	if r.status_code == 200:
		tree = ET.fromstring(r.content)
		return tree
	else:
		print('Error:', r)


def parseXML(tree, columns):
	items = [[[] for c in columns]]
	# heads = []
	for row, el in enumerate(tree.iter('item')):
		items.append([[] for c in columns])
		children = list(el)
		for child in children:
			colHead = child.tag.replace("{http://base.google.com/ns/1.0}", "")
			# if colHead not in heads:
				# heads.append(colHead)
			colContent = child.text
			items[row][columns.index(colHead)].append(colContent)
	# print(heads)
	
	return items
	

def writeCSV(items, columns):
	delimeter = '\t'
	endLine = '\n'
	with open(CSVPATH, 'w+') as csvFile:
		csvFile.write(delimeter.join(columns) + endLine)
		for i, row in enumerate(items):
			for j, column in enumerate(row):
				items[i][j] = ', '.join(items[i][j])
			line = delimeter.join(row)
			csvFile.write(line + endLine)


def authenticate():
	creds = None
	if os.path.exists('token.pickle'):
		with open('token.pickle', 'rb') as token:
			creds = pickle.load(token)
	if not creds or not creds.valid:
		if creds and creds.expired and creds.refresh_token:
			creds.refresh(Request())
		else:
			flow = InstalledAppFlow.from_client_secrets_file(
				'credentials.json', SCOPES)
			creds = flow.run_local_server(port=0)
		with open('token.pickle', 'wb') as token:
			pickle.dump(creds, token)
	
	return creds


def uploadToGdrive(service):
	# check if file already exists in DN Store XML folder
	results = service.files().list(
		pageSize=10, fields="nextPageToken, files(id, name, parents)", orderBy='folder').execute()
	files = results.get('files', [])

	fileName = 'DNstoreFeed.csv'
	folder = '146OGdCEKNdVjikvTDDOA919lAff2DB07'
	fileExists = False
	for f in files:
		if f['name'] == fileName and f['parents'][0] == folder:
			fileExists = True
			fileId = f['id']
	
	# update the file data
	if fileExists:
		file_metadata = {'name': fileName}
		media = MediaFileUpload(CSVPATH, mimetype='text/csv')
		file = service.files().update(body=file_metadata,
											media_body=media,
											fields='id',
											fileId=fileId).execute()
		print('Updated: %s' % file.get('id'))

	# create the file
	else:
		file_metadata = {'name': fileName, 'parents': [folder]}
		media = MediaFileUpload(CSVPATH, mimetype='text/csv')
		file = service.files().create(body=file_metadata,
											media_body=media,
											fields='id').execute()
		print('Created: %s' % file.get('id'))


def main():
	columns = ['title', 'link', 'description', 'image_link', 'stock', 'price', 'brand', 'sku', 
		'upc', 'category', 'additional_image_link', 'sale_price', 'sale_price_effective_date']

	tree = getXML()
	items = parseXML(tree, columns)

	writeCSV(items, columns)

	creds = authenticate()
	service = build('drive', 'v3', credentials=creds)

	uploadToGdrive(service)


if __name__ == "__main__":
	main()