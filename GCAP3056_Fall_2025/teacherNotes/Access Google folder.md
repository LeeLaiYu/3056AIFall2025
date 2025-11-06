# Google Docs API Access

https://docs.google.com/document/d/19ND3APGCVjd-UC1ie0kurt9YXlJw-smf1EfuF6szMJ0/edit?tab=t.updffvjz6ygq

[[emergency alert system project]] 

[[leverage Google docs and other existing tools]] 

## Python Code for Google Docs API Access

### 1. Setup and Authentication

```python
import os
import json
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = [
    'https://www.googleapis.com/auth/documents.readonly',
    'https://www.googleapis.com/auth/documents',
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/drive'
]

class GoogleDocsAPI:
    def __init__(self, credentials_file='credentials.json', token_file='token.json'):
        self.credentials_file = credentials_file
        self.token_file = token_file
        self.creds = None
        self.docs_service = None
        self.drive_service = None
        self.authenticate()
    
    def authenticate(self):
        """Authenticate and create Google API service objects"""
        # The file token.json stores the user's access and refresh tokens.
        if os.path.exists(self.token_file):
            self.creds = Credentials.from_authorized_user_file(self.token_file, SCOPES)
        
        # If there are no (valid) credentials available, let the user log in.
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_file, SCOPES)
                self.creds = flow.run_local_server(port=0)
            
            # Save the credentials for the next run
            with open(self.token_file, 'w') as token:
                token.write(self.creds.to_json())
        
        # Build the service objects
        self.docs_service = build('docs', 'v1', credentials=self.creds)
        self.drive_service = build('drive', 'v3', credentials=self.creds)

    def extract_document_id(self, url):
        """Extract document ID from Google Docs URL"""
        if '/document/d/' in url:
            return url.split('/document/d/')[1].split('/')[0]
        return url

    def read_document(self, document_id):
        """Read the entire content of a Google Doc"""
        try:
            # Retrieve the documents contents from the Docs service.
            document = self.docs_service.documents().get(documentId=document_id).execute()
            
            print(f'Document Title: {document.get("title")}')
            content = self.extract_text_from_document(document)
            return {
                'title': document.get('title'),
                'content': content,
                'document_id': document_id,
                'revision_id': document.get('revisionId')
            }
        
        except HttpError as err:
            print(f'An error occurred: {err}')
            return None

    def extract_text_from_document(self, document):
        """Extract plain text from document structure"""
        content = []
        body = document.get('body', {})
        
        if 'content' in body:
            for element in body['content']:
                if 'paragraph' in element:
                    paragraph = element['paragraph']
                    if 'elements' in paragraph:
                        paragraph_text = ''
                        for elem in paragraph['elements']:
                            if 'textRun' in elem:
                                paragraph_text += elem['textRun']['content']
                        content.append(paragraph_text.strip())
                elif 'table' in element:
                    # Handle tables
                    table = element['table']
                    for row in table.get('tableRows', []):
                        for cell in row.get('tableCells', []):
                            cell_content = self.extract_text_from_content(cell.get('content', []))
                            if cell_content.strip():
                                content.append(f"Table Cell: {cell_content.strip()}")
        
        return '\n'.join(content)

    def extract_text_from_content(self, content_elements):
        """Helper function to extract text from content elements"""
        text = ''
        for element in content_elements:
            if 'paragraph' in element:
                paragraph = element['paragraph']
                if 'elements' in paragraph:
                    for elem in paragraph['elements']:
                        if 'textRun' in elem:
                            text += elem['textRun']['content']
        return text

    def get_document_metadata(self, document_id):
        """Get document metadata using Drive API"""
        try:
            file = self.drive_service.files().get(
                fileId=document_id,
                fields='id,name,createdTime,modifiedTime,owners,permissions,size,mimeType'
            ).execute()
            
            return {
                'id': file.get('id'),
                'name': file.get('name'),
                'created_time': file.get('createdTime'),
                'modified_time': file.get('modifiedTime'),
                'owners': file.get('owners', []),
                'mime_type': file.get('mimeType'),
                'size': file.get('size')
            }
        
        except HttpError as err:
            print(f'An error occurred: {err}')
            return None

    def list_documents_in_folder(self, folder_id):
        """List all Google Docs in a specific folder"""
        try:
            query = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.document'"
            results = self.drive_service.files().list(
                q=query,
                fields='files(id,name,createdTime,modifiedTime)'
            ).execute()
            
            return results.get('files', [])
        
        except HttpError as err:
            print(f'An error occurred: {err}')
            return []

    def search_documents(self, query_text):
        """Search for documents containing specific text"""
        try:
            query = f"fullText contains '{query_text}' and mimeType='application/vnd.google-apps.document'"
            results = self.drive_service.files().list(
                q=query,
                fields='files(id,name,createdTime,modifiedTime)'
            ).execute()
            
            return results.get('files', [])
        
        except HttpError as err:
            print(f'An error occurred: {err}')
            return []

    def create_document(self, title, content=None):
        """Create a new Google Doc"""
        try:
            document = {
                'title': title
            }
            
            doc = self.docs_service.documents().create(body=document).execute()
            document_id = doc.get('documentId')
            
            if content:
                self.insert_text(document_id, content)
            
            return {
                'document_id': document_id,
                'title': doc.get('title'),
                'url': f'https://docs.google.com/document/d/{document_id}/edit'
            }
        
        except HttpError as err:
            print(f'An error occurred: {err}')
            return None

    def insert_text(self, document_id, text, index=1):
        """Insert text into a document at specified index"""
        try:
            requests = [
                {
                    'insertText': {
                        'location': {
                            'index': index,
                        },
                        'text': text
                    }
                }
            ]
            
            result = self.docs_service.documents().batchUpdate(
                documentId=document_id,
                body={'requests': requests}
            ).execute()
            
            return result
        
        except HttpError as err:
            print(f'An error occurred: {err}')
            return None

    def export_document(self, document_id, export_format='txt'):
        """Export document to different formats"""
        try:
            mime_types = {
                'txt': 'text/plain',
                'pdf': 'application/pdf',
                'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                'html': 'text/html'
            }
            
            mime_type = mime_types.get(export_format, 'text/plain')
            
            result = self.drive_service.files().export(
                fileId=document_id,
                mimeType=mime_type
            ).execute()
            
            return result
        
        except HttpError as err:
            print(f'An error occurred: {err}')
            return None
```

### 2. Usage Examples

```python
# Example usage for GCAP3056 Emergency Alert System project

def main():
    # Initialize the Google Docs API client
    api = GoogleDocsAPI()
    
    # Document ID from the URL above
    doc_url = "https://docs.google.com/document/d/19ND3APGCVjd-UC1ie0kurt9YXlJw-smf1EfuF6szMJ0/edit?tab=t.updffvjz6ygq"
    doc_id = api.extract_document_id(doc_url)
    
    print(f"Working with document ID: {doc_id}")
    
    # Read the document content
    document_data = api.read_document(doc_id)
    if document_data:
        print(f"\nDocument Title: {document_data['title']}")
        print(f"\nContent:\n{document_data['content'][:500]}...")  # First 500 chars
        
        # Save to local file
        with open(f"emergency_alert_doc_{doc_id}.txt", 'w', encoding='utf-8') as f:
            f.write(f"Title: {document_data['title']}\n\n")
            f.write(document_data['content'])
        print(f"\nDocument saved to emergency_alert_doc_{doc_id}.txt")
    
    # Get document metadata
    metadata = api.get_document_metadata(doc_id)
    if metadata:
        print(f"\nDocument Metadata:")
        print(f"Name: {metadata['name']}")
        print(f"Created: {metadata['created_time']}")
        print(f"Modified: {metadata['modified_time']}")
        print(f"Owners: {[owner.get('displayName', 'Unknown') for owner in metadata['owners']]}")
    
    # Search for related documents
    related_docs = api.search_documents("emergency alert")
    if related_docs:
        print(f"\nFound {len(related_docs)} related documents:")
        for doc in related_docs[:5]:  # Show first 5
            print(f"- {doc['name']} (ID: {doc['id']})")
    
    # Export document to different formats
    print("\nExporting document...")
    txt_export = api.export_document(doc_id, 'txt')
    if txt_export:
        with open(f"emergency_alert_export_{doc_id}.txt", 'wb') as f:
            f.write(txt_export)
        print(f"Document exported as TXT")
    
    pdf_export = api.export_document(doc_id, 'pdf')
    if pdf_export:
        with open(f"emergency_alert_export_{doc_id}.pdf", 'wb') as f:
            f.write(pdf_export)
        print(f"Document exported as PDF")

if __name__ == '__main__':
    main()
```

### 3. Batch Processing for Multiple Documents

```python
# Batch processing for GCAP3056 project documents

def batch_process_gcap_documents():
    api = GoogleDocsAPI()
    
    # List of document URLs for different GCAP3056 projects
    project_documents = {
        'emergency_alert': '19ND3APGCVjd-UC1ie0kurt9YXlJw-smf1EfuF6szMJ0',
        # Add more document IDs as needed
    }
    
    processed_docs = []
    
    for project_name, doc_id in project_documents.items():
        print(f"\nProcessing {project_name} document...")
        
        # Read document
        doc_data = api.read_document(doc_id)
        if doc_data:
            # Save to project-specific file
            filename = f"GCAP3056_{project_name}_{doc_id}.md"
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(f"# {doc_data['title']}\n\n")
                f.write(f"**Document ID**: {doc_id}\n")
                f.write(f"**Project**: {project_name.replace('_', ' ').title()}\n\n")
                f.write("## Content\n\n")
                f.write(doc_data['content'])
            
            processed_docs.append({
                'project': project_name,
                'title': doc_data['title'],
                'filename': filename,
                'doc_id': doc_id
            })
            
            print(f"✓ Saved: {filename}")
    
    # Generate summary report
    with open('GCAP3056_documents_summary.md', 'w', encoding='utf-8') as f:
        f.write("# GCAP3056 Google Documents Summary\n\n")
        f.write(f"**Processed**: {len(processed_docs)} documents\n\n")
        
        for doc in processed_docs:
            f.write(f"## {doc['title']}\n")
            f.write(f"- **Project**: {doc['project'].replace('_', ' ').title()}\n")
            f.write(f"- **Document ID**: {doc['doc_id']}\n")
            f.write(f"- **Local File**: {doc['filename']}\n")
            f.write(f"- **Google Docs URL**: https://docs.google.com/document/d/{doc['doc_id']}/edit\n\n")
    
    print(f"\n✓ Summary report saved: GCAP3056_documents_summary.md")
    return processed_docs
```

### 4. Installation Requirements

```bash
# Install required packages
pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib
```

### 5. Setup Instructions

1. **Enable Google Docs API**:
   - Go to [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project or select existing one
   - Enable Google Docs API and Google Drive API
   - Create credentials (OAuth 2.0 Client ID)
   - Download credentials JSON file as `credentials.json`

2. **First Run**:
   - Place `credentials.json` in your project directory
   - Run the script - it will open a browser for authentication
   - Grant necessary permissions
   - `token.json` will be created automatically for future runs

3. **File Structure**:
   ```
   GCAP3056/
   ├── Access Google folder.md (this file)
   ├── credentials.json (your OAuth credentials)
   ├── token.json (auto-generated after first auth)
   ├── google_docs_api.py (main API code)
   └── process_docs.py (usage examples)
   ```

### 6. Error Handling and Best Practices

```python
# Enhanced error handling
class GoogleDocsAPIError(Exception):
    pass

def safe_api_call(func, *args, **kwargs):
    """Wrapper for safe API calls with retry logic"""
    import time
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except HttpError as e:
            if e.resp.status == 429:  # Rate limit
                wait_time = 2 ** attempt
                print(f"Rate limited. Waiting {wait_time} seconds...")
                time.sleep(wait_time)
                continue
            elif e.resp.status == 403:  # Forbidden
                print(f"Access forbidden: {e}")
                break
            else:
                print(f"HTTP Error: {e}")
                break
        except Exception as e:
            print(f"Unexpected error: {e}")
            break
    
    return None
```

## Integration with GCAP3056 Projects

This API access code can be used across all GCAP3056 projects:

- **[[Emergency-Alert-System/emergency alert system project]]** - Access emergency alert documentation
- **[[HKO chatbot project]]** - Retrieve HKO chatbot analysis documents  
- **[[Bus-Stop-Optimization/moodle forum post GCAP 3056 -Topic 2 Bus Stop Merger Optimization Project]]** - Access bus optimization research
- **[[SCMP letter collection]]** - Manage letter writing campaigns
- **[[Transport Department]]** - Access government correspondence

## Security Notes

- Keep `credentials.json` and `token.json` secure and never commit to version control
- Use environment variables for sensitive data in production
- Regularly review and rotate API credentials
- Implement proper access controls for shared documents 