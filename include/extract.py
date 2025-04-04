import requests
from bs4 import BeautifulSoup
import pandas as pd
import os



def build_metadata_url(language: str, doc_type: str, article: int = None) -> str:
    """
    Construct a metadata URL for querying ECHR documents.

    Parameters:
    language (str): The language code for the documents.
    doc_type (str): The type of document to query.
    article (int, optional): Article number to filter by. Default is None.

    Returns:
    str: Base URL for metadata query with placeholders for start and length.
    """
    BASEURL = "https://hudoc.echr.coe.int/app/query/results?"

    metadata = """
    select=sharepointid,Rank,ECHRRanking,itemid,
    docname,doctype,appno,conclusion,importance,originatingbody,typedescription,kpdate,
    extractedappno,doctypebranch,respondent
    """

    article_filter = f"((article={article})) AND " if article else ""
    
    url = f"""{BASEURL}query=contentsitename:ECHR AND 
    (NOT (doctype=PR OR doctype=HFCOMOLD OR doctype=HECOMOLD)) AND ((languageisocode="{language}")) AND 
    {article_filter}((documentcollectionid="{doc_type}"))&select={metadata}&sort=&start={{start}}&length={{length}}&rankingModelId=1111111-0000-0000-0000-0000
    """

    return url


class Crawler:
    def __init__(self, language, doc_type: str, article: int = None):
        self.language = language
        self.doc_type = doc_type
        self.article = article
        self.url_metadata = build_metadata_url(language, doc_type, article)
        self.number_of_cases = self.get_number_of_caselaws()
        
    def get_number_of_caselaws(self):
        """
        Retrieve the number of case laws from the metadata URL.

        Returns:
        int: The total number of case laws available.
        """
        # Send a request to the metadata URL with a small batch size
        # to get the total count of available case laws.
        res = requests.get(self.url_metadata.format(start=0, length=1))
        
        # Extract and return the 'resultcount' from the JSON response.
        # If 'resultcount' is not present, return 0 as default.
        return res.json().get('resultcount', 0)
    
    def download_metadata(self, file_path, batch_size=100) -> None:
        """
        Download metadata for a large number of case laws in batches.
        """  

        all_metadata = []
        total_cases = self.get_number_of_caselaws()
        print(f"Total cases to download: {total_cases}")
        
        for start in range(0, total_cases, batch_size):
            print(f"Downloading cases {start} to {start + batch_size}...")
            resp = requests.get(self.url_metadata.format(start=start, length=batch_size))
            if resp.status_code == 200:
                data = resp.json()
                
                for result in data['results']:
                    res = result['columns']
                    # res['url'] = self.url_pdfs.format(item_id=res['itemid'], case_name=res['docname'])
                    all_metadata.append(res)
            else:
                print(f"Failed to download batch starting at {start}. Status code: {res.status_code}")
        
        df = pd.DataFrame(data=all_metadata)
        df.to_csv(file_path, index=False)


def extract_data(
        language="ENG",
        doc_type='JUDGMENTS', 
        local_file_path='./plugins/data.csv',
):
    

    article = 0 # to download data from all articles
    crawler = Crawler(language, 
                      doc_type, 
                      article)
    
    crawler.download_metadata(local_file_path)



def remove_local_file(file_path):
    # Check if the file exists before deleting
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"{file_path} has been deleted.")
    else:
        print(f"{file_path} does not exist.")