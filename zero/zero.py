from bs4 import BeautifulSoup
import json
import pandas as pd
import numpy as np
import os
import requests
from zipfile import ZipFile
from sqlalchemy import create_engine
import psycopg2
import psycopg2.extras
from datetime import datetime

def build_year(abspath,**kwargs):
    raw = SOPRDir(abspath)
    df = SOPRdf(raw,**kwargs)
    return df

def issu_filter(alist,atoken):
    for x in alist:
        if atoken in x:
            return True
    return False

class SOPRDownloader(object):
    base_url = 'http://soprweb.senate.gov/downloads/'
        
    def download_files(self,year,datapath,**kwargs):
        if 'qtr' in kwargs.keys():
            qtrs = [str(x) for x in xrange(1,int(kwargs['qtr'])+1)]
        else:
            qtrs = [1,2,3,4]
        for qtr in qtrs:
            filename = ''.join([str(year),'_',str(qtr),'.zip'])
            url = ''.join([self.base_url,filename])
            try:
                r = requests.get(url)
                filepath = os.path.join(datapath,filename)
                with open(filepath,'wb') as outfile:
                    outfile.write(r.content)
                self.filenames.append(filepath)
            except:
                print 'Failed to download {0}'.format(filename)

            files_path = os.path.join(datapath,str(year))

            if str(year) not in os.listdir(datapath):
                os.mkdir(files_path)
            inf = ZipFile(filepath)
            inf.extractall(files_path)
            inf.close()
            os.remove(filepath)

    def __init__(self,year,datapath,**kwargs):
        self.filenames = []
        self.download_files(year,datapath,**kwargs)

class SOPRdf(object):

    def filter_issue(self,issue):
        self.df[issue] = self.df['issues'].apply(lambda x:issue in x)

    def __init__(self,records,**kwargs):
        df = pd.DataFrame(records.records)
        df['received_date'] = df['received'].apply(lambda x:datetime.strptime(x[0:18],"%Y-%m-%dT%H:%M:%S").date())
        self.df = df
        self.filtered = self.df
        if 'issue' in kwargs.keys():
            self.filter_issue(kwargs['issue'])
            self.filtered = self.filtered.ix[self.filtered[kwargs['issue']] == True]        
        if 'year' in kwargs.keys():
            self.filtered = self.filtered.ix[df['year'] == kwargs['year']]
        if 'deadline' in kwargs.keys():
            self.filtered = self.filtered.ix[df['received_date'] < datetime.strptime(kwargs['deadline'],"%Y-%m-%d").date()]
        
        

        
class SOPRDB(object):

    def __init__(self,table,tablename,engine):
        table.to_sql(tablename,engine,if_exists='replace')

        
class SOPRDir(object):

    def get_targets(self,abspath):
        targets = [os.path.join(abspath,stem) for stem in os.listdir(abspath)]
        return targets

    def build_db(self,target_list):
        self.records = []
        for target in target_list:
            try:
                addition = SOPRPage(target)
                self.records.extend(addition.records)
            except:
                print 'Could not include {0}'.format(target)
            
    def __init__(self, abspath, **kwargs):
        targets = self.get_targets(abspath)
        self.build_db(targets)
        

class SOPRPage(object):
    """
    Store all values in a page of SOPR lobbying data.
    Each record pertains to a firm:client pair.
    Firms and clients have unique IDs
    """

    def load_page(self, abspath):
        with open(abspath,'r') as inf:
            raw = inf.read()
        soup = BeautifulSoup(raw)
        return soup

    def get_value(self,tag,attr):
         if attr in tag.attrs.keys():
             return tag[attr]
         else:
             return 'empty'

    def make_records(self, xmlsoup):
        filings = xmlsoup.publicfilings.find_all('filing')
        self.records = []
        for doc in filings:
            record = {}
            record['id'] = doc['id']
            record['amount'] = doc['amount']
            record['type'] = doc['type']
            record['period'] = doc['period']
            record['received'] = doc['received']
            record['year'] = doc['year']
            record['client_id'] = str(doc.client['clientid']).zfill(10)
            record['registrant_id'] = str(doc.registrant['registrantid']).zfill(10)
            record['cli_reg'] = str(record['client_id']) + str(record['registrant_id'])
            record['registrant_name'] = doc.registrant['registrantname']
            record['registrant_country'] = self.get_value(doc.registrant,'registrantcountry')
            record['registrant_ppb_country'] = self.get_value(doc.registrant,'registrantppbcountry')
            record['registrant_state'] = self.get_value(doc.registrant,['registrantstate'])
            record['client_name'] = doc.client['clientname']
            record['client_country'] = self.get_value(doc.client,'clientcountry')
            record['client_state'] = self.get_value(doc.client,'clientstate')
            record['clientpbbcountry'] = self.get_value(doc.client,'clientpbbcountry')
            record['clientppbstate'] = self.get_value(doc.client,'clientppbstate')
            record['government_entities'] = [entity['goventityname'] for entity \
                                             in doc.find_all('governmententity')]
            record['issues'] = [issue['code'] for issue in doc.find_all('issue')]
            record['specific_issue'] = [self.get_value(issue,'specificissue') for issue in doc.find_all('issue')]
            self.records.append(record)

    def __init__(self,abspath,**kwargs):
        page = self.load_page(abspath)
        self.make_records(page)
        
