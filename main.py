import pandas as pd
from elsapy.elsclient import ElsClient
from elsapy.elsprofile import ElsAuthor, ElsAffil
from elsapy.elsdoc import FullDoc, AbsDoc
from elsapy.elssearch import ElsSearch
import json
import pandas
from unpywall import Unpywall
import os
import openpyxl
from unpywall.utils import UnpywallCredentials

UnpywallCredentials('hadj_ahmed@aucegypt.edu')
os.environ['UNPAYWALL_EMAIL'] = 'hadj_ahmed@aucegypt.edu'
con_file = open("config.json")
config = json.load(con_file)
con_file.close()

client = ElsClient(config['apikey'])
client.inst_token = config['insttoken']

# doi_doc = FullDoc(doi='10.1016/j.bcra.2022.100088')
# if doi_doc.read(client):
#     print("doi_doc.title: ", doi_doc.title)
#     doi_doc.write()
# else:
#     print("Read document failed.")

# doc_srch = ElsSearch("AFFIL(\"American University in Cairo\") AND PUBYEAR > 2021 AND DOCTYPE(ar)", 'scopus')
# doc_srch.execute(client, get_all=True)
# print("doc_srch has", len(doc_srch.results), "results.")
df = pandas.read_json("dump.json")
df = df.dropna(subset=['prism:doi'])
df['prism:doi']=df['prism:doi'].str.strip()
mylist = df['prism:doi'].tolist()
newdf = Unpywall.doi(dois=mylist, errors='ignore')
newdf['doi']=newdf['doi'].str.strip()
# newdf.to_excel("unpywall.xlsx")
# print(df)
df.rename(columns={'prism:doi':'doi'}, inplace=True)
df.sort_values(by=['doi'])
newdf.sort_values(by=['doi'])
df.to_excel('SCOPUS.xlsx')
newdf.to_excel('UNPAYWALL.xlsx')
# finaldf = df_cd = pd.merge(df, newdf, how='left', on = 'doi')
# print(newdf['doi'])
# print(df['doi'])
