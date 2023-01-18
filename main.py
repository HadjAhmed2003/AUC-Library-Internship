import math

import numpy as np
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
import crossref_commons.retrieval
from ast import literal_eval
from pybliometrics.scopus import AbstractRetrieval
import pybliometrics
def get_credentials(): #A function to get the necessary credentials to run the APIs
    #These lines are to get the necessary information from the user
    email = input("Enter your email for the Unpaywall API: ")
    scopus_key = input("Enter your SCOPUS API key: ")
    has_insttoken = input("Do you have a SCOPUS Institutional Token (insttoken)? (y/n) ")
    if(has_insttoken == 'y' or 'Y'):
        insttoken = input("Enter your SCOPUS Institutional Token (insttoken): ")
    else:
        insttoken = ""
    #Passing the email to activate the Unpaywall API
    UnpywallCredentials(email)
    os.environ['UNPAYWALL_EMAIL'] = email
    #Adding the information for the SCOPUS API to the JSON configuration file
    con_file = open("config.json", "r")
    config = json.load(con_file)
    con_file.close()
    config['apikey'] = scopus_key
    config[insttoken] = insttoken
    con_file = open("config.json", "w")
    con_file.write(json.dumps(config))
    con_file.close()
    #Creating the SCOPUS API Client
    client = ElsClient(config['apikey'])
    client.inst_token = config['insttoken']
    #We return the SCOPUS client as we will later on need it for other functions
    return client

def scopus_search(client): #A function that serves to do a search on SCOPUS and return the data as a JSON file
    #Getting the necessary information from the user (Assuming that he is looking for journal articles)
    affiliation = input("Enter the affiliation of your authors: ")
    year_of_publication = int(input("Enter the starting year of the articles: "))
    #Making the search using the SCOPUS API
    doc_srch = ElsSearch("AFFIL(\""+affiliation+"\") AND PUBYEAR > "+str(year_of_publication-1)+" AND DOCTYPE(ar)", 'scopus')
    doc_srch.execute(client, get_all=True)


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

# pybliometrics.scopus.utils.create_config()
df = pandas.read_json("dump.json")
df = df.dropna(subset=['prism:doi'])
df['prism:doi']=df['prism:doi'].str.strip()
mylist = df['prism:doi'].tolist()
newdf = Unpywall.doi(dois=mylist, errors='ignore')
newdf['doi']=newdf['doi'].str.strip()
# newdf.to_excel("unpywall.xlsx")
# print(df)
df.rename(columns={'prism:doi':'doi'}, inplace=True)

tdf = newdf[['z_authors']].copy()
tdf.rename(columns={'z_authors':'Authors'}, inplace=True)
tdf["Authors"] = tdf["Authors"].map(str).apply(literal_eval)
tdf["index"] = range(1, len(tdf) + 1)
tdf = tdf.explode("Authors")

tdf["Given"] = tdf["Authors"].str["given"]
tdf["Family"] = tdf["Authors"].str["family"]
tdf["ORCID"] = tdf["Authors"].str["ORCID"]
tdf["Affiliation"] = tdf["Authors"].str["affiliation"]
tdf.pop("Authors")
tdf["counter2"] = tdf.groupby("index").cumcount() + 1

tdf = tdf.pivot(index="index", columns=["counter2"])
tdf.columns = [f"{a}_{b}" for a, b in tdf.columns]
tdf = tdf[sorted(tdf, key=lambda c: int(c.split("_")[-1]))]
# tdf.to_excel('names.xlsx')
i = 4
j = 1
while(i<len(tdf.axes[1])-1):
    # tdf['Affiliation_{}'.format(j)].map(str).apply(literal_eval)
    for index, row in tdf.iterrows():
        tlist = row['Affiliation_{}'.format(j)]
        result = ""
        if(isinstance(tlist, list)):
            for x in tlist:
                if(len(x)>0):
                    result+=x['name']+','
            result = result[:len(result)-1]
            row['Affiliation_{}'.format(j)] = result
    j += 1
    i += 4

for index, row in tdf.iterrows():
    i = 4
    j = 1
    while(i<len(tdf.axes[1])-1):
        if(j<=4 and str(row['Affiliation_{}'.format(j)]).find("American University in Cairo")!=-1):
            break
        elif(j>4 and str(row['Affiliation_{}'.format(j)]).find("American University in Cairo")!=-1):
            temp = row['Affiliation_{}'.format(j)]
            row['Affiliation_{}'.format(j)]=row['Affiliation_4']
            row['Affiliation_4'] = temp
            temp = row['Given_{}'.format(j)]
            row['Given_{}'.format(j)] = row['Given_4']
            row['Given_4'] = temp
            temp = row['Family_{}'.format(j)]
            row['Family_{}'.format(j)] = row['Family_4']
            row['Family_4'] = temp
            temp = row['ORCID_{}'.format(j)]
            row['ORCID_{}'.format(j)] = row['ORCID_4']
            row['ORCID_4'] = temp
            break
        i+=4
        j+=1
tdf.insert(0, 'doi', newdf['doi'].values)

upload_df = newdf[['doi']].copy()
upload_df['title'] = newdf['title'].values
upload_df['document_type'] = newdf['genre'].values
upload_df['publication_date'] = newdf['published_date'].values
upload_df['fulltext_url'] = newdf['doi_url'].values
upload_df['keywords'] = np.NaN
upload_df['abstract'] = np.NaN
upload_df['author1_fname']=tdf['Given_1'].values
upload_df['author1_mname']=np.NaN
upload_df['author1_lname']=tdf['Family_1'].values
upload_df['author1_suffix']=np.NaN
upload_df['author1_email']=np.NaN
upload_df['author1_institution']=tdf['Affiliation_1'].values
upload_df['author1_is_corporate']=False
upload_df['author2_fname']=tdf['Given_2'].values
upload_df['author2_mname']=np.NaN
upload_df['author2_lname']=tdf['Family_2'].values
upload_df['author2_suffix']=np.NaN
upload_df['author2_email']=np.NaN
upload_df['author2_institution']=tdf['Affiliation_2'].values
upload_df['author2_is_corporate']=False
upload_df['author3_fname']=tdf['Given_3'].values
upload_df['author3_mname']=np.NaN
upload_df['author3_lname']=tdf['Family_3'].values
upload_df['author3_suffix']=np.NaN
upload_df['author3_email']=np.NaN
upload_df['author3_institution']=tdf['Affiliation_3'].values
upload_df['author3_is_corporate']=False
upload_df['author4_fname']=tdf['Given_4'].values
upload_df['author4_mname']=np.NaN
upload_df['author4_lname']=tdf['Family_4'].values
upload_df['author4_suffix']=np.NaN
upload_df['author4_email']=np.NaN
upload_df['author4_institution']=tdf['Affiliation_4'].values
upload_df['author4_is_corporate']=False
upload_df['all_authors']=np.NaN
upload_df['disciplines']=np.NaN
upload_df['comments']=np.NaN
upload_df['custom_citaion']=np.NaN
upload_df['department']=np.NaN
upload_df['department_2']=np.NaN
upload_df['department_3']=np.NaN
upload_df['department_4']=np.NaN
upload_df['department_5']=np.NaN
upload_df['embargo_date']=np.NaN
upload_df['fpage']=np.NaN
upload_df['funding_number']=np.NaN
upload_df['fundref']=np.NaN
upload_df['identifier']=np.NaN
upload_df['issnum']=np.NaN
upload_df['lpage']=np.NaN
upload_df['orcid']=np.NaN
upload_df['program']=np.NaN
upload_df['season']=np.NaN
upload_df['pubmedid']=np.NaN
upload_df['scopus_id']=np.NaN
upload_df['volnum']=np.NaN
upload_df['translator']=np.NaN
upload_df['source_publication']=np.NaN
upload_df['web_address']=np.NaN
upload_df.to_excel('final.xls')
# for index, row in upload_df.iterrows():
#     doi = row['doi']
# ab = AbstractRetrieval(newdf.iloc[10]['doi'], view = 'META')
    # upload_df.loc[upload_df['doi']==doi, 'keywords'] = ','.join(ab.authkeywords)
    # upload_df.loc[upload_df['doi']==doi, 'abstract'] = ab.abstract
# print(ab)


# df.sort_values(by=['doi'])
# newdf.sort_values(by=['doi'])
# finalnames = pandas.DataFrame(columns=['temp'])
# for i in newdf['z_authors']:
#     tempdf = pandas.DataFrame(i)
#     tempdf2 = tempdf.iloc[[0]]
#     for index in range(1, len(tempdf.index)):
#         tempdf2.join(tempdf.iloc[[index]], )
#
#     finalnames = pandas.concat([finalnames,tempdf2])
# finalnames = finalnames.drop('temp', axis=1)

# test = list()
# finalnames = pandas.DataFrame(columns=['temp'])
# df_json = pandas.json_normalize(crossref_commons.retrieval.get_publication_as_json(mylist[0]))
# finalnames = pandas.concat([finalnames, df_json], axis=1)

# for doi in mylist[1:20]:
#     df_json = pandas.json_normalize(crossref_commons.retrieval.get_publication_as_json(doi))
#     finalnames = pandas.concat([finalnames, df_json])
# finalnames.to_excel('names.xlsx')
# with open('counseling3.json', 'w') as output_file:
#     json.dump(test, output_file)




# names = pandas.json_normalize(newdf['z_authors'])
# names.to_excel('names.xlsx')
# finalnames = pandas.DataFrame(columns=['temp'])
# for x in range(names.shape[1]):
#     names[x] = names[x].apply(json.loads)
#     df_json = pandas.json_normalize(names[x])
#     finalnames = pandas.concat([finalnames, df_json], axis=1)

# df.to_excel('SCOPUS.xlsx')
# newdf.to_excel('UNPAYWALL.xlsx')
# finaldf = df_cd = pd.merge(df, newdf, how='left', on = 'doi')
# print(newdf['doi'])
# print(df['doi'])
