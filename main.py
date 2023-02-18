import warnings
import logging
import shutil
import math
import getpass
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
warnings.simplefilter('ignore')
def get_credentials(): #A function to get the necessary credentials to run the APIs
    #These lines are to get the necessary information from the user
    email = input("Enter your email for the Unpaywall API: ")
    scopus_key = input("Enter your SCOPUS API key: ")
    has_insttoken = input("Do you have a SCOPUS Institutional Token (insttoken)? (y/n) ")
    if(has_insttoken == 'y' or has_insttoken == 'Y'):
        insttoken = input("Enter your SCOPUS Institutional Token (insttoken): ")
    else:
        insttoken = ""
    #Passing the email to activate the Unpaywall API
    UnpywallCredentials(email)
    os.environ['UNPAYWALL_EMAIL'] = email
    #Adding the information for the SCOPUS API to the JSON configuration file
    dic = {'apikey': "", 'insttoken': ""}
    con_file = open("config.json", "w")
    con_file.write(json.dumps(dic))
    con_file.close()
    con_file = open("config.json", "r")
    config = json.load(con_file)
    con_file.close()
    config['apikey'] = scopus_key
    config['insttoken'] = insttoken
    con_file = open("config.json", "w")
    con_file.write(json.dumps(config))
    con_file.close()
    #Creating the SCOPUS API Client
    client = ElsClient(config['apikey'])
    client.inst_token = config['insttoken']
    #We return the SCOPUS client as we will later on need it for other functions
    return client

def scopus_search(client,affiliation): #A function that serves to do a search on SCOPUS and return the data as a JSON file
    #Getting the necessary information from the user (Assuming that he is looking for journal articles)
    affiliation = input("Enter the affiliation of your authors: ")
    year_of_publication = int(input("Enter the starting year of the articles: "))
    #Making the search using the SCOPUS API
    doc_srch = ElsSearch("AFFIL(\""+affiliation+"\") AND PUBYEAR > "+str(year_of_publication-1)+" AND DOCTYPE(ar)", 'scopus')
    doc_srch.execute(client, get_all=True)

def get_scopus_dataframe():#This function creates a dataframe from the JSON file of the search results from the SCOPUS API
    scopus_df = pandas.read_json('dump.json') #Convert JSON to a Dataframe
    scopus_df = scopus_df.dropna(subset=['prism:doi']) #dropping the rows with no DOI as they become problematic later on
    scopus_df['prism:doi'] = scopus_df['prism:doi'].str.strip() # We remove leading and trailing white spaces from the DOIs
    scopus_df.rename(columns={'prism:doi': 'doi'}, inplace=True) #Renaming the DOIs column
    return scopus_df

def unpaywall_search(scopus_df): #This function gets additional information on the scopus search results through unpaywall
    doi_list = scopus_df['doi'].tolist() #We convert the DOIs column in the socopus df into a list
    unpaywall_df = Unpywall.doi(dois=doi_list, errors='ignore') #We use the API to get us data on every entry in the list and we ignore problematic DOIs
    unpaywall_df['doi'] = unpaywall_df['doi'].str.strip() # We remove leading and trailing white spaces from the DOIs
    return unpaywall_df

def clean_authors(unpaywall_df): #This function cleans the authors data obtained from unpaywall as an array of dictionaries
    authors_df = unpaywall_df[['z_authors']].copy() # We create a single column dataframe with the authors column from the unpaywall dataframe
    authors_df.rename(columns={'z_authors': 'Authors'}, inplace=True) #Change the column's name to authors as it looks cleaner ;)
    authors_df["Authors"] = authors_df["Authors"].map(str).apply(literal_eval) #We evaluate the python objects inside the column
    authors_df["index"] = range(1, len(authors_df) + 1) # We create an index column
    authors_df = authors_df.explode("Authors") #We explode the authors' dictionaries' fields
    authors_df["Given"] = authors_df["Authors"].str["given"]#We Create a column for every field
    authors_df["Family"] = authors_df["Authors"].str["family"]
    authors_df["ORCID"] = authors_df["Authors"].str["ORCID"]
    authors_df["Affiliation"] = authors_df["Authors"].str["affiliation"]
    authors_df.pop("Authors")#We remove the authors' column
    authors_df["counter2"] = authors_df.groupby("index").cumcount() + 1 #The subsequent lines of codes count the number of authors and in every cell and create columns accordingly
    authors_df = authors_df.pivot(index="index", columns=["counter2"])
    authors_df.columns = [f"{a}_{b}" for a, b in authors_df.columns]
    authors_df = authors_df[sorted(authors_df, key=lambda c: int(c.split("_")[-1]))]
    return authors_df

def clean_affiliations(authors_df, affiliation): #This function cleans the affiliations on the authors dataframe
    i = 4 #increments for looping
    j = 1
    #The subsequent loop converts the affiliations into a comma separated string
    while (i < len(authors_df.axes[1]) - 1):
        for index, row in authors_df.iterrows():
            affiliation_list = row['Affiliation_{}'.format(j)]
            affiliation_string = ""
            if (isinstance(affiliation_list, list)):
                for x in affiliation_list:
                    if (len(x) > 0):
                        affiliation_string += x['name'] + ','
                affiliation_string = affiliation_string[:len(affiliation_string) - 1]
                row['Affiliation_{}'.format(j)] = affiliation_string
        j += 1
        i += 4
    #This part puts AUC-Affiliated author among the first four
    for index, row in authors_df.iterrows():
        i = 4
        j = 1
        while (i < len(authors_df.axes[1]) - 1):
            if (j <= 4 and str(row['Affiliation_{}'.format(j)]).find(affiliation) != -1):
                break
            elif (j > 4 and str(row['Affiliation_{}'.format(j)]).find(affiliation) != -1):
                temp = row['Affiliation_{}'.format(j)]
                row['Affiliation_{}'.format(j)] = row['Affiliation_4']
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
            i += 4
            j += 1
    for index, row in authors_df.iterrows():
        if(row['Given_1']==None):
            i = 1
            j = 1
            while (i < len(authors_df.axes[1]) - 5):
                row['Given_{}'.format(j)] = row['Given_{}'.format(j+1)]
                row['Family_{}'.format(j)] = row['Family_{}'.format(j + 1)]
                row['ORCID_{}'.format(j)] = row['ORCID_{}'.format(j + 1)]
                row['Affiliation_{}'.format(j)] = row['Affiliation_{}'.format(j + 1)]
                row['Given_{}'.format(j+1)] = ""
                row['Family_{}'.format(j+1)] = ""
                row['ORCID_{}'.format(j+1)] = ""
                row['Affiliation_{}'.format(j+1)] = ""
                i = i + 4
                j = j + 1
    return authors_df

def populate_upload_df(scopus_df, unpaywall_df, authors_df): #This function creates and populates the upload df
    upload_df = unpaywall_df[['doi']].copy()
    upload_df = upload_df.reset_index(drop=True)
    upload_df['title'] = unpaywall_df['title'].values
    upload_df['document_type'] = unpaywall_df['genre'].values
    upload_df['publication_date'] = unpaywall_df['published_date'].values
    upload_df['fulltext_url'] = unpaywall_df['first_oa_location.url_for_pdf'].values
    upload_df['keywords'] = np.NaN
    upload_df['abstract'] = np.NaN
    upload_df['author1_fname'] = authors_df['Given_1'].values
    upload_df['author1_mname'] = np.NaN
    upload_df['author1_lname'] = authors_df['Family_1'].values
    upload_df['author1_suffix'] = np.NaN
    upload_df['author1_email'] = np.NaN
    upload_df['author1_institution'] = authors_df['Affiliation_1'].values
    upload_df['author1_is_corporate'] = False
    upload_df['author2_fname'] = authors_df['Given_2'].values
    upload_df['author2_mname'] = np.NaN
    upload_df['author2_lname'] = authors_df['Family_2'].values
    upload_df['author2_suffix'] = np.NaN
    upload_df['author2_email'] = np.NaN
    upload_df['author2_institution'] = authors_df['Affiliation_2'].values
    upload_df['author2_is_corporate'] = False
    upload_df['author3_fname'] = authors_df['Given_3'].values
    upload_df['author3_mname'] = np.NaN
    upload_df['author3_lname'] = authors_df['Family_3'].values
    upload_df['author3_suffix'] = np.NaN
    upload_df['author3_email'] = np.NaN
    upload_df['author3_institution'] = authors_df['Affiliation_3'].values
    upload_df['author3_is_corporate'] = False
    upload_df['author4_fname'] = authors_df['Given_4'].values
    upload_df['author4_mname'] = np.NaN
    upload_df['author4_lname'] = authors_df['Family_4'].values
    upload_df['author4_suffix'] = np.NaN
    upload_df['author4_email'] = np.NaN
    upload_df['author4_institution'] = authors_df['Affiliation_4'].values
    upload_df['author4_is_corporate'] = False
    upload_df['all_authors'] = np.NaN
    upload_df['disciplines'] = np.NaN
    upload_df['comments'] = np.NaN
    upload_df['custom_citaion'] = np.NaN
    upload_df['department'] = np.NaN
    upload_df['department_2'] = np.NaN
    upload_df['department_3'] = np.NaN
    upload_df['department_4'] = np.NaN
    upload_df['department_5'] = np.NaN
    upload_df['embargo_date'] = "0"
    upload_df['fpage'] = np.NaN
    upload_df['funding_number'] = np.NaN
    upload_df['fundref'] = np.NaN
    upload_df['identifier'] = np.NaN
    upload_df['issnum'] = np.NaN
    upload_df['lpage'] = np.NaN
    upload_df['orcid'] = np.NaN
    upload_df['program'] = np.NaN
    upload_df['season'] = np.NaN
    upload_df['pubmedid'] = np.NaN
    upload_df['scopus_id'] = np.NaN
    upload_df['volnum'] = np.NaN
    upload_df['translator'] = np.NaN
    upload_df['source_publication'] = unpaywall_df['journal_name'].values
    upload_df['web_address'] = np.NaN
    #filling by DOI match from SCOPUS DF
    for index, row in upload_df.iterrows():
        if (len(scopus_df[scopus_df['doi'] == row['doi']]['dc:identifier'].tolist()) > 0):
            upload_df.at[index, 'identifier'] = str(scopus_df[scopus_df['doi'] == row['doi']]['dc:identifier'].tolist()[0])[10:]
        if (len(scopus_df[scopus_df['doi'] == row['doi']]['prism:volume'].tolist()) > 0 and not math.isnan(
                scopus_df[scopus_df['doi'] == row['doi']]['prism:volume'].tolist()[0])):
            value = str(scopus_df[scopus_df['doi'] == row['doi']]['prism:volume'].tolist()[0])
            upload_df.at[index, 'volnum'] = value[:len(value) - 2]
        if (len(scopus_df[scopus_df['doi'] == row['doi']]['pubmed-id'].tolist()) > 0 and not math.isnan(
                scopus_df[scopus_df['doi'] == row['doi']]['pubmed-id'].tolist()[0])):
            value = str(scopus_df[scopus_df['doi'] == row['doi']]['pubmed-id'].tolist()[0])
            upload_df.at[index, 'pubmedid'] = value[:len(value) - 2]
        if (len(scopus_df[scopus_df['doi'] == row['doi']]['prism:pageRange'].tolist()) > 0):
            pages = str(scopus_df[scopus_df['doi'] == row['doi']]['prism:pageRange'].tolist()[0]).split('-')
            if(len(pages)>1):
                upload_df.at[index, 'fpage'] = pages[0]
                upload_df.at[index, 'lpage'] = pages[1]
            else:
                upload_df.at[index, 'fpage'] = pages[0]
                upload_df.at[index, 'lpage'] = pages[0]
    upload_df = upload_df.replace(to_replace='None', value=np.nan)
    return upload_df

def clean_files(upload_df):
    logging.shutdown()
    username = getpass.getuser()
    upload_df.to_excel("C:\\Users\\{}\\Desktop\\FOUNT_upload.xls".format(username), index=False)
    os.remove('config.json')
    os.remove('unpaywall_cache')
    os.remove('dump.json')
    shutil.rmtree('data')
    shutil.rmtree('logs')
    print("Program completed successfully.\nThe data is on your desktop as a file called 'FOUNT_upload.xls'.")

affiliation = ""
scopus_search(get_credentials(),affiliation)
scopus_df = get_scopus_dataframe()
unpaywall_df = unpaywall_search(scopus_df)
authors_df = clean_authors(unpaywall_df)
authors_df = clean_affiliations(authors_df,affiliation)
upload_df = populate_upload_df(scopus_df,unpaywall_df,authors_df)
clean_files(upload_df)