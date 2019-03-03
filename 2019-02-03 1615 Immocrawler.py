
# coding: utf-8

# In[1]:


# -*- coding: cp1252-*-

####PRELIMINARIES####

#module import#
from urllib.request import urlopen
from urllib.error import HTTPError
from urllib.error import URLError
from bs4 import BeautifulSoup
import pandas as pd
import re
import datetime
import numpy as np

class ImmoCrawler:
    
    def __init__(self,  types_and_regions, domain="https://www.immobilienscout24.de/Suche/S-T"):
        """Initialize the immo crawler object."""
                
        self._current_datetime = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self._type_list, self._region_list = types_and_regions        
        self._domain = domain
        self._data = pd.DataFrame()       
        
    
    def _get_data(self, url):
        """Get data from link."""        
        try:
            url_raw = url#save url as string for real estate type
            url = urlopen(url)
        except HTTPError as e:
            return None
        except URLError as e:
            return None
        try:
            site_extract = BeautifulSoup(url.read(), "lxml")
            rawdata_extract = site_extract.find_all("div", {"class":"result-list-entry__data"})#extract every result box
        except AttributeError as e:
            return None
        
        price = []
        size = []
        location = []
        ownership = []
        immo_type = []
        #print(rawdata_extract)
        for i in range(1,len(rawdata_extract)):
            try:
                price.append(rawdata_extract[i].find_all("dd")[0].get_text().strip())#extract price
            except:
                price.append(None)
            try:
                size.append(rawdata_extract[i].find_all("dd")[1].get_text().strip())#extract size
            except:
                size.append(None)
            try:
                location.append(rawdata_extract[i].find_all("div", {"class":"result-list-entry__address"})[0].get_text().strip())#extract location
            except:
                location.append(None)

            if "/Wohnung" in url_raw:
                immo_type.append("Wohnung")
            elif "/Haus" in url_raw:
                immo_type.append("Haus")
            elif "/Grundstueck" in url_raw:
                immo_type.append("Grundstueck")
            else:
                immo_type.append(None)

            if "-Miete" in url_raw:
                ownership.append("Miete")
            elif "-Kauf" in url_raw:
                ownership.append("Kauf")
            else:
                ownership.append(None)
            
        self._data = self._data.append(pd.DataFrame({"price":price,
                                                  "size":size,
                                                  "location":location,
                                                  "real_estate":immo_type,
                                                  "ownership":ownership}),
                                    ignore_index=True)            
        
        
    def immo_crawl(self):
        """Crawl the given sites."""
        
        def get_max(url):
            """Get the last link for every asset type."""
            try:
                #print ('Trying url', url)
                url = urlopen(url)                
            except:
                print("Fehler beim Oeffnen der Website {}".format(url))
            try:
                site_extract = BeautifulSoup(url.read(), "lxml")
            except:
                print("Fehler beim Einlesen in BeautifulSoup der Website {}".format(url))            
            try:
                options_min_max = []
                for option in site_extract.find_all("option"):
                    options_min_max.append(option["value"])
                
                if len(options_min_max) == 0:
                    options_min_max = [1,1]
                else:
                    options_min_max = [1, int(options_min_max[-1])]
                
            except:
                print("Fehler beim Loop")            
            try:               
                link_list = [1, int(options_min_max[1])]                                
            except:
                print("Fehler beim Erstellen der Link Liste:\n" + str(link_list))
            else:
                return link_list   
            
        
        max_dict = {}#initialize dictionary for maximum values of links

        site_kreis_list = list()
        for site in self._type_list:
            for kreis in self._region_list:
                site_kreis=site+kreis
                site_kreis_list.append(site_kreis)
                max_dict[site_kreis] = get_max(domain+site_kreis)#associate maximal link value with specific sub-site
                #print("For Kreis ", kreis, "have max_dict", max_dict)

        link_list_full = []#initialize list for full links to crawl#
        for site_kreis in max_dict:
            #print("For Kreis ", kreis, "have max_dict", max_dict)
            for i in range(1,max_dict[site_kreis][-1]+1):
                link_list_full.append(domain+"/P-"+str(i)+site_kreis)#populate link_list_full
        link_count = 1#start for progress indicator

        len_link_list_full = len(link_list_full)#end for progress indicator
        for link in link_list_full:
            print("Crawling: "+link+" (link #"+str(link_count)+" of "+str(len_link_list_full)+")")#print progress
            link_count += 1#add to progress indicator
            self._get_data(link)           
        

    def clean_and_save_data(self):
        """Clean the data."""        
        
        #self._data.to_csv("immoscout_data_raw_"+self._current_datetime +".csv", sep=";", index=False)
        
        def clean_pricesize(data):
            data = data.replace("€", "")
            data = data.replace(".", "")
            data = data.replace("m²", "")
            data = re.sub(re.compile(" \D.*"), "", data)
            data = data.strip()
            #data = pd.to_numeric(data)            
            return data

        def get_firstlayer(data):
            fist_layer = data.split(",")[0]
            return fist_layer.strip()

        def get_lastlayer(data):
            last_layer = data.split(",")[-1]
            return last_layer.strip()

        self._data = self._data.dropna(axis=0)
        self._data["price"] = self._data["price"].apply(clean_pricesize)
        self._data["size"] = self._data["size"].apply(clean_pricesize)
        self._data["location_first"] = self._data["location"].apply(get_firstlayer)
        self._data["location_last"] = self._data["location"].apply(get_lastlayer)
        self._data["crawled"] = self._current_datetime        
        self._data.to_csv("immoscout_data_clean_"+self._current_datetime +".csv", sep=";", index=False)
    
    def add_data_to_db(self, db_name = 'immoscout_data_clean_DB.csv'):
        """Add data to master db."""
        
        df = pd.read_csv(db_name, sep=";")
        df = df.append(self._data, sort=True)
        df = df.drop_duplicates()
        df.to_csv(db_name, sep=";", index=False)       


# In[2]:


#define top level domain
domain="https://www.immobilienscout24.de/Suche/S-T"

types_and_regions = [[#"/Wohnung-Miete",
             #"/Haus-Miete",
             #"/Wohnung-Kauf",
             "/Haus-Kauf",
             "/Grundstueck-Kauf"],
            ["/Bayern/Fuerstenfeldbruck-Kreis",
             "/Bayern/Dachau-Kreis",
             "/Bayern/Starnberg-Kreis",
             "/Bayern/Freising-Kreis",
             "/Bayern/Erding-Kreis",
             "/Bayern/Ebersberg-Kreis",
             "/Bayern/Muenchen-Kreis",
            ]
]    


# In[3]:


session = ImmoCrawler(types_and_regions)
session.immo_crawl()
session.clean_and_save_data()
session._data
session.add_data_to_db()

