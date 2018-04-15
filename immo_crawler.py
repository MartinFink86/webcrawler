# -*- coding: utf-8 -*-

####PRELIMINARIES####

#module import#
from urllib.request import urlopen
from urllib.error import HTTPError
from urllib.error import URLError
from bs4 import BeautifulSoup
from pandas import DataFrame
import re
import datetime


#get current date#
current_datetime_master = datetime.datetime.now()
current_datetime = current_datetime_master.strftime("%Y%m%d_%H%M%S")

###PARAMETERS
#define sub-sites#
site_list = [#"/Wohnung-Miete",
             #"/Haus-Miete",
             #"/Wohnung-Kauf",
             "/Haus-Kauf",
             "/Grundstueck-Kauf"]

#define top level domain
domain="https://www.immobilienscout24.de/Suche/S-T"

#define the regions where to look
kreis_list = ["/Bayern/Fuerstenfeldbruck-Kreis",
             "/Bayern/Dachau-Kreis"]

#initialize dataframe
immoscout_data = DataFrame()


####EXTRACTING LINKS FOR CRAWLER####

#get the last link for every asset type#
def get_max(url):
    try:
        url = urlopen(url)
    except:
        print("Fehler beim Oeffnen der Website")
    try:
        site_extract = BeautifulSoup(url.read(), "lxml")
    except:
        print("Fehler beim Einlesen in BeautifulSoup")
    try:
        max_link = max([int(n["value"]) for n in site_extract.find_all("option")])#get the maximum value for links in a specific sub-site
    except:
        print("Fehler beim Loop")
    else:
        return max_link



####EXTRACT DATA FROM EVERY SINGLE LINK####

def get_data(url):
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
    global immoscout_data#use global dataframe
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
    immoscout_data = immoscout_data.append(DataFrame({"price":price,
                                                  "size":size,
                                                  "location":location,
                                                  "real_estate":immo_type,
                                                  "ownership":ownership}),
    ignore_index=True)




####START CRAWLER####

def immo_crawl(site_list:list, kreis_list:list):
    max_dict = {}#initialize dictionary for maximum values of links

    site_kreis_list = list()
    for site in site_list:
        for kreis in kreis_list:
            site_kreis=site+kreis
            site_kreis_list.append(site_kreis)
            max_dict[site_kreis] = get_max(domain+site_kreis)#associate maximal link value with specific sub-site

    link_list_full = []#initialize list for full links to crawl#
    for site_kreis in max_dict:
        for i in range(1,max_dict[site_kreis]):
            link_list_full.append(domain+"/P-"+str(i)+site_kreis)#populate link_list_full
    link_count = 1#start for progress indicator

    len_link_list_full = len(link_list_full)#end for progress indicator
    for link in link_list_full:
        print("Crawling: "+link+" (link #"+str(link_count)+" of "+str(len_link_list_full)+")")#print progress
        link_count += 1#add to progress indicator
        get_data(link)



immo_crawl(site_list, kreis_list)#start crawler


####PROCESS DATA####

immoscout_data.to_csv("immoscout_data_raw_"+current_datetime+".csv", sep=";", index=False)#export unprocessed data


#clean data#
def clean_pricesize(data):
    data = data.replace("€", "")
    data = data.replace(".", "")
    data = data.replace("m²", "")
    data = re.sub(re.compile(" \D.*"), "", data)
    data = data.strip()
    return data


def get_firstlayer(data):
    fist_layer = data.split(",")[0]
    return fist_layer.strip()

def get_lastlayer(data):
    last_layer = data.split(",")[-1]
    return last_layer.strip()

immoscout_data_clean = immoscout_data.dropna(axis=0)
immoscout_data_clean["price"] = immoscout_data_clean["price"].apply(clean_pricesize)
immoscout_data_clean["size"] = immoscout_data_clean["size"].apply(clean_pricesize)
immoscout_data_clean["location_first"] = immoscout_data_clean["location"].apply(get_firstlayer)
immoscout_data_clean["location_last"] = immoscout_data_clean["location"].apply(get_lastlayer)

immoscout_data_clean["crawled"] = current_datetime_master.strftime("%Y-%m-%d %H:%M:%S")

immoscout_data_clean.to_csv("immoscout_data_clean_"+current_datetime+".csv", sep=";", index=False)
