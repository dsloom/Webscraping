import os
import re
from pathlib import Path
from pprint import pprint

import mysql.connector
import requests
from bs4 import BeautifulSoup

os.chdir(Path(__file__).parent)


mydb = mysql.connector.connect(
    host = "localhost", # or the ip of the mysql server
    user = "root",
    password = "root",
    database = "gans"
)
    
city_info=[]
API_KEY="e2f672798f504d6effdaf5b0acdce040"
METRIC="metric"


def  po(city):

    url=f"https://en.wikipedia.org/wiki/{city}"  
    page=requests.get(url)
    soup=BeautifulSoup(page.content,"lxml")                            
    info_rows = soup.select("#mw-content-text > div.mw-parser-output > table.infobox.ib-settlement.vcard > tbody > tr.mergedrow")
    for node in info_rows:
        headings = node.select("th.infobox-label")
        if headings == None:
            continue
        header_text = headings[0].get_text()
        if re.match(r'.*(city|total).*', header_text, re.IGNORECASE):
            data_text = node.select("td.infobox-data")[0].get_text()
            if "km" not in data_text:
                return data_text


#def population():

def geography():

    cities=["Berlin","Aachen","Hamburg","Stuttgart"]
    for city in cities:
        wiki_url=f"https://en.wikipedia.org/wiki/{city}"  
        page=requests.get(wiki_url)
        soup=BeautifulSoup(page.content,"lxml")
        all_p = soup.find(class_="infobox-data")
        for  link in all_p.find_all("a"):
            country=link.get('title')
        #population_node = soup.select("#mw-content-text > div.mw-parser-output > table.infobox.ib-settlement.vcard > tbody > tr:nth-child(22) > td")
        #Population=population_node[0].get_text()
        #print(Population) 
        
                #print(city, data_text)    
        #Population = population(city)
        open_weather_url=f"https://api.openweathermap.org/data/2.5/forecast?q={city}&appid={API_KEY}&units={METRIC}"
        response=requests.get(open_weather_url)
        data=response.json() 
        population=data["city"]["population"]
        country_code=data["city"]["country"]

   


        latitude=soup.find("span","latitude").text.strip()
        longitude=soup.find("span","longitude").text.strip()
        city_geography={
            "city":city,
            "country":country,
            "country_code":country_code,
            "population":population,
            "latitude":convert_deg_to_decimal_deg(latitude),
            "longitude":convert_deg_to_decimal_deg(longitude)
        }
        city_info.append(city_geography)
    
    #pprint(city_info)
    return city_info

def reset_db():
    cursor = mydb.cursor() 

    # 3. SQL Statement
    sql = "DELETE  FROM cities;"
    # 4. Execute (RUN) the SQL Statement
    cursor.execute(sql)
    mydb.commit()   
def insert_city_info_into_db(city,country,country_code,population,latitude,longitude):
      # 2. create a cursor (like a servent)
    cursor = mydb.cursor() 

    # 3. SQL Statement
    sql = "INSERT INTO cities (city,country,country_code,population,latitude,longitude) VALUES (%s,%s,%s,%s,%s,%s);"
    data = (city,country,country_code,population,latitude,longitude)
    # 4. Execute (RUN) the SQL Statement
    cursor.execute(sql, data)

    # 6. Commit the changes  --> store the changes permanently
    mydb.commit()


    return cursor.lastrowid  

#dd = parse_dms("78°55'44.33324'N" )


def convert_deg_to_decimal_deg(degree_location):
    #x = x.upper()
    #print(z)
    deg, time_portion = degree_location.split("°")
    time_parts = time_portion.split("′")

    minutes, seconds =  0, 0
    direction = time_parts[-1]
    if len(time_parts) >= 2:
        minutes = time_parts[0]
    if len(time_parts) >= 3:
        seconds = time_parts[1]
    #print(deg)
    #print(minutes)
    #print(seconds)
    #print(direction)
    decimal_location=(float(deg) + float(minutes)/60 + float(seconds)/(60*60)) * (-1 if direction in ['W', 'S'] else 1)  
    #print(y)
    return round(decimal_location,4)
def store_data_to_db(city_details):
    for city_detail in city_details:
        insert_city_info_into_db(city_detail["city"],city_detail["country"],city_detail["country_code"],city_detail["population"],city_detail["latitude"],city_detail["longitude"])


def main():
    reset_db()
    city_details=geography()
    #print(x)
    #x=[{city:berlin,lati..:..,longi  :..},{....}]
    store_data_to_db(city_details)

main()
