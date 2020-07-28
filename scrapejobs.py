""" This module is written to scrape job postong for Aircraft maintenance jobs posted in Indeed Canada.
    The scraped jobs id added to a pandas data frame and then loaded to an database using ETL scripts.

"""

import requests

import bs4

from bs4 import BeautifulSoup  # this the package that has BeautifulSoup method

import pandas as pd

import time

# Prepare to get the content of the website

pagenum=0

url="https://ca.indeed.com/jobs?q=Aircraft+Maintenance+Engineer&l=Canada&start="+str(pagenum)

#conducting a request of the stated URL above:
page = requests.get(url)    # page object stores the unparsed raw HTML document as a long string
page.raise_for_status()		#  returns an HTTPError if the HTTP request returned an unsuccessful status code.

# TODO: Process the page object, specifying a desired format of “page” using the html parser - this allows python to read the various components of the page

soup = BeautifulSoup(page.text, 'html.parser')

# print(soup.prettify())    # to test output

# To do: Harvest Job title

def harvestjobtitle(block):
    """ Function to iterate through the anchor tags that have a class

        jobtitle and harvest the title attribute of the anchor tag
    """
    jobtitle= []
    for title in block.find_all('a',{'class':['jobtitle']}):
        jobtitle.append(title['title'])		#title here is one of the attributes of <a> tag
    return(jobtitle)
    
    


""" To do: Harvest  Company Name-Company names , as most would appear in <span> tags, with “class”:”company”. 
    Rarely, however, they will be housed in <span> tags with “class”:”result-link-source”."""

def harvestcompanyname(block):
    companyname= []
    
    companies= block.find_all('span',{'class':['company']})
    
    if len(companies) >0:
        for company in companies:
            companyname.append(company.text.strip())		#title here is one of the attributes of <a> tag
    else:
        for company in block.find_all('span',{'class':['result-link-source']}):
            companyname.append(company.text.strip())
    return(companyname)
    

# To do: Harvest Location
""" Locations are either located under the <span> tags or <div> tags. location text is within “class” : “location” 
    attributes.
"""

def harvestlocation(block):
    location=[]
    spans = block.find_all('span', attrs={'class': 'location'})
    if len(spans) > 0:
        for span in spans:
            location.append(span.text.strip())
    else :
        for div in block.find_all('div', attrs={'class': 'location'}):
            location.append(div.text.strip())
    return(location)

# To do: Harvest Salary
# To do: Posted date

def harvestpages(block):
    #dates=block.find('div',attrs=({'id':"searchCountPages"}))
    dates = soup.find('div', id='searchCountPages').text
    page = [int (dt) for dt in dates.split() if dt.isdigit()]
    pagenumber=page[1]//12
    print(pagenumber)
    return(pagenumber)
        
    
    

# To do: Store data in a Pandas dataframe:

# Defining the dataframe & initializing it
columns = ['job_title', 'company_name', 'location']
jobs_df = pd.DataFrame(columns = columns)


# Final scraping procedure


pagenumber = harvestpages(soup)
num=0

while pagenum < pagenumber :
    time.sleep(1)  #ensuring at least 1 second between  grabs

    for div in soup.find_all(name='div', attrs={'class':'row'}):    
        joblist=[]
        joblist.append(harvestjobtitle(div) )
        joblist.append(harvestcompanyname(div))
        joblist.append(harvestlocation(div))
        jobs_df.loc[num] = joblist
        num = (len(jobs_df) + 1) 
        
    pagenum+=1
    
    url = "https://ca.indeed.com/jobs?q=Aircraft+Maintenance+Engineer&l=Canada&start="+str(pagenum)
          
    
#appending list of job post info to dataframe at index num

print(jobs_df)
#harvestpages(div)
