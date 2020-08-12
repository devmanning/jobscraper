""" This module is written to scrape job posting for Aircraft maintenance jobs posted in Indeed Canada.
    The scraped jobs are added to a pandas data frame and then loaded to an database using ETL scripts.
    The ETL process is handled in a separate script called ETL.py. ETL.py imports this module.

"""
# Todo: import required libraries

import requests

import bs4

from bs4 import BeautifulSoup  # this the package that has BeautifulSoup method

import pandas as pd

import time

import datetime

from datetime import date, timedelta

'''
Resources that can be used:
 ℹ️ The program logic is obtained from :https://medium.com/@msalmon00/web-scraping-job-postings-from-indeed-96bd588dcb4b
 To write a dataframe to csv file: dataFrame.to_csv (r'/home/dev/pythonprojects/virtualenv/python3.8.2/jobscraper
                                                        /query.csv', index = False,header=True)
 # to test output in the computer screen use - print(soup.prettify())    
'''


# Prepare to get the content of the website

# To do: Defining variables that will be required by the script

pagenum=0

url="https://ca.indeed.com/jobs?q=Aircraft+Maintenance&l=Canada&start="+str(pagenum)

#conducting a request of the stated URL above:
page = requests.get(url)    # page object stores the unparsed raw HTML document as a long string
page.raise_for_status()		#  returns an HTTPError if the HTTP request returned an unsuccessful status code.

# TODO: Process the page object, specifying a desired format of “page” using the html parser - this allows python to read the various components of the page

soup = BeautifulSoup(page.text, 'html.parser')


# defining all the functions, that will be used in the main scraping script (the while loop)


# To do: Harvest Job title

def harvestjobtitle(block):
    """ Function to iterate through the anchor tags that have a class

        jobtitle and harvest the title attribute of the anchor tag
    """
    jobtitle=''
    for title in block.find_all('a',{'class':['jobtitle']}):
        jobtitle=(title['title'])		#title here is one of the attributes of <a> tag
    return(jobtitle)
    
    


""" To do: Harvest  Company Name-
    ℹ️Learning Moment:Company names , as most would appear in <span> tags, with “class”:”company”. 
    Rarely, however, they will be housed in <span> tags with “class”:”result-link-source”.
"""

def harvestcompanyname(block):
    """ Function to iterate through the span tags that have a class

        company and harvest the text attribute of the span tag
    """
    companyname= ''
    
    companies= block.find_all('span',{'class':['company']})
    
    if len(companies) >0:
        for company in companies:
            companyname=(company.text.strip())		#title here is one of the attributes of <a> tag
    else:
        for company in block.find_all('span',{'class':['result-link-source']}):
            companyname=(company.text.strip())
    return(companyname)
    

# To do: Harvest Location
""" Locations are either located under the <span> tags or <div> tags. location text is within “class” : “location” 
    attributes.
"""

def harvestlocation(block):
    """ Function to iterate through the span tags & the DIV tags that have a class

        company and harvest the text attribute of the span/div tag
    """
    location=''
    spans = block.find_all('span', attrs={'class': 'location'})
    if len(spans) > 0:
        for span in spans:
            location=(span.text.strip())
    else :
        for div in block.find_all('div', attrs={'class': 'location'}):
            location=(div.text.strip())
    return(location)

# To do: Harvest Salary - for future

# To do: Posted date:

""" Posted date is a text attribute of span tag with class:date.The text is further processed
    using if/else logic to return a psoting date. Text content is as current as August-12-2020
    this piece of code needs maintenance-to update the if else logic
"""

def harvestpostingdates(block):
    """ Function to iterate through the span tags with class date,
        strips the text of the attribute,the date value is a calculated 
        value from the text using (if/else contro; flow) it returns a date string
    """
    
    spans=block.find_all('span',attrs=({'class':"date"})) 
    postingdate=''
    
    for span in spans :
        text=(span.text.strip())
        
        days = [int (num) for num in text.split() if num.isdigit()]
        
        postdate= date.today()
        
        if len(days)>0 :
            postdate = date.today() - timedelta(days[0])
            
        elif len(days)==0 and text == 'today':
            days=[0]
            postdate = date.today() - timedelta(days[0])
            
        elif len(days)==0 and text =='30+ days ago':
            days=[30]
            postdate = date.today() - timedelta(days[0])
            
        elif len(days)==0 and text =='justposted':
            days=[0]
            postdate = date.today() - timedelta(days[0])
            
        postingdate=(str(postdate))
            
    return(postingdate)
    
    
    

def harvestpages(block):
    ''' This function is to help in iterating throught all the job posting.It takes the 
        DIV id='searchCountPages' value &returns the number of jobs posted in the site as an integer value. 
    '''    
    dates = block.find('div', id='searchCountPages').text.strip()
    page = [int (dt) for dt in dates.split() if dt.isdigit()]
    pagenumber=page[1] #//12
        
    return(pagenumber)
    

        
   
    

# To do: Store data in a Pandas dataframe:

# Defining the dataframe & initializing it

columns = ['job_title', 'company_name', 'location','postingdate']

jobs_df = pd.DataFrame(columns = columns)


# Final scraping procedure

''' the jobnumber variable stores the number of jobs posted in the site.it is used for the break statement
    when the iteration reaches the the number of job posts it quits from both the while & for loop.

'''
jobnumber = harvestpages(soup)

pagenumber = 50 # you can chnage this number as the while loop is dicated by a break statement using the jobnumber value
                # froom the above step

num=0
var=0
    
while pagenum <= pagenumber :
    

    for div in soup.find_all(name='div', attrs={'class':'row'}):    
        joblist=[]
        joblist.append(harvestjobtitle(div))
        joblist.append(harvestcompanyname(div))
        joblist.append(harvestlocation(div))
        joblist.append(harvestpostingdates(div))        
        jobs_df.loc[num] = joblist
        num = (len(jobs_df) + 1) 
        if num > jobnumber:
            break
        
    pagenum+=1
    var=len(jobs_df)
      
    if var >= jobnumber:
        break
    
    url = "https://ca.indeed.com/jobs?q=Aircraft+Maintenance&l=Canada&start="+str(pagenum*10)
    time.sleep(1)  #ensuring at least 1 second between  grabs  
    
    page = requests.get(url)    # page object stores the unparsed raw HTML document as a long string
    page.raise_for_status()		#  returns an HTTPError if the HTTP request returned an unsuccessful status code.
    soup = BeautifulSoup(page.text, 'html.parser') 
    
    
# returning the data frame when included in other scripts:

jobs_df



