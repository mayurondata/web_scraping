from bs4 import BeautifulSoup
import requests
import os
import json
import pandas as pd
import logging
import dateparser
 
# storing fetched data in lists
 
 
config_fie_path = r'config.json'
 
# reading data from config.json
def yahoo_scrapper(config_fie_path):
   
    logging.basicConfig(level = logging.DEBUG , filename='log.txt' , format='%(asctime)s %(levelname)s:%(message)s')
   
    headline_list = []
    preview_list = []
    source_list = []
    date_list = []
    news_link_list = []
    search_string = []
    scrapped_data ={}
    with open(config_fie_path,'r') as fp:
        configs = json.load(fp)
 
    companies = configs['companies']
    keywords = configs['keywords']
    number_of_pages = configs['number_of_pages']    
 
    try:
        for c in range(0, len(companies)):
            for k in range(0,len(keywords)):
                input_parameter_for_url = f"{companies[c]}{keywords[k]}"
               
               
 
                yahoo_template = 'https://in.news.search.yahoo.com/search?q={}'
                yahoo_final_url = yahoo_template.format(input_parameter_for_url)
 
 
                response = requests.get(yahoo_final_url)
                html_text = response.text
                soup = BeautifulSoup(html_text , 'lxml')
               
                for page in range(number_of_pages):
                    all_news = soup.select('ol>li>div>ul')
                   
                    for news in all_news:
                        headlines = news.select('ul>li>h4>a')
                        for headline in headlines:
                            headline_list.append(headline.text)
                       
                        previews = news.select('li>p')
                        for preview in previews:
                            preview_list.append(preview.text)
                           
                        sources = news.find('span',class_ = 's-source mr-5 cite-co')
                        for source in sources:
                            source_list.append(source.text)
                           
                        dates = news.find('span',class_ = 'fc-2nd s-time mr-8')
                        for date in dates:
                            date = date.text.replace(" . ", "")
                            date  = dateparser.parse(date)
                            date_list.append(date)
                           
                        links = news.select("h4>a")
                        for link in links:
                            news_link_list.append(link['href'])
                            print('hhhhhh')
                        search_string.append(input_parameter_for_url)
                           
                   
                next_page_response = requests.get(soup.find('a',class_='next')['href'])
                soup = BeautifulSoup(next_page_response.content ,'lxml')
                   
            scrapped_data = {
            'Header': source_list,
            'Title' : headline_list,
            # 'Preview' : preview_list,
            'Timestamp' : date_list,
            'serach_string' : search_string,
            'News link' : news_link_list
            }
           
            # print(scrapped_data)
           
            # dataframe = pd.DataFrame.from_dict(scrapped_data,orient='index')
            dataframe = pd.concat({k: pd.Series(v) for k ,v in scrapped_data.items()},axis=1)
            # print(dataframe)
           
        return dataframe
   
    except:
        logging.error('Error occured')
   
 
result = yahoo_scrapper(config_fie_path)
print(result)
