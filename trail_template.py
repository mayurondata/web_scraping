# scraper.py
import json
import pandas as pd
import requests
from bs4 import BeautifulSoup
import logging
import dateparser
# import mylib1
 


class Scrapper:
    def __init__(self):
        self.config_file_path = r'config.json'
    
    def yahoo_scrapper(self,config_file_path):
        self.config_file_path = config_file_path
        logging.basicConfig(level = logging.DEBUG , filename='log.txt' , format='%(asctime)s %(levelname)s:%(message)s')

        engine ='YAHOO'
        headline_list = []
        preview_list = []
        source_list = []
        date_list = []
        news_link_list = []
        search_string = []
        scrapped_data ={}
        search_engine = []
        with open(config_file_path,'r') as fp:
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
                            
                            search_string.append(input_parameter_for_url)
                             
                            search_engine.append(engine) 
                            
                    
                    next_page_response = requests.get(soup.find('a',class_='next')['href'])
                    soup = BeautifulSoup(next_page_response.content ,'lxml')
                    
                scrapped_data = {
                'Header': source_list,
                'Title' : headline_list,
                # 'Preview' : preview_list,
                'Timestamp' : date_list,
                'search_string' : search_string,
                'Search Engine' : search_engine,
                'News link' : news_link_list
                }
            
                dataframe = pd.concat({k: pd.Series(v) for k ,v in scrapped_data.items()},axis=1)
            
            return dataframe
    
        except:
            logging.error('Error occured')
            
            
    def fetch_data_google(self,config_file_path):
        self.config_file_path = config_file_path
        engine = "GOOGLE"
        with open(config_file_path,'r') as fp:
            configs = json.load(fp)

        companies = configs['companies']
        keywords = configs['keywords']
        max_pages = configs['number_of_pages'] 
        search_string = []   
        # date_list = []
        try:
            for c in range(0,len(companies)):
                for k in range(0,len(keywords)):
                    input_parameter_for_url = f"{companies[c]}{keywords[k]}"
                    
                    
                    base_url = 'https://www.google.com/search?q='
                    start_parameter = "&start="
                    result_data = []
                    search_engine = []
                    for page_number in range(max_pages):
                                
                                url = f"{base_url}{input_parameter_for_url}{start_parameter}{page_number * 10}"
                                # print(url)
                                response = requests.get(url)
                                if response.status_code == 200:
                                    soup = BeautifulSoup(response.text, 'html.parser')
                                    data_1 = soup.find_all("div", class_="BNeawe UPmit AP7Wnd lRVwie")
                                    news_soup = soup.find_all("div", class_="BNeawe vvjwJb AP7Wnd") 
                                    timestamp_soup = soup.find_all("span" , class_ = "r0bn4c rQMQod")
                                    data = soup.find_all(class_ = "Gx5Zad fP1Qef xpd EtOod pkphOe")
                                    
                                    link = []
                                    for i in data:
                                        k=i.find('a')['href']
                                        for j in k.split(","):
                                            link.append(j)
                                            search_string.append(input_parameter_for_url)
                                            search_engine.append(engine)
                                    header11 = [i.text for i in data_1]
                                    title = [j.text for j in news_soup]
                                    abc = [t.text for t in timestamp_soup] 
                                    timestt = [dateparser.parse(abc[i])  for i in range(len(abc)) if i % 2 == 0]

                                    
                                    # search_string= [[t] for t in range(len(input_parameter_for_url))]
                                    # serach_string = serach_string.append(input_parameter_for_url)
                                    # search_engine = ["GOOGLE " for val in range(len(timestt))]
                                    # result_data.extend(list(zip(header11, title, timestt)))
                                    # print(result_data)
                                    
                                    scrapped_Data = {
                                        'Header': header11,
                                        'Title' : title,
                                        # 'Preview' : preview_list,
                                        'Timestamp' : timestt,
                                        'search_string' : search_string,
                                        'Search Engine' : search_engine,
                                        'News link' : link  
                                        
                                    }
                                    
                                    dataframe = pd.concat({k: pd.Series(v) for k ,v in scrapped_Data.items()},axis=1)
                                    
                                    return dataframe
        except :
                    logging.error(f"Failed to fetch page {page_number}")
        # return pd.concat({'Header' :header11 , 'Title': title , 'Timestamp' : timestt ,'search_string' : search_string , 'Search_Engine' :search_engine, "URL" : link})
        
      
    def fetch_data_bing(self,config_file_path):
        engine = 'BING'
        title =[]
        subhead = []
        timestamp1 = []
        url2 = []
        date1 =[]
        count = 1
        search_string = []
        search_engine = []
        self.config_file_path =config_file_path
        
        with open(config_file_path,'r') as fp:
            configs = json.load(fp)
            
        companies = configs['companies']
        keywords = configs['keywords']
        number_of_pages = configs['number_of_pages'] 
            
        try:     
            for c in range(0 ,len(companies)):
                for k in range(0 ,len(keywords)):
                    input_parameter_for_url = f"{companies[c]}{keywords[k]}"
                    
                    bing_template = 'https://www.bing.com/news/search?q={}'
                    bing_final_url = bing_template.format(input_parameter_for_url)
                    part_url = '/urlnews/infinitescrollajax?page='    
                while count<=number_of_pages:
                    
                    url1 = f'{bing_final_url}{part_url}{number_of_pages}'
                    base_url = requests.get(url1)
                    soup = BeautifulSoup(base_url.text,"html.parser")
                    data = soup.find_all(class_ = "news-card newsitem cardcommon")
                    for i in data:
                        title.append(i['data-title'])
                        subhead.append(i['data-author'])
                        url2.append(i['data-url'])
                        date = i.find_all("span" )[2]
                        temp = dateparser.parse(date["aria-label"]).date()
                        search_string.append(input_parameter_for_url)
                        date1.append(temp.strftime("%Y-%m-%d"))
                        search_engine.append(engine)
                    count += 1
                    scrapped_Data = {
                                            'Header': subhead,
                                            'Title' : title,
                                            # 'Preview' : preview_list,
                                            'Timestamp' : date1,
                                            'search_string' : search_string,
                                            'Search Engine' : search_engine,
                                            'News link' : url2  
                                            
                                        }
                                        
                    dataframe = pd.concat({k: pd.Series(v) for k ,v in scrapped_Data.items()},axis=1)
            return dataframe 
        except:
            print(logging.error('Error occured'))
            
     
      
        
yahoo_frame = Scrapper()
result_yahoo = yahoo_frame.yahoo_scrapper(r'D:\Python assignments\Assignment_1\config.json')
print(result_yahoo)
google_frame = Scrapper()
result_google = google_frame.fetch_data_google(r'D:\Python assignments\Assignment_1\config.json')
print(result_google)
print(result_google.columns)
bing_frame = Scrapper()
result_bing = bing_frame.fetch_data_bing(r'D:\Python assignments\Assignment_1\config.json')
print(result_bing)

frames = [result_yahoo,result_google,result_bing]
final_frame = pd.concat(frames)
print(final_frame)

final_frame.to_csv('Merge.csv')