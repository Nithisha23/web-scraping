pip install cssselect

#imports 

import requests
import pandas as pd
import regex as re
from lxml import html
from lxml import etree
import urllib3
import bs4
from cssselect import GenericTranslator
from scipy.stats import shapiro
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler


pd.set_option('display.max_rows',70)

httpmng = urllib3.PoolManager()

#combining the tables with the movies and the tables with the url

url_data1 = pd.read_csv("/content/drive/My Drive/rpython/url.csv")
url_data_mov = pd.read_excel(r"/content/drive/My Drive/rpython/complete_data.xlsx")
url_data = url_data1.merge(url_data_mov, on='Unique_ID')

#url_data is our intial data which has the movie name and imdb url of the movie

#scraping the data for the Release_year

def date():
  date = []
  for i in url_data["url"]:
    res = requests.get(i)
    soup = bs4.BeautifulSoup(res.text,"lxml")
    title = soup.select("title")
    date.append(re.findall("([0-9]{4})", str(title)))
  return date  
 date = date()
 date = pd.DataFrame(date, columns=["Release_year"])
  
 #scraping the data for the ratings
  
def rating():
  rating = []
  for i in url_data["url"]:
    res = requests.get(i)
    soup = bs4.BeautifulSoup(res.text,"lxml")
    info = soup.select(".ratingValue")
    more_info = re.findall("([0-9].[0-9])", str(info))
    rating.append(more_info[0])
  return rating  
 rating = rating()
 rating = pd.DataFrame(rating, columns=["rating"])
  
 #added the release data and ratings to our initial data
  
 newData = pd.concat([url_data, date, rating], axis=1)
  
#scraping the data for the budget
  
def budget():
  budget = []
  for i in url_data["url"]:
    resp = httpmng.request('GET', i)
    tagtree = html.fromstring(resp.data)
    xp_budget = "//div[@id='titleDetails']/div[7]/text()"
    imdb_budget = tagtree.xpath(xp_budget)
    budget.append(imdb_budget)
  return budget   
budget = budget()
  
#using regex to clean the data

budget_value = []
for i in budget:
  more_info = re.findall("\$([0-9]+\,[0-9]+\,[0-9]+)", str(i[1]))
  more_info = re.sub('\,', '', str(more_info))
  more_info = re.findall("([0-9]+)", str(more_info))
  budget_value.append(more_info)
Budget = pd.DataFrame(budget_value, columns=["Budget"])

#scraping the data for the Cumulative_Worldwide_Gross

worldgross = []
for i in url_data["url"]:
  resp = httpmng.request('GET', i)
  tagtree = html.fromstring(resp.data)
  xp_worldgross = "//div[@id='titleDetails']/div[10]/text()"
  imdb_worldgross = tagtree.xpath(xp_worldgross)
  #imdb_budget = imdb_budget[1]
  worldgross.append(imdb_worldgross)

#using regex to clean the Cumulative_Worldwide_Gross
  
worldgross_value = []
for i in worldgross:
  more_info = re.findall("\$([0-9]+\,[0-9].*\,[0-9]+)", str(i[1]))
  more_info = re.sub('\,', '', str(more_info))
  more_info = re.findall("([0-9]+)", str(more_info))
  worldgross_value.append(more_info) 
  
Cumulative_Worldwide_Gross = pd.DataFrame(worldgross_value, columns=["Cumulative_Worldwide_Gross"])

newData = pd.concat([url_data, date, rating, Budget, Cumulative_Worldwide_Gross], axis=1)

#checking missing values 

missing_url = newData[(newData["Cumulative_Worldwide_Gross"].isnull())]

#updated the Xpath selector to get the missing values 

miss_worldgross = []
for i in missing_url["url"]:
  resp = httpmng.request('GET', i)
  tagtree = html.fromstring(resp.data)
  xp_worldgross = "//div[@id='titleDetails']/div[9]/text()"
  miss_imdb_worldgross = tagtree.xpath(xp_worldgross)
  miss_worldgross.append(miss_imdb_worldgross)

miss_worldgross_value = []
for i in miss_worldgross:
  more_info = re.findall("\$([0-9]+\,[0-9].*\,[0-9]+)", str(i[1]))
  more_info = re.sub('\,', '', str(more_info))
  more_info = re.findall("([0-9]+)", str(more_info))
  miss_worldgross_value.append(more_info)
  
miss_Cumulative_Worldwide_Gross = pd.DataFrame(miss_worldgross_value, columns=["Cumulative_Worldwide_Gross"], index=[7,8,33,34,35,39,43,46,47,55,59])
missing_url = missing_url.drop("Cumulative_Worldwide_Gross", axis = 1)
miss_newData = pd.concat([missing_url, miss_Cumulative_Worldwide_Gross], axis=1)
miss_newData = pd.concat([missing_url, miss_Cumulative_Worldwide_Gross], axis=1)

#repeating the above process 

final_miss_url = miss_newData[(miss_newData["Cumulative_Worldwide_Gross"].isnull())]
final_not_miss_url = miss_newData[(miss_newData["Cumulative_Worldwide_Gross"].notnull())]
semi_fin_data = final_not_miss_url.append(newData[(newData["Cumulative_Worldwide_Gross"].notnull())])

final_miss_worldgross = []
for i in final_miss_url["url"]:
  resp = httpmng.request('GET', i)
  tagtree = html.fromstring(resp.data)
  xp_worldgross = "//div[@id='titleDetails']/div[8]/text()"
  final_miss_imdb_worldgross = tagtree.xpath(xp_worldgross)
  final_miss_worldgross.append(final_miss_imdb_worldgross)
  
final_miss_worldgross_value = []
for i in final_miss_worldgross:
  more_info = re.findall("\$([0-9]+\,[0-9].*\,[0-9]+)", str(i[1]))
  more_info = re.sub('\,', '', str(more_info))
  more_info = re.findall("([0-9]+)", str(more_info))
  final_miss_worldgross_value.append(more_info)
  
#filtered the rows with only non-null values for Cumulative_Worldwide_Gross and appended it to the data points derived from the updated xpath  

final_miss_Cumulative_Worldwide_Gross = pd.DataFrame(final_miss_worldgross_value, columns=["Cumulative_Worldwide_Gross"], index=[33,34,35,39])
final_step = final_miss_url.drop("Cumulative_Worldwide_Gross", axis = 1)
final_miss_newData = pd.concat([final_step, final_miss_Cumulative_Worldwide_Gross], axis=1)
final_data = final_miss_newData.append(semi_fin_data)


##scraping the data for the length of the movie 
def time():
  time = []
  for i in url_data["url"]:
    res = requests.get(i)
    soup = bs4.BeautifulSoup(res.text,"lxml")
    imdb_time = soup.select("time")
    time.append(re.findall("PT([0-9]+)M", str(imdb_time[1])))
  return time  
time = time()
length_movie = pd.DataFrame(time, columns=["Movie_length"])
num_data = pd.concat([final_data, length_movie], axis=1)

#checking data types
num_data.dtypes

#changing data types
num_data["rating"] = num_data["rating"].astype(float)
num_data["Budget"] = num_data["Budget"].astype(float)
num_data["Cumulative_Worldwide_Gross"] = num_data["Cumulative_Worldwide_Gross"].astype(float)
num_data["Movie_length"] = num_data["Movie_length"].astype(float)
num_data["Release_year"] = num_data["Release_year"].astype(int)

#adding a derived column profit with budget and Cumulative_Worldwide_Gross
#profit = Cumulative_Worldwide_Gross - budget
num_data["Profit"] = num_data["Cumulative_Worldwide_Gross"] - num_data["Budget"]

#looping through the movie links to collect the links of the review page

user_url = []
for i in url_data["url"]:
  resp = httpmng.request('GET', i)
  tagtree = html.fromstring(resp.data)
  xp_rl = "//div[@id='titleUserReviewsTeaser']/div/a[2]/@href"
  imdb_rl = tagtree.xpath(xp_rl)
  imdb_rl = str(imdb_rl[0])
  imdb_rl = ['https://www.imdb.com',imdb_rl]
  imdb_rl = ''.join(imdb_rl)
  user_url.append(imdb_rl)
  
#collecting the top 10 user review links for all the movies

review_link = []
for i in user_url:
  links = []
  clean_link = []
  final_links = []
  final_10 = []
  res = requests.get(i)
  soup = bs4.BeautifulSoup(res.text,"lxml")
  for link in soup.find_all('a', href = True):
    links.append(link['href'])
  for i in links:
    if re.match("(/review/.*/)", str(i)) is not None:
      clean_link.append(re.findall("(/review/.*/)", str(i)))
    else:
      continue
  for i in clean_link:
    if i not in final_links:
      final_links.append(i)
  final_links = final_links[0:10]
  for i in final_links:
    step = str(i[0])
    step1 = ['https://www.imdb.com',step]
    step2 = ''.join(step1)
    final_10.append(step2)
  review_link.append(final_10)
  
  
#using the above obtained links to scrape the data for reviews 
all_reviews = []
for i in review_link:
  reviews = []
  for j in i:
    resp = httpmng.request('GET', j)
    tagtree = html.fromstring(resp.data)
    xp_rl1 = GenericTranslator().css_to_xpath("div.content>div")
    xp_rl1 = xp_rl1 + '/text()'
    imdb_rl1 = tagtree.xpath(xp_rl1)
    reviews.append(imdb_rl1)
  all_reviews.append(reviews)

#consolidating the data to concate it to the inital dataset
r1 = []
r2 = []
r3 = []
r4 = []
r5 = []
r6 = []
r7 = []
r8 = []
r9 = []
r10 = []
for i in all_reviews:
  r1.append(str(i[0]))
  r2.append(str(i[1]))
  r3.append(str(i[2]))
  r4.append(str(i[3]))
  r5.append(str(i[4]))
  r6.append(str(i[5]))
  r7.append(str(i[6]))
  r8.append(str(i[7]))
  r9.append(str(i[8]))
  r10.append(str(i[9]))
   
#cleaning the data using regex
rev1 = []
rev2 = []
rev3 = []
rev4 = []
rev5 = []
rev6 = []
rev7 = []
rev8 = []
rev9 = []
rev10 = []
for i in r1:
  more_info = re.findall("\[(.*)\]", str(i))
  rev1.append(more_info)
for i in r2:
  more_info = re.findall("\[(.*)\]", str(i))
  rev2.append(more_info)
for i in r3:
  more_info = re.findall("\[(.*)\]", str(i))
  rev3.append(more_info)
for i in r4:
  more_info = re.findall("\[(.*)\]", str(i))
  rev4.append(more_info)
for i in r5:
  more_info = re.findall("\[(.*)\]", str(i))
  rev5.append(more_info)
for i in r6:
  more_info = re.findall("\[(.*)\]", str(i))
  rev6.append(more_info)
for i in r7:
  more_info = re.findall("\[(.*)\]", str(i))
  rev7.append(more_info)
for i in r8:
  more_info = re.findall("\[(.*)\]", str(i))
  rev8.append(more_info)
for i in r9:
  more_info = re.findall("\[(.*)\]", str(i))
  rev9.append(more_info)
for i in r10:
  more_info = re.findall("\[(.*)\]", str(i))
  rev10.append(more_info)

#converting the reviews to pandas dataframe
r1 = pd.DataFrame(rev1, columns=["r1"])
r2 = pd.DataFrame(rev2, columns=["r2"])
r3 = pd.DataFrame(rev3, columns=["r3"])
r4 = pd.DataFrame(rev4, columns=["r4"])
r5 = pd.DataFrame(rev5, columns=["r5"])
r6 = pd.DataFrame(rev6, columns=["r6"])
r7 = pd.DataFrame(rev7, columns=["r7"])
r8 = pd.DataFrame(rev8, columns=["r8"])
r9 = pd.DataFrame(rev9, columns=["r9"])
r10 = pd.DataFrame(rev10, columns=["r10"])
  
#concatenating to get the final dataset
final_data = pd.concat([num_data, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10], axis=1)

#checking for null values
final_data.isna().sum()
