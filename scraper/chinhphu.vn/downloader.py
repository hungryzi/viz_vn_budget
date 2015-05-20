from bs4 import BeautifulSoup
import requests
import re
import shutil
from threading import Thread
import os

def cookies(): return {'BPC': '146e3f89963af155bb0e43f80cd4878d'}

def request(url):
  return requests.get(url, cookies=cookies())

def href(a):
  return a['href']

def parse_year_page(url):
  year_page = request(url)
  soup = BeautifulSoup(year_page.text, "lxml")

  main_table = soup.find(id="rg3047")
  doc_links = map(href, main_table.find_all("a", class_="tinmoi"))

  in_parallel(parse_doc_page, doc_links)

def parse_doc_page(url):
  doc_page = request(url)
  soup = BeautifulSoup(doc_page.text, "lxml")

  main_table = soup.find(id="rg3047")
  download_a = main_table.find(href=re.compile("datafile.chinhphu.vn"))

  if download_a:
    download(download_a['href'])
  else:
    print "WARNING: no download link found for " + url

def download(url):
  print "Downloading " + url

  url_parts = url.split('/')
  year = url_parts[-3]
  file_name = url_parts[-1]

  destination = year + '/' + file_name

  if not os.path.exists(os.path.dirname(destination)):
    os.makedirs(os.path.dirname(destination))

  response = requests.get(url, cookies=cookies(), stream=True)
  with open(destination, 'wb') as out_file:
    shutil.copyfileobj(response.raw, out_file)
    del response

def in_parallel(fn, l):
   for i in l:
      Thread(target=fn, args=(i,)).start()

if __name__=="__main__":
  url = "http://www.chinhphu.vn/portal/page/portal/chinhphu/solieungansachnhanuoc"

  budget_page = request(url)
  soup = BeautifulSoup(budget_page.text, "lxml")

  side_table = soup.find(id="rg3048")
  year_links = map(href, side_table.find_all("a", class_="tinmoi"))

  in_parallel(parse_year_page, year_links)

