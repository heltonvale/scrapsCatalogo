import requests
from bs4 import BeautifulSoup
import pandas as pd
from sys import stdout
import logging
from time import sleep
import os

def config_log():
  stdout_handler = logging.StreamHandler(stdout)
  handlers = [stdout_handler]
  logging.basicConfig(
  level=logging.INFO,
  format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
  handlers=handlers)
  logger = logging.getLogger('JOB')
  return logger

logger = config_log()

site = os.environ.get('site')
caminho = os.environ.get('caminho')

#### BUSCA URLs SUBCATEGORIA AUTOPEÇAS NIVEL0
 
page = requests.get(f"{site}/categoria/autopecas")
soup = BeautifulSoup(page.content, 'html.parser')
subcats = soup.find_all(class_ = 'y-catBox')
subcats_url = [f"{site}"+x.find('a').attrs['href'] for x in subcats]

#### BUSCA URLs SUBCATEGORIA AUTOPEÇAS NIVEL1

subcats_url_N1 = []

for u in  subcats_url:
    page = requests.get(u)
    soup = BeautifulSoup(page.content, 'html.parser')
    subcats = soup.find_all(class_ = 'y-catBox')
    subcats_url_N1.extend([f"{site}"+x.find('a').attrs['href'] for x in subcats])
    
#### BUSCA URLs SUBCATEGORIA AUTOPEÇAS NIVEL2

subcats_url_N2 = []

for u in  subcats_url_N1:
    page = requests.get(u)
    soup = BeautifulSoup(page.content, 'html.parser')
    subcats = soup.find_all(class_ = 'y-catBox')
    subcats_url_N2.extend([f"{site}"+x.find('a').attrs['href'] for x in subcats])

#subcats_url_N2_1 = [k for k in subcats_url_N2 if 'autopecas/freios' in k]    

#### BUSCA URLs SKUs
    
skulist = []

for u in subcats_url_N2:
    flag=True
    page = page = requests.get(u)
    while flag:
        try:
            logger.info(page.url)
            sleep(1)
            soup = BeautifulSoup(page.content, 'html.parser')
            sku = soup.find_all(class_ = 'item-container')
            skulist.extend([f"{site}"+x.find('a').attrs['href'] for x in sku])
            pagination = soup.find(class_='pagination-next')
            next_page='https://www.canaldapeca.com.br'+pagination.find('a').attrs['href']
            page = requests.get(next_page)
        except KeyError:
            flag=False
        except AttributeError:
            flag=False


#### BUSCA ATRIBUTOS PRODUTOS

skujson = []
count=0
for i in skulist:
    try:
        count = count + 1
        log = i +' link '+ str(count)
        sleep(1)
        logger.info(log)
        url=i
        page=requests.get(i)
        soup=soup = BeautifulSoup(page.content, 'html.parser')
        fabspec = soup.find(class_ = 'manufacturer-specs')
        specs = [{x.get_text(strip=True).split(':')[0]:x.get_text(strip=True).split(':')[1].strip() for x in fabspec.find_all('p')}]
        skudesc = soup.find(class_='sku-title').get_text(strip=True).strip()
        skuinfo = soup.find(class_='product-infos')
        info = [{x.get_text(strip=True).split(':')[0].strip():x.get_text(strip=True).split(':')[1].strip() for x in skuinfo.find_all('p')}]
        cod_montadora = [{x.get_text(strip=True).split(':')[0].strip():x.get_text(strip=True).split(':')[1].strip() for x in skuinfo.find_all('li')}]
        skuapplication = soup.find_all(class_='applications_list')
        application_list = [{x.find(class_='application_make make').get_text(" ", strip=True).strip() \
                             : list(map(lambda items: items.get_text(" ",strip=True).strip(), x.find_all('li')))  for x in skuapplication}]
        barracat = soup.find(class_='breadcrumb-list')
        J = {"url":url,
             "cod_montadora":cod_montadora,
             "especsfab":specs,
             "infofab":info,
             "descrição":skudesc,
             "aplicação":application_list,
             "categoria1":barracat.find_all(itemprop ='title')[1].get_text(strip=True),
             "categoria2":barracat.find_all(itemprop ='title')[2].get_text(strip=True),
             "categoria3":barracat.find_all(itemprop ='title')[3].get_text(strip=True),
             "categoria4":barracat.find_all(itemprop ='title')[4].get_text(strip=True)
             }
        skujson.append(J)
    except AttributeError:
            continue
    except IndexError:
            continue



df = pd.DataFrame(skujson)
df['especsfabJS']=df['especsfab'].apply(pd.Series)
df = df.drop('especsfabJS', 1).assign(**pd.DataFrame(df['especsfabJS'].tolist()))
del df['especsfab']

df['infofabJS']=df['infofab'].apply(pd.Series)
df.drop('infofabJS', 1).assign(**pd.DataFrame(df['infofabJS'].tolist()))
del df['infofab']

df.to_csv(caminho, header=True, index= False, sep='^')
            