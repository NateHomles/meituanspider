# -*- coding: utf-8 -*-
"""
Created on Tue Mar 29 17:24:40 2022

@author: User
"""

import csv
import json
import os

import requests
import pymysql

import random
import time
import numpy as np
import pandas as pd

from pymongo import MongoClient
from py2neo import Graph, Node
from bs4 import BeautifulSoup
import lxml
headers = [
    {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.74 Safari/537.36'
    },
    {
        'User-Agent': 'AiMeiTuan /HUAWEI-4.4.2-HUAWEI MLA-AL10-720x1280-240-5.5.4-254-863254010002128-qqcpd'
    }
]

cookies={'cookie':'uuid=1c65411321a643de9a55.1648442160.1.0.0; _lxsdk_cuid=17fcecf333ac8-01318e2ecfc45b-9771539-104040-17fcecf333ac8; mtcdn=K; _lxsdk=17fcecf333ac8-01318e2ecfc45b-9771539-104040-17fcecf333ac8; _hc.v=a3311b8a-67a5-890a-3256-f66a473989e9.1648442231; ci=20; client-id=75587d46-0667-434b-bda3-fa6dbaffd4df; userTicket=riGSJEPGPXGlcjfPnhkjeTgaBYHrTssKitZSwiRg; lat=23.17559; lng=113.26841; lt=7wGKELJ8iHpHvfmgZ6Th2ofSoXQAAAAABBEAAD3KzN9G98-qkSQBOX-oJhmpfFKAhSwYqRNZETeJVcflmuQkVke14sh9aSRlOIVGvQ; u=774939370; n=%E7%BA%A6%E5%AE%9A%E5%A5%BD%EF%BC%8C%E5%86%8D%E8%A7%81; token2=7wGKELJ8iHpHvfmgZ6Th2ofSoXQAAAAABBEAAD3KzN9G98-qkSQBOX-oJhmpfFKAhSwYqRNZETeJVcflmuQkVke14sh9aSRlOIVGvQ; unc=%E7%BA%A6%E5%AE%9A%E5%A5%BD%EF%BC%8C%E5%86%8D%E8%A7%81; __mta=48615833.1648479948794.1648610470889.1648610474321.23; firstTime=1648610493657; _lxsdk_s=17fd8d5a0db-33f-98b-162%7C%7C33'}
#爬url和poiid
raw='http://gz.meituan.com/meishi/'
rows=[]
for index in range(2):
    if index == 0:
        continue;
    url = raw + 'pn' + str(index) +'/';
    page = requests.get(url = url,cookies = cookies,headers = random.choice(headers))
    time.sleep(3)
    print(index)
    print(page.status_code) 
    if(page.status_code!=200):
        print("error!")
        continue
    
    soup1 = BeautifulSoup(page.text,"lxml") 
    soup2=soup1.find_all('script') 
    text=soup2[16].get_text().strip()       
    text=text[19:-1]        
    result=json.loads(text) 
    print('decoded finished')
    result=result['poiLists']        
    result=result['poiInfos']
    for i in result:
        rows.append([i['title'],str(i['poiId']),str(i['avgScore']),i['address'],str(i['allCommentNum']),str(i['avgPrice'])]) 
    print('next round')
#进入另外一个链接爬评论
urlpool=[]
comment=[]
rawcom1='https://gz.meituan.com/meishi/api/poi/getMerchantComment?\
    uuid=1c65411321a643de9a55.1648442160.1.0.0&platform=1&partner=\
    126&originUrl=https%3A%2F%2Fgz.meituan.com%2Fmeishi%2F'
rawcom2='%2F&riskLevel=1&optimusCode=100&id='
rawcom3='&userId=774939370&offset=0&pageSize=1000&sortType=1'
cookies2={'cookie':'uuid=1c65411321a643de9a55.1648442160.1.0.0; _lxsdk_cuid=17fcecf333ac8-01318e2ecfc45b-9771539-104040-17fcecf333ac8; mtcdn=K; _lxsdk=17fcecf333ac8-01318e2ecfc45b-9771539-104040-17fcecf333ac8; _hc.v=a3311b8a-67a5-890a-3256-f66a473989e9.1648442231; ci=20; client-id=75587d46-0667-434b-bda3-fa6dbaffd4df; userTicket=riGSJEPGPXGlcjfPnhkjeTgaBYHrTssKitZSwiRg; lat=23.17559; lng=113.26841; lt=7wGKELJ8iHpHvfmgZ6Th2ofSoXQAAAAABBEAAD3KzN9G98-qkSQBOX-oJhmpfFKAhSwYqRNZETeJVcflmuQkVke14sh9aSRlOIVGvQ; u=774939370; n=%E7%BA%A6%E5%AE%9A%E5%A5%BD%EF%BC%8C%E5%86%8D%E8%A7%81; token2=7wGKELJ8iHpHvfmgZ6Th2ofSoXQAAAAABBEAAD3KzN9G98-qkSQBOX-oJhmpfFKAhSwYqRNZETeJVcflmuQkVke14sh9aSRlOIVGvQ; unc=%E7%BA%A6%E5%AE%9A%E5%A5%BD%EF%BC%8C%E5%86%8D%E8%A7%81; firstTime=1648612391382; _lxsdk_s=17fd8d5a0db-33f-98b-162%7C%7C38; __mta=48615833.1648479948794.1648610474321.1648612391612.24'}

origin=[]
count=0
for index in urlpool:
    count+=1
    country=0
    if (count in [1,2,4,5,6,7,8,10,11,12,13,14,15]):
        print('skip finished round',count)
        continue
    #url=rawcom1+str(index[1])+rawcom2+str(index[1])+rawcom3
    #urlpool.append(url)
    response=requests.get(url=index,cookies=cookies2,headers = random.choice(headers))
    try:
        temp=json.loads(response.text)
        origin.append(temp)
    except:
        print('skip round',count,'due to unloadable')
        continue    
    try:
        for i in range(len(temp['data']['comments'])):
            comment.append(temp['data']['comments'][i]['comment'])
        print('round')
        print(count)
    except:
        while ~temp['data']:
            country+=1
            response=requests.get(url=index,cookies=cookies2,headers = random.choice(headers))
            temp=json.loads(response.text)
            for i in range(len(temp['data']['comments'])):
                comment.append(temp['data']['comments'][i]['comment'])
            print('round',count,'   ','retry',country,'time')
           
print(urlpool[10])
response=requests.get(url=urlpool[0],cookies=cookies2,headers = random.choice(headers))
print(response.text)
test=requests.get(url='https://gz.meituan.com/meishi/pn01/',cookies=cookies,headers=random.choice(headers))
print(test.text)

df=pd.DataFrame({'comments':comment})
df.to_csv(r'./comment.csv',index=0,encoding='UTF-8-sig')
df2=pd.DataFrame({'url':urlpool})
df2.to_csv(r'./url.csv',index=0,encoding='UTF-8-sig')