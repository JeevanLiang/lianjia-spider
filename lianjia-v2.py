# -*- coding: utf-8 -*-
"""
Created on Thu Jul 19 19:57:26 2018

@author: msi-pc
"""

import requests
from bs4 import BeautifulSoup
import random
import time 
from lxml import etree
import urllib.request
import urllib
import os

main_url='https://gz.lianjia.com/ershoufang/huadou/pg{}/'

def pages_url(get_num=100):  # 生成url
    url = main_url
    for url_next in range(10, int(get_num)):
        yield url.format(url_next)
        

def Details_all_url(page_url):
    details_url=requests.get(page_url)
    if details_url.status_code !=200:
        print('allurlerror!!')
        return
    dom_tree=etree.HTML(details_url.content.decode('utf-8'))
    return dom_tree.xpath("/html/body/div/div/ul[@class='sellListContent']/li/a/@href")

def Details_url(url):
    headers={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'}
    source=requests.get(url,headers)
    if source.status_code !=200:
        print('urlerror')
        return
    html=requests.get(url,headers=headers).content.decode('utf-8')
    dom_tree=etree.HTML(html)
    html_image=dom_tree.xpath("/html/body/div/div/div/div[@class='content-wrapper housePic']/div/div/div/img/@src")
    tag_image=random.randint(0,20)
    if len(dom_tree.xpath("/html/body/div/div/div/div[@class='content-wrapper housePic']/div/div/div/span/text()"))>=1:
        tag_image=dom_tree.xpath("/html/body/div/div/div/div[@class='content-wrapper housePic']/div/div/div/span/text()")
    bf=BeautifulSoup(source.text,'lxml')
    num=bf.select('.houseRecord')[0].text[4:-2]#编号
    path="D:\\pyProject\\lianjia-picture-8.8\\huadou\\"+num
    mkdir(path)
    cou=0
    for u in html_image:
        if len(tag_image)>=cou:           
            download_pic(u,tag_image[cou],path)
        else:
            download_pic(u,tag_image,path)
        cou+=1

    
def mkdir(path):
 
	folder = os.path.exists(path)
 
	if not folder:                   #判断是否存在文件夹如果不存在则创建为文件夹
		os.makedirs(path)            #makedirs 创建文件时如果路径不存在会创建这个路径 
	else:
		print ("Warnning!!!dataError")
    
    
def download_pic(img_url,tag,path):
    filename=tag+'.jpg'
    if os.path.exists(filename):       # 判断图片是否存在
        os.remove(filename)
    dist = os.path.join(path, filename)
    urllib.request.urlretrieve(urllib.parse.quote(img_url,safe=":/"), dist,None)
    
    
if __name__ == '__main__':
    a=0
    for url in pages_url(100):
        for detailsurl in Details_all_url(url):
            time.sleep(1)
            Details_url(detailsurl)
            print(a)
            a+=1
    print("done!!!")