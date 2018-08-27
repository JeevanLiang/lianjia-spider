# -*- coding: utf-8 -*-
"""
Created on Wed Jul 11 23:52:59 2018

@author: msi-pc
"""
import pandas as pd
import requests
from bs4 import BeautifulSoup
from lxml import etree
import json


main_url='https://gz.lianjia.com/ershoufang/nansha/'
kfs='null'
wy='null'
wyf='null'
jznd='null'

def pages_url(get_num):  # 生成url
    url = main_url+'pg{}/'
    for url_next in range(1, int(get_num)):
        yield url.format(url_next)
        
def Details_all_url(page_url):
    details_url=requests.get(page_url)
    if details_url.status_code !=200:
        print('allurlerror!!')
        return
    dom_tree=etree.HTML(details_url.text)
    return dom_tree.xpath("/html/body/div/div/ul[@class='sellListContent']/li/a/@href")

def get_resblock(dom_tree):
    headers={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'}
    get_url=dom_tree.xpath("/html/body/div/div/div/div[@class='communityName']/a/@href")
    if len(get_url) < 1:
        return
    xiaoqu_url='https://gz.lianjia.com'+get_url[0]
    xiaoquSource=requests.get(xiaoqu_url,headers)
    if xiaoquSource.status_code!=200:
        print('xiaoquurlfail')
        return
    dom_treex=etree.HTML(xiaoquSource.text)
    global jznd
    global kfs
    global wy
    global wyf
    if len(dom_treex.xpath("/html/body/div/div/div/div/span/text()"))>5:
        if len(dom_treex.xpath("/html/body/div/div/div/div/span/text()")[5])>3:
            jznd=dom_treex.xpath("/html/body/div/div/div/div/span/text()")[5][:4]
    if len(dom_treex.xpath("/html/body/div/div/div/div/span/text()"))>13:
        kfs=dom_treex.xpath("/html/body/div/div/div/div/span/text()")[13]
    if len(dom_treex.xpath("/html/body/div/div/div/div/span/text()"))>11:
        wy=dom_treex.xpath("/html/body/div/div/div/div/span/text()")[11]
    if len(dom_tree.xpath("/html/body/div/div/div/div/span/text()"))>9:
        wyf=dom_treex.xpath("/html/body/div/div/div/div/span/text()")[9]


def Details_url(url):
    headers={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'}
    source=requests.get(url,headers)
    dom_tree=etree.HTML(source.text)
    if len(dom_tree.xpath('/ html / body / script[21]/text()'))>0 :
        if len(dom_tree.xpath('/ html / body / script[21]/text()')[0].split('\n'))>15:
            latitude = dom_tree.xpath('/ html / body / script[21]/text()')[0].split('\n')[16].split("'")[1].split(',')
    if source.status_code !=200:
        print('urlerror')
        return
    bf=BeautifulSoup(source.text,'lxml')
    data={}
    get_resblock(dom_tree)
    data['建筑年份']=jznd
    data['开发商']=kfs
    data['物业公司']=wy
    data['物业费']=wyf
    if bf.select('.base')[0].text.split('\n') :
        baselist=bf.select('.base')[0].text.split('\n')
    tranlist=bf.select('.transaction')[0].text.split('\n')
    data['编号']=bf.select('.houseRecord')[0].text[4:-2]#编号
    data['小区']=bf.select('.communityName')[0].text[4:-2]#小区
    data['标题']=bf.select('.main')[0].text#标题
    data['所在区域']=bf.select('.areaName')[0].text[4:].replace('\xa0',' ')#所在区域
    data['价格']=bf.select('.price')[0].text.split(' ')[0]#价格
    data['税费']=bf.select('.price')[0].text.split(' ')[1][2:]#税费
    if bf.select('.base')[0].text.split('\n') :
        data['房屋户型']=baselist[4][4:]#房屋户型
        data['楼层']=baselist[5][4:]#楼层
        data['建筑面积']=baselist[6][4:]#建筑面积
        data['户型结构']=baselist[7][4:]#户型结构
        data['套内面积']=baselist[8][4:]#套内面积
        data['建筑类型']=baselist[9][4:]#建筑类型
        if len(baselist)>10 :
            data['房屋朝向']=baselist[10][4:]#房屋朝向
            data['建筑结构']=baselist[11][4:]#建筑结构
            data['装修情况']=baselist[12][4:]#装修情况
            data['梯户比例']=baselist[13][4:]#梯户比例
            data['电梯']=baselist[14][4:]#电梯
            data['产权年限']=baselist[15][4:]#产权年限
    data['挂牌时间']=tranlist[6]#挂牌时间
    data['交易权属']=tranlist[10]#交易权属
    data['上次交易']=tranlist[14]#上次交易
    data['房屋用途']=tranlist[18]#房屋用途
    data['房屋年限']=tranlist[22]#房屋年限
    data['产权所属']=tranlist[26]#产权所属
    data['抵押信息']=tranlist[31][-3:]#抵押信息
    data['房本备件']=tranlist[36]#房本备件
    data['经度']=latitude[0]
    data['纬度']=latitude[1]
    #data['']=strlist[][:]
    #print(source.text)
    pandas_to_csv(data)
    turn_poi(data['纬度'],data['经度'])


def pandas_to_csv(data):  # 储存到csv
    pd_look = pd.DataFrame(data,index=[0])
    pd_look.to_csv('链家二手房v3-nansha.csv',index=True,mode='a',header=False,encoding="utf_8_sig")


def turn_poi(wd,jd):
    lists=['小学','中学','地铁站','公交站','商场','医院']
    ak=['yf5P2AMA6TBFOAP8gp0OuB2vUGDCuFeu',
        'waQMHTHiPdGF7OA4znG01vkpXtsTEXFN',
        'zRxnBQEyw5EP9R0hhOrQyVDoKkWDoqny',
        'ySlRfuBcFcPiaIXyB7BGSQCzV38dmCfj',
        'ODLuA4IbkuOkFFrX1HU8Q9TTRyWGLmhc',
        'pTOOtGM0Li693pwiIza7UhwdediESQGB',
        'mCq1C04xYsGif4yq3EGywZY6bAVR6eHX',
        'QI5dKrWxs6WStA4jC1UKnhPXy5aaydtv',
        'R8kWLppHgesE1rG1r5NT3ZyUHVmookNW',
        'nEAuUAiQGxIpS5dEEoG7dg1ydPdAh2VZ',
        'x1en4c5ybt7aUCarrPIDZvsY5X1dxtUE',
        'MBiVKCxqFBG7Pwu9GxpENxvZpubBPoCL',
        '4PKXL4rBGvgsVdUbEwXDfjfISQMpL8fe',
        '1rsPcBrjIVPmavCSMMzGh4igg8VHTBYr',
        'WO4SW6g9m6YEkt7gwOqCNloCbHvAggBL',
        'cVaWaLDpYsT8RFYShE9aottmoNSp8i6u',
        'Hh2GMtwxROpLPNVawUmfjMMz5r0kUGl2',
        'dRutZjNPnmmR4leAY4ljwGyA6rAGMlme',
        'tU7zQbrmUSFRGcbdRAuGmYxsLevorK9V',
        'rKmgcjkTgLIOowC9nSXMfgcV4gCwH0T9',
        'N70rweZ87gBne6CzwoThVAoyAi8Zd1Pl',
        'XmngvsTcZzhL0fXBSy6ngCuLMOCziwWQ',
        'aECTdVC1k6fUnUEm3wkmqsrTApUn6jiu',
        '8WWcsUXAToObkMPNFwfloYi7Lq3G9MfL',
        'xl7ys0UKSdXMZUjeG1Ekmfz9ScSFeU4R',
        'DLV8xKfWvPdFpQytRC1jZxKiGnawFXbR',
        'wZWUi0mfecdu34hlzO6kWgbtuAgoLy3E',
        '1bFAysdHZYkiuPE1hmjnSU2G8F8CDWzs',
        'DYOpKxpQ8hzvTEGBeodmIiLG7eM1OE7F',
        'DM0WrLQEzPuu1fLoo7e2A1Qnz3pFzpzq',
        'B5KrVvcnrGplq1CHhgdGTkdKHeXuBIGD'
        ]
    for element in lists:
        for akx in ak:
            response=requests.get('http://api.map.baidu.com/place/v2/search?query='+element+'&location='+wd+','+jd+'&radius=1000&output=json&ak='+akx)
            jsonobj=json.loads(response.text)
            if 'results' in jsonobj:
                break
        i=1
        data={}
        data['0']=element
        while i<len(jsonobj['results']):
            data[str(i)]=jsonobj['results'][i]['name']+'  '+str(jsonobj['results'][i]['location']['lat'])+'  '+str(jsonobj['results'][i]['location']['lng'])
            i+=1
        pandas_to_csv(data)
        
    


if __name__ == '__main__':
    num=0
    for url in pages_url(100):
        for detailsurl in Details_all_url(url):
            Details_url(detailsurl)
            #Details_url(detailsurl)
            num+=1
            print(num)
    print('done')