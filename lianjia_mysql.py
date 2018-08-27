# -*- coding: utf-8 -*-
"""
Created on Mon Aug 13 14:53:21 2018

@author: msi-pc
"""

import requests
from bs4 import BeautifulSoup
from lxml import etree
import json
import pymysql
import logging

main_url='https://gz.lianjia.com/ershoufang/tianhe/'
area='天河'
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
    dom_tree=etree.HTML(details_url.content.decode('utf-8'))
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
    dom_treex=etree.HTML(xiaoquSource.content.decode('utf-8'))
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
    dom_tree=etree.HTML(source.content.decode('utf-8'))
    if len(dom_tree.xpath('/ html / body / script[21]/text()'))>0 :
        if len(dom_tree.xpath('/ html / body / script[21]/text()')[0].split('\n'))>15:
            latitude = dom_tree.xpath('/ html / body / script[21]/text()')[0].split('\n')[16].split("'")[1].split(',')
    if source.status_code !=200:
        print('urlerror')
        return
    bf=BeautifulSoup(source.text,'lxml')
    vill={}
    base={}
    get_resblock(dom_tree)
    vill['建筑年份']=jznd#building_age
    vill['开发商']=kfs#developer
    vill['物业公司']=wy#property_company
    vill['物业费']=wyf#property_charges
    vill['小区名称']=bf.select('.communityName')[0].text[4:-2]#village_name
    if bf.select('.base')[0].text.split('\n') :
        baselist=bf.select('.base')[0].text.split('\n')
    tranlist=bf.select('.transaction')[0].text.split('\n')
    base['编号']=bf.select('.houseRecord')[0].text[4:-2]#number
    base['小区']=bf.select('.communityName')[0].text[4:-2]#village
    base['标题']=bf.select('.main')[0].text#title
    base['所在区域']=bf.select('.areaName')[0].text[4:].replace('\xa0',' ')#area
    base['价格']=bf.select('.price')[0].text.split(' ')[0]#price
    base['税费']=bf.select('.price')[0].text.split(' ')[1][2:]#tax
    if bf.select('.base')[0].text.split('\n') :
        base['房屋户型']=baselist[4][4:]#house_type
        base['楼层']=baselist[5][4:]#floor
        base['建筑面积']=baselist[6][4:]#built_up_area
        base['户型结构']=baselist[7][4:]#house_structure
        base['套内面积']=baselist[8][4:]#inner_area
        base['建筑类型']=baselist[9][4:]#building_type
        if len(baselist)>10 :
            base['房屋朝向']=baselist[10][4:]#orientations
            base['建筑结构']=baselist[11][4:]#buliding_structure
            base['装修情况']=baselist[12][4:]#decoration_situation
            base['梯户比例']=baselist[13][4:]#lift_proportion
            base['电梯']=baselist[14][4:]#lift
            base['产权年限']=baselist[15][4:]#years_of_property_rights
    base['挂牌时间']=tranlist[6]#listing_time
    base['交易权属']=tranlist[10]#trading_rights
    base['上次交易']=tranlist[14]#last_transaction
    base['房屋用途']=tranlist[18]#housing_use
    base['房屋年限']=tranlist[22]#housing_years
    base['产权所属']=tranlist[26]#Property_rights_belong
    base['抵押信息']=tranlist[31][-3:]#Mortgage_information
    base['房本备件']=tranlist[36]#Spare_parts
    vill['经度']=latitude[0]#longitude
    vill['纬度']=latitude[1]
    saveVillageInMysql(vill)
    saveBaseInMysql(base)
    

'''
def pandas_to_csv(data):  # 储存到xlsx
    pd_look = pd.DataFrame(data,index=[0])
    pd_look.to_csv('链家二手房v3-nansha.csv',index=True,mode='a',header=False,encoding="utf_8_sig")
'''

def saveBaseInMysql(base):
    db = pymysql.connect(host="localhost", port=3306, user='root', passwd='980117',db='lianjia')
    cursor = db.cursor()
    sql = "INSERT INTO BASEINFO(房屋编号,区域,小区名称,标题,所在区域,价格,房屋户型,楼层,建筑面积\
            ,户型结构,套内面积,建筑类型,房屋朝向,建筑结构,装修情况,梯户比例\
            ,电梯,产权年限,挂牌时间,交易权属,上次交易,房屋用途,房屋年限,产权所属,抵押信息,房本备件) \
           VALUES ('%d','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'\
           ,'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s' )" % \
           (int(base["编号"]),area,base["小区"],base["标题"],base["所在区域"],base["价格"],base["房屋户型"],base["楼层"]
           ,base["建筑面积"],base["户型结构"],base["套内面积"],base["建筑类型"],base["房屋朝向"],base["建筑结构"]
           ,base["装修情况"],base["梯户比例"],base["电梯"],base["产权年限"],base["挂牌时间"]
           ,base["交易权属"],base["上次交易"],base["房屋用途"],base["房屋年限"],base["产权所属"],base["抵押信息"],base["房本备件"])
    try:
            # 执行sql语句
        cursor.execute(sql)
            # 执行sql语句
        db.commit()
    except Exception:
   # 发生错误时回滚
        LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
        logging.basicConfig(filename='my.log', level=logging.DEBUG, format=LOG_FORMAT)
        logging.error("This is a error log in line 148.")
        logging.error(Exception)
        db.rollback()
    db.close()


def saveVillageInMysql(vill):
    db = pymysql.connect(host="localhost", port=3306, user='root', passwd='980117',db='lianjia')
    cursor = db.cursor()
    sql = "INSERT INTO VILLAGE(小区名称,经度,纬度,建筑年代,开发商,物业公司,物业费) \
           VALUES ('%s','%s','%s','%s','%s','%s','%s' )" % \
           (vill['小区名称'],vill['经度'],vill['纬度'],vill['建筑年份'],vill['开发商'],vill['物业公司'],vill['物业费'])
    try:
            # 执行sql语句
        cursor.execute(sql)
            # 执行sql语句
        db.commit()
        turn_poi(vill['纬度'],vill['经度'],vill['小区名称'])
    except Exception:
   # 发生错误时回滚
        LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
        logging.basicConfig(filename='lianjialog.log', level=logging.DEBUG, format=LOG_FORMAT)
        logging.error("This is a error log in line 167.")
        logging.error(Exception)
        db.rollback()
    db.close()



def saveAroundInMysql(arou):
    db = pymysql.connect(host="localhost", port=3306, user='root', passwd='980117',db='lianjia')
    cursor = db.cursor()
    sql = "INSERT INTO AROUND(小区名称,设施类型,设施名称,设施经度,设施纬度) \
           VALUES ('%s','%s','%s','%s','%s','%s' )" % \
           (arou['小区名称'],arou['类型'],arou['名称'],arou['经度'],arou['纬度'])
    try:
            # 执行sql语句
        cursor.execute(sql)
            # 执行sql语句
        db.commit()
    except Exception:
   # 发生错误时回滚
        LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
        logging.basicConfig(filename='my.log', level=logging.DEBUG, format=LOG_FORMAT)
        logging.error("This is a error log in line 189.")
        logging.error(Exception)
        db.rollback()
    db.close()




def turn_poi(wd,jd,vill):
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
        i=0
        while i<len(jsonobj['results']):
            arou={}
            arou['类型']=element
            arou['名称']=jsonobj['results'][i]['name']
            arou['经度']=str(jsonobj['results'][i]['location']['lat'])
            arou['纬度']=str(jsonobj['results'][i]['location']['lng'])
            arou['小区名称']=vill
            saveAroundInMysql(arou)
            i+=1
        
    


if __name__ == '__main__':
    num=0
    for url in pages_url(100):
        for detailsurl in Details_all_url(url):
            Details_url(detailsurl)
            #Details_url(detailsurl)
            num+=1
            print(num)
    print('done')