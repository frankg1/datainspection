#-*- coding: utf-8 -*-
import xml.dom.minidom
import cx_Oracle
import os
import datetime
import numpy as np
from  sklearn.cluster import DBSCAN
from haversine import haversine
from getOutPack import convex_hull
from getMeanPoint import getMeanPoint
from getWeightPoint import getWeightPoint
from osgeo import gdal
from osgeo import ogr
from osgeo import osr
from getangle import calc_angle
from multiprocessing import cpu_count
from concurrent import futures
import time
'''
部署步骤
1 改orcconfig
2 放开小区线程
3 改target table
4 屏蔽所有画图
4 屏蔽all print
5 改成多进程
6 放开部分屏蔽进程
'''

orcConfig ='crnop/pc#yvc@10.164.25.65:54065/lrnop'
#orcConfig ='crnop/pc#yvc@134.192.245.131:61521/lrnop'
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

class Inspection():
    alloidsofroom=[]
    allsiteofhz=[]
    dbscan = DBSCAN(eps=0.0005, min_samples=5)
    abspath = os.path.abspath('.')
    workspace=abspath
    shpname = 'shpname'
    cpucount = int(cpu_count())
    processcount = int(20)
    locallist=[]
    localoidlist=[]
    targettable='INSPECTION_TARGET'
    PARTITION_IN_CLASS=''
    cfg_xml='cfg.xml'
    district_id_list=[]
    conn=None
    cursor=None
    def __init__(self,PARTI):
        #是否读取本地列表
        self.PARTITION_IN_CLASS=PARTI
        self.getinsertedlist(self.PARTITION_IN_CLASS)
        # print(self.locallist)
        self.loadxml()
        self.conn=cx_Oracle.connect(orcConfig)
        self.cursor=self.conn.cursor()
        #print(self.locallist)
        #print(self.district_id_list)
        pass
    def getinsertedlist(self,PARTITION=PARTITION_IN_CLASS):
        PARTITION=self.PARTITION_IN_CLASS
        sql=" SELECT DISTINCT ADDRESS,OID FROM "+self.targettable+" where start_time=to_date('"+PARTITION+"','yyyymmdd')"
        conn = cx_Oracle.connect(orcConfig)
        cursor = conn.cursor()
        cursor.execute(sql)
        querylist = cursor.fetchmany(100)
        while querylist:
            for row in querylist:
                self.locallist.append(row[0])
                self.localoidlist.append(row[1])
                pass
            querylist = cursor.fetchmany(100)
        del cursor
        conn.close()


    def getALLOidofRoom(self,length=1000000):
        sql="SELECT DISTINCT OID FROM NECUR_CELL_L_MR_tp WHERE COVER_TYPE<>'1' AND ROWNUM<"+str(length)
        #print('查询所有小区oid的sql: ',sql)
        conn = cx_Oracle.connect(orcConfig)
        cursor = conn.cursor()
        cursor.execute(sql)
        querylist = cursor.fetchmany(100)
        while querylist:
            for row in querylist:
                self.alloidsofroom.append(row[0])
                pass
            querylist = cursor.fetchmany(100)
        del cursor
        conn.close()

    def getALLAddressofHZ(self,length=1000000):
        sql="SELECT DISTINCT ADDRESS FROM NECUR_CELL_L_MR_tp WHERE COVER_TYPE='1' AND ROWNUM<"+str(length)
        #print('查询所有宏站的站址: ',sql)
        conn = cx_Oracle.connect(orcConfig)
        cursor = conn.cursor()
        cursor.execute(sql)
        querylist = cursor.fetchmany(100)
        while querylist:
            for row in querylist:
                self.allsiteofhz.append(row[0])
                pass
            querylist = cursor.fetchmany(100)
        del cursor
        conn.close()

    def reclassify(self,db,X):
        '''重新聚类，返回筛选后的节点，筛选后的核心店，筛选后的标签和聚类数'''
        labels=db.labels_
        n_clusters=len(set(labels))-(1 if -1 in labels else 0)
        dic={}
        for i in range(n_clusters):
            dic[i]=len(list(X[labels==i].flatten()))/2
        sum=0
        for k,v in dic.items():
            sum=sum+v
        mean=(sum+0.0)/len(dic)
        liscu=[]
        for k,v in dic.items():
            if v>=mean:
                liscu.append(k)
        #这里只保留1 个
        if len(liscu)>1:
            liscu=liscu[0:1]
        bigcu=[]
        for i in liscu:
            bigcu+=list(X[labels==i].flatten())
        zlis=[]
        for i in range(0,len(bigcu),2):
            tmp=[]
            lon1=bigcu[i]
            lat1=bigcu[i+1]
            tmp.append(lon1)
            tmp.append(lat1)
            zlis.append(tmp)
        dbscan1=DBSCAN(eps=0.0005, min_samples=5)
        zlis=np.array(zlis)
        db=dbscan1.fit(zlis)
        core=db.core_sample_indices_
        Xcore=zlis[core]
        labels=db.labels_
        n_clusters=len(set(labels))-(1 if -1 in labels else 0)
        return zlis,Xcore,labels,n_clusters

    def getVirtualSite(self,OIDlist,PARTITION=PARTITION_IN_CLASS):
        ta=time.time()
        PARTITION=self.PARTITION_IN_CLASS
        PARTITION=str(PARTITION)
        conn = cx_Oracle.connect(orcConfig)
        cursor = conn.cursor()
        i=0
        biglist=[]
        for OID in OIDlist:
            if OID in self.localoidlist:
                print('小区已经提取')
                continue
            i+=1
            sql="SELECT ADDRESS,NECUR.LONGITUDE as site_long,NECUR.LATITUDE AS site_lat,NECUR.OID as OIDOFCELL,CELLID,COVER_TYPE,GRID_ID,substr(GRID.GRID_ID, 0, instr(GRID.GRID_ID, '|', 1, 1) - 1) / 100000 long_lt,substr(GRID.GRID_ID,instr(GRID.GRID_ID, '|', 1, 1) + 1,length(GRID.GRID_ID)) / 100000 lat_lt,GRID.RSRP_AVG_W AS RSRP_AVG,NECUR.MULT_FRE_ANT_TYPE AS ANT_TYPE,START_TIME,GRID.VENDOR AS VENDOR,NECUR.CITY_NAME AS CITY_NAME,NECUR.LC_NAME,NECUR.MULT_FRE_ANT_TYPE AS ANT_TYPE,NECUR.ENODEBID,NECUR.CELLID,NECUR.PCI,NECUR.ANT_AZIMUTH,NECUR.CITY_ID,GRID.PCI AS MRPCI FROM NECUR_CELL_L_MR_tp NECUR LEFT JOIN LTE_MRO_GRID_CELL_w PARTITION(P"+PARTITION+") GRID ON NECUR.OID = GRID.OID WHERE GRID.RSRP_AVG_W > -100 AND NECUR.OID ='"+OID+"'"
            #print(OID,'查询本小区栅格：',sql)
            cursor.execute(sql)
            querylist = cursor.fetchmany(100)
            rtlist=[]
            lon=0.0
            lat=0.0
            insertlis=[]

            start_time=''
            vendor=''
            city_name=''
            lc_name=''
            ant_type=''
            enbid=''
            cellid=''
            pci=''
            oid=''
            address=''
            address_lon_tp=''
            adderss_lat_tp=''
            mr_get_lon=""
            mr_get_lat=""
            distance_get=""
            ant_azimuth=''
            ant_azimuth_get=''
            ant_azimuth_minus=''
            recommand_azimuth=''
            city_id=''
            mrpci=''

            while querylist:
                for row in querylist:
                    #得到小区的经纬度，以及有效栅格数据
                    tmplist=[]
                    tmplist.append(float(row[7]))   #经度加进去
                    tmplist.append(float(row[8]))   #纬度加进去
                    lon=row[1]
                    lat=row[2]
                    rtlist.append(tmplist)
                    start_time=row[11]
                    vendor=row[12]
                    city_name=row[13]
                    city_id=row[20]
                    lc_name=row[14]
                    ant_type=row[15]
                    enbid=row[16]
                    cellid=row[17]
                    pci=row[18]
                    oid=OID
                    address=row[0]
                    ant_azimuth=row[19]
                    mrpci=row[21]
                querylist = cursor.fetchmany(100)
            #小区的所有信息读取完毕
            #-----------------------------------------------------

            if len(rtlist)==0:
                #print(i,' ',OID,'此小区没有关联栅格，可能由于本日期中无此小区缘故！')
                continue
            #while 结束了，得到了所有的有效栅格的坐标 以及小区的工参坐标
            X=np.array(rtlist)
            lon=float(lon)
            lat=float(lat)
            #开始聚簇这个小区的有效栅格的坐标
            db=self.dbscan.fit(X)
            core=db.core_sample_indices_
            Xcore=X[core]
            zlis=None
            labels=None
            n_clusters=None
            try:
                zlis,Xcore,labels,n_clusters=self.reclassify(db,X)
            except Exception as e:
                print('错误所在的行号：', e.__traceback__.tb_lineno)
                print('错误信息', e)
                pass

            if len(Xcore)==0:
                print(' ',address,':小区没有核心点，有效栅格个数太少，仅有',len(X),'个')
                #self.writeintoshp_room(address,X,None,None,None,[lon,lat])
                distance_get='栅格少'
                address_lon_tp=lon
                adderss_lat_tp=lat
                ant_azimuth_get='360'
                recommand_azimuth='360'
                insertlis.append(start_time)
                insertlis.append(vendor)
                insertlis.append(city_name)
                insertlis.append(lc_name)
                insertlis.append(ant_type)
                insertlis.append(enbid)
                insertlis.append(cellid)
                insertlis.append(pci)
                insertlis.append(oid)
                insertlis.append(address)
                insertlis.append(address_lon_tp)
                insertlis.append(adderss_lat_tp)
                insertlis.append(mr_get_lon)
                insertlis.append(mr_get_lat)
                insertlis.append(distance_get)
                insertlis.append(ant_azimuth)
                insertlis.append(ant_azimuth_get)
                insertlis.append(recommand_azimuth)
                insertlis.append(ant_azimuth_minus)
                insertlis.append(city_id)
                insertlis.append(mrpci)
                biglist.append(insertlis)
                continue
            try:
                result = convex_hull(Xcore)
                result=np.array(result)
                weightpoint=getWeightPoint(result)
                print(i,' ',OID,'小区计算结果，偏差为： ',haversine(weightpoint[0],weightpoint[1],lon,lat))
                address_lon_tp=lon
                adderss_lat_tp=lat
                mr_get_lon=str(round(float(weightpoint[0]),5))
                mr_get_lat=str(round(float(weightpoint[1]),5))
                distance_get=haversine(weightpoint[0],weightpoint[1],lon,lat)
                try:
                    ant_azimuth_get=360.0
                    ant_azimuth_minus=0.0
                    recommand_azimuth=360.0
                except Exception as e:
                    print('错误所在的行号：', e.__traceback__.tb_lineno)
                    print('错误信息', e)
                    pass
                insertlis.append(start_time)
                insertlis.append(vendor)
                insertlis.append(city_name)
                insertlis.append(lc_name)
                insertlis.append(ant_type)
                insertlis.append(enbid)
                insertlis.append(cellid)
                insertlis.append(pci)
                insertlis.append(oid)
                insertlis.append(address)
                insertlis.append(address_lon_tp)
                insertlis.append(adderss_lat_tp)
                insertlis.append(mr_get_lon)
                insertlis.append(mr_get_lat)
                insertlis.append(distance_get)
                insertlis.append(ant_azimuth)
                insertlis.append(ant_azimuth_get)
                insertlis.append(recommand_azimuth)
                insertlis.append(ant_azimuth_minus)
                insertlis.append(city_id)
                insertlis.append(mrpci)
                biglist.append(insertlis)
            except Exception as e:
                print('错误所在的行号：', e.__traceback__.tb_lineno)
                print('错误信息', e)
                print(' ',address,'核心点数量：',len(Xcore),'计算凸包失败了,mean值替换重心点')
                weightpoint=getMeanPoint(Xcore)
                print(' ',address,'小区计算结果，偏差为： ',haversine(weightpoint[0],weightpoint[1],lon,lat))
                #self.writeintoshp_room(address,X,Xcore,None,weightpoint,[lon,lat])
                address_lon_tp=lon
                adderss_lat_tp=lon
                mr_get_lon=str(round(float(weightpoint[0]),5))
                mr_get_lat=str(round(float(weightpoint[1]),5))
                distance_get=str(round(float(haversine(weightpoint[0],weightpoint[1],lon,lat)),5))

                insertlis.append(start_time)
                insertlis.append(vendor)
                insertlis.append(city_name)
                insertlis.append(lc_name)
                insertlis.append(ant_type)
                insertlis.append(enbid)
                insertlis.append(cellid)
                insertlis.append(pci)
                insertlis.append(oid)
                insertlis.append(address)
                insertlis.append(address_lon_tp)
                insertlis.append(adderss_lat_tp)
                insertlis.append(mr_get_lon)
                insertlis.append(mr_get_lat)
                insertlis.append(distance_get)
                insertlis.append(ant_azimuth)
                insertlis.append(ant_azimuth_get)
                insertlis.append(recommand_azimuth)
                insertlis.append(ant_azimuth_minus)
                insertlis.append(city_id)
                insertlis.append(mrpci)
                biglist.append(insertlis)
        tb=time.time()
        print('mini thread over  num:',len(OIDlist),'time costs:',tb-ta)
        del cursor
        conn.close()
        self.insert(biglist)

    def getVirtualSiteOfHZ(self,AddressList,PARTITION=PARTITION_IN_CLASS):
        ta=time.time()
        PARTITION=self.PARTITION_IN_CLASS
        PARTITION=str(PARTITION)
        conn = cx_Oracle.connect(orcConfig)
        cursor = conn.cursor()
        i=0
        alloids=[]
        biglist=[]
        for address in AddressList:
            if address in self.locallist:
                print('站址已经提取')
                continue

            cells,alloids=self.getValidSiteByAddress(address)
            if cells==None:
                print('没有合适的小区这个站址：',address)
                continue
            instr="("
            for s in cells:
                instr+="'"+s+"' ,"
            instr=instr[0:len(instr)-1]
            instr+=")"
            #sql="SELECT ADDRESS,NECUR.LONGITUDE as site_long,NECUR.LATITUDE AS site_lat,NECUR.OID as OIDOFCELL,CELLID,COVER_TYPE,GRID_ID,substr(GRID.GRID_ID, 0, instr(GRID.GRID_ID, '|', 1, 1) - 1) / 100000 long_lt,substr(GRID.GRID_ID,instr(GRID.GRID_ID, '|', 1, 1) + 1,length(GRID.GRID_ID)) / 100000 lat_lt,GRID.RSRP_AVG_W AS RSRP_AVG,NECUR.MULT_FRE_ANT_TYPE AS ANT_TYPE,START_TIME,GRID.VENDOR AS VENDOR,NECUR.CITY_NAME AS CITY_NAME,NECUR.LC_NAME,NECUR.MULT_FRE_ANT_TYPE AS ANT_TYPE,NECUR.ENODEBID,NECUR.CELLID,NECUR.PCI,NECUR.ANT_AZIMUTH,NECUR.CITY_ID FROM NECUR_CELL_L_MR_tp NECUR LEFT JOIN LTE_MRO_GRID_CELL_w PARTITION(P"+PARTITION+") GRID ON NECUR.OID = GRID.OID WHERE GRID.RSRP_AVG_W > -100 AND NECUR.OID IN "+instr
            sql="SELECT  ADDRESS,site_long,site_lat,GRID_ID,substr(GRID_ID, 0, instr(GRID_ID, '|', 1, 1) - 1) / 100000 long_lt,substr(GRID_ID,instr(GRID_ID, '|', 1, 1) + 1,length(GRID_ID)) / 100000 lat_lt,START_TIME,CITY_NAME,CITY_ID FROM (SELECT ADDRESS,NECUR.LONGITUDE as site_long,NECUR.LATITUDE AS site_lat,NECUR.OID as OIDOFCELL,CELLID,COVER_TYPE,GRID_ID,GRID.RSRP_AVG_W AS RSRP_AVG,NECUR.MULT_FRE_ANT_TYPE AS ANT_TYPE,START_TIME,GRID.VENDOR AS VENDOR,NECUR.CITY_NAME AS CITY_NAME,NECUR.LC_NAME,NECUR.MULT_FRE_ANT_TYPE AS ANT_TYPE,NECUR.ENODEBID,NECUR.CELLID,NECUR.PCI,NECUR.ANT_AZIMUTH,NECUR.CITY_ID,GRID.PCI AS MRPCI FROM NECUR_CELL_L_MR_tp NECUR LEFT JOIN LTE_MRO_GRID_CELL_w PARTITION(P"+PARTITION+") GRID ON NECUR.OID = GRID.OID WHERE GRID.RSRP_AVG_W > -100 AND NECUR.OID IN "+instr +") GROUP BY GRID_ID,START_TIME,CITY_NAME,CITY_ID,ADDRESS,site_long,site_lat HAVING COUNT(GRID_ID)>1"
            #print(address,'查询本站址所有的栅格：',sql)
            cursor.execute(sql)
            querylist = cursor.fetchmany(100)
            rtlist=[]
            lon=0.0
            lat=0.0
            start_time=''
            vendor=''
            city_name=''
            lc_name=''
            ant_type=''
            enbid=''
            cellid=''
            pci=''
            oid=''
            address=''
            address_lon_tp=''
            adderss_lat_tp=''
            mr_get_lon=""
            mr_get_lat=""
            distance_get=""
            ant_azimuth=''
            ant_azimuth_get='0.0'
            ant_azimuth_minus='0.0'
            recommand_azimuth='0.0'
            city_id=''
            mrpci=''
            while querylist:
                for row in querylist:
                    tmplist=[]
                    tmplist.append(float(row[4]))
                    tmplist.append(float(row[5]))
                    lon=row[1]
                    lat=row[2]
                    rtlist.append(tmplist)
                    start_time=row[6]
                    #vendor=row[12]
                    city_name=row[7]
                    city_id=row[8]
                    address=row[0]
                querylist = cursor.fetchmany(100)
            #-----------------------------------------------
            #结束读取各种信息

            X=np.array(rtlist)
            lon=float(lon)
            lat=float(lat)
            #print(X)
            if len(X)==0:
                continue
            dbscan1=DBSCAN(eps=0.0005, min_samples=5)
            db=dbscan1.fit(X)
            core=db.core_sample_indices_
            Xcore=X[core]
            zlis=None
            labels=None
            n_clusters=None
            try:
                zlis,Xcore,labels,n_clusters=self.reclassify(db,X)
            except Exception as e:
                print('错误所在的行号：', e.__traceback__.tb_lineno)
                print('错误信息', e)
                pass
            if len(Xcore)==0:
                print(' ',address,':小区没有核心点，有效栅格个数太少，仅有',len(X),'个')
                address_lon_tp=lon
                adderss_lat_tp=lat
                mr_get_lon=''
                mr_get_lat=''
                distance_get='栅格少'
            else:
                try:
                    result = convex_hull(Xcore)
                    result=np.array(result)
                    weightpoint=getWeightPoint(result)
                    print(i,' ',address,'小区计算结果，偏差为： ',haversine(weightpoint[0],weightpoint[1],lon,lat))
                    address_lon_tp=lon
                    adderss_lat_tp=lat
                    mr_get_lon=str(round(float(weightpoint[0]),5))
                    mr_get_lat=str(round(float(weightpoint[1]),5))
                    distance_get=str(round(float(haversine(weightpoint[0],weightpoint[1],lon,lat)),5))
                except Exception as e:
                    print('错误所在的行号：', e.__traceback__.tb_lineno)
                    print('错误信息', e)
                    weightpoint=getMeanPoint(Xcore)
                    print(' ',address,'核心点数量：',len(Xcore),'计算凸包失败了,mean值替换重心点')
                    print(' ',address,'小区计算结果，偏差为： ',haversine(weightpoint[0],weightpoint[1],lon,lat))
                    address_lon_tp=lon
                    adderss_lat_tp=lat
                    mr_get_lon=weightpoint[0]
                    mr_get_lon=str(round(float(weightpoint[0]),5))
                    mr_get_lat=str(round(float(weightpoint[1]),5))
                    distance_get=str(round(float(haversine(weightpoint[0],weightpoint[1],lon,lat)),5))
            instr="("
            for s in alloids:
                instr+="'"+s+"' ,"
            instr=instr[0:len(instr)-1]
            instr+=")"
            sql1="SELECT DISTINCT ADDRESS,NECUR.LONGITUDE as site_long,NECUR.LATITUDE AS site_lat,NECUR.OID as OIDOFCELL,CELLID,COVER_TYPE,NECUR.MULT_FRE_ANT_TYPE AS ANT_TYPE,START_TIME,GRID.VENDOR AS VENDOR,NECUR.CITY_NAME AS CITY_NAME,NECUR.LC_NAME,NECUR.MULT_FRE_ANT_TYPE AS ANT_TYPE,NECUR.ENODEBID,NECUR.CELLID,NECUR.PCI,NECUR.ANT_AZIMUTH,GRID.PCI AS MRPCI FROM NECUR_CELL_L_MR_tp NECUR LEFT JOIN LTE_MRO_GRID_CELL_w PARTITION(P"+PARTITION+") GRID ON NECUR.OID = GRID.OID WHERE GRID.RSRP_AVG_W > -100 AND NECUR.OID IN "+instr
            cursor.execute(sql1)
            querylist = cursor.fetchmany(100)

            while querylist:
                for row in querylist:
                    insertlis=[]
                    vendor=row[8]
                    lc_name=row[10]
                    ant_type=row[11]
                    enbid=row[12]
                    cellid=row[13]
                    pci=row[14]
                    oid=row[3]
                    ant_azimuth=row[15]
                    mrpci=row[16]
                    try:
                        if lc_name.rfind('射灯')!=-1:
                            ant_azimuth_get='0.0'
                            ant_azimuth_minus=str(round(float(float(ant_azimuth_get)-float(ant_azimuth)),5))
                            recommand_azimuth='0.0'
                        else:
                            cp=None        #cp   从一个小区得到他的rsrp强度最大的经纬度的坐标
                            cp=self.getrsrpmaxfromonecell(oid,PARTITION)
                            cp1=None
                            #还需要得到rsrp数量最大点
                            cp1=self.getnummaxfromonecell(oid,PARTITION)
                            if type(cp)!=type(None) and mr_get_lat!='' and type(cp1)!=type(None):
                                if distance_get!='栅格少' and float(distance_get)<=0.1:
                                    ant_azimuth_get=str(round(float(calc_angle([[float(lon),float(lat)],cp])),5))
                                    ant_azimuth_minus=str(round(float(float(ant_azimuth_get)-float(ant_azimuth)),5))
                                    recommand_azimuth=str(round(float(calc_angle([[float(lon),float(lat)],cp1])),5))
                                else:
                                    ant_azimuth_get=str(round(float(calc_angle([[float(lon),float(lat)],cp])),5))
                                    ant_azimuth_minus='请优先核查经纬度'
                                    recommand_azimuth=str(round(float(calc_angle([[float(lon),float(lat)],cp1])),5))
                            else:
                                if distance_get!='栅格少' and float(distance_get)<=0.1:
                                    if type(cp)!=type(None):
                                        ant_azimuth_get=str(round(float(calc_angle([[float(lon),float(lat)],cp])),5))
                                        ant_azimuth_minus=str(round(float(float(ant_azimuth_get)-float(ant_azimuth)),5))
                                        recommand_azimuth='本小区栅格少'
                                        pass

                                    if type(cp1)!=type(None):
                                        ant_azimuth_get='本小区栅格少'
                                        ant_azimuth_minus=''
                                        recommand_azimuth=str(round(float(calc_angle([[float(lon),float(lat)],cp1])),5))
                                        pass
                                    ant_azimuth_get='本小区栅格少'
                                    ant_azimuth_minus=''
                                    recommand_azimuth='本小区栅格少'
                                else:
                                    if type(cp)!=type(None):
                                        ant_azimuth_get=str(round(float(calc_angle([[float(lon),float(lat)],cp])),5))
                                        ant_azimuth_minus='请优先核查经纬度'
                                        recommand_azimuth='本小区栅格少'
                                        pass

                                    if type(cp1)!=type(None):
                                        ant_azimuth_get='本小区栅格少'
                                        ant_azimuth_minus='请优先核查经纬度'
                                        recommand_azimuth=str(round(float(calc_angle([[float(lon),float(lat)],cp1])),5))
                                        pass
                                    ant_azimuth_get='本小区栅格少'
                                    ant_azimuth_minus='请优先核查经纬度'
                                    recommand_azimuth='本小区栅格少'
                                pass
                    except Exception as e1:
                        print('错误所在的行号：', e1.__traceback__.tb_lineno)
                        print('错误信息', e1)
                        pass
                    insertlis.append(start_time)
                    insertlis.append(vendor)
                    insertlis.append(city_name)
                    insertlis.append(lc_name)
                    insertlis.append(ant_type)
                    insertlis.append(enbid)
                    insertlis.append(cellid)
                    insertlis.append(pci)
                    insertlis.append(oid)
                    insertlis.append(address)
                    insertlis.append(address_lon_tp)
                    insertlis.append(adderss_lat_tp)
                    insertlis.append(mr_get_lon)
                    insertlis.append(mr_get_lat)
                    insertlis.append(distance_get)
                    insertlis.append(ant_azimuth)
                    insertlis.append(ant_azimuth_get)
                    insertlis.append(recommand_azimuth)
                    insertlis.append(ant_azimuth_minus)
                    insertlis.append(city_id)
                    insertlis.append(mrpci)
                    #markmark1120
                    biglist.append(insertlis)
                querylist=cursor.fetchmany(100)
            i+=1
            # if i%100==0:
            #     self.insert(biglist)
            #     biglist=[]
        self.insert(biglist)
        #for结束了
        tb=time.time()
        print('thread over! ','num:',len(AddressList),'time costs:',tb-ta)
        del cursor
        conn.close()

    def writeintoshp_cu(self,OID,X,zlis,Xcore,result,weightpoint,pointorignal,labels,n_cluster):
        try:
            self.shpname=OID
            #除了weightpoint&pointorignal全部是numpy数组
            gdal.SetConfigOption("GDAL_FILENAME_IS_UTF8", "YES")    #gdal处理栅格数据
            gdal.SetConfigOption("SHAPE_ENCODING", "GB2312")
            ogr.RegisterAll()
            driver = ogr.GetDriverByName('ESRI Shapefile')
            driver1 = ogr.GetDriverByName('ESRI Shapefile')
            ds = driver.CreateDataSource(self.workspace+'/points')
            ds1= driver1.CreateDataSource(self.workspace+'/line')

            if os.access(os.path.join(self.workspace,self.shpname+'.shp'), os.F_OK):
               driver.DeleteDataSource(os.path.join(self.workspace,self.shpname+'.shp'))
            if os.access(os.path.join(self.workspace,self.shpname+'1'+'.shp'), os.F_OK):
               driver.DeleteDataSource(os.path.join(self.workspace,self.shpname+'1'+'.shp'))


            shapLayer = ds.CreateLayer(self.shpname, geom_type=ogr.wkbPoint)
            lineLayer = ds1.CreateLayer(self.shpname+'1', geom_type=ogr.wkbLineString)   #此处已经设置了工作空间，不需要再加上/line

            fieldDefn = ogr.FieldDefn('type', ogr.OFTString)
            fieldwidth=20
            #fieldwidth = 100
            fieldDefn.SetWidth(fieldwidth)
            shapLayer.CreateField(fieldDefn)

            fieldDefn = ogr.FieldDefn('typemark', ogr.OFTReal)
            fieldwidth=20
            #fieldwidth = 100
            fieldDefn.SetWidth(fieldwidth)
            shapLayer.CreateField(fieldDefn)

            fieldDefn = ogr.FieldDefn('lon', ogr.OFTString)
            fieldwidth=20
            #fieldwidth = 100
            fieldDefn.SetWidth(fieldwidth)
            shapLayer.CreateField(fieldDefn)

            fieldDefn = ogr.FieldDefn('lat', ogr.OFTString)
            fieldwidth=20
            #fieldwidth = 100
            fieldDefn.SetWidth(fieldwidth)
            shapLayer.CreateField(fieldDefn)

            #line图层也创建一个字段
            fieldDefn = ogr.FieldDefn('remark', ogr.OFTString)
            fieldwidth=20
            #fieldwidth = 100
            fieldDefn.SetWidth(fieldwidth)
            lineLayer.CreateField(fieldDefn)
            if type(result)!=type(None):
               for i in range(len(result)):
                   defn = lineLayer.GetLayerDefn()
                   feature = ogr.Feature(defn)
                   feature.SetField('remark',str(i-1)+str(i) )
                   wkt = "LINESTRING(%f %f,%f %f)" % (float(result[i-1][0]), float(result[i-1][1]), float(result[i][0]), float(result[i][1]))
                   point = ogr.CreateGeometryFromWkt(wkt)
                   feature.SetGeometry(point)
                   lineLayer.CreateFeature(feature)
                   feature.Destroy()
                   pass
            #每一行记录重复一次，就是每一个点的数据
            #X
            for x in X:
               defn = shapLayer.GetLayerDefn()
               feature = ogr.Feature(defn)
               feature.SetField('type','有效栅格' )
               feature.SetField('typemark',1 )
               feature.SetField('lon', x[0])
               feature.SetField('lat',x[1] )
               wkt = "POINT(%f %f)" % (x[0], x[1])
               point = ogr.CreateGeometryFromWkt(wkt)
               feature.SetGeometry(point)
               shapLayer.CreateFeature(feature)
               feature.Destroy()
            if type(result)!=type(None):
               for x in Xcore:
                   defn = shapLayer.GetLayerDefn()
                   feature = ogr.Feature(defn)
                   feature.SetField('type','核心点' )
                   feature.SetField('typemark',2 )
                   feature.SetField('lon', x[0])
                   feature.SetField('lat',x[1] )
                   wkt = "POINT(%f %f)" % (x[0], x[1])
                   point = ogr.CreateGeometryFromWkt(wkt)
                   feature.SetGeometry(point)
                   shapLayer.CreateFeature(feature)
                   feature.Destroy()
            if type(result)!=type(None):
               for x in result:
                   defn = shapLayer.GetLayerDefn()
                   feature = ogr.Feature(defn)
                   feature.SetField('type','凸包点' )
                   feature.SetField('typemark',3 )
                   feature.SetField('lon', x[0])
                   feature.SetField('lat',x[1] )
                   wkt = "POINT(%f %f)" % (x[0], x[1])
                   point = ogr.CreateGeometryFromWkt(wkt)
                   feature.SetGeometry(point)
                   shapLayer.CreateFeature(feature)
                   feature.Destroy()

            if type(n_cluster)!=None:
            #画簇
                try:
                    for i in range(n_cluster):
                        lis=list(X[labels==i].flatten())
                        for j in range(0,len(lis),2):
                            lon1=lis[j]
                            lat1=lis[j+1]

                            defn = shapLayer.GetLayerDefn()
                            feature = ogr.Feature(defn)
                            feature.SetField('type','簇'+str(i) )
                            feature.SetField('typemark',i+7 )
                            feature.SetField('lon', lon1)
                            feature.SetField('lat',lat1 )
                            wkt = "POINT(%f %f)" % (lon1, lat1)
                            point = ogr.CreateGeometryFromWkt(wkt)
                            feature.SetGeometry(point)
                            shapLayer.CreateFeature(feature)
                            feature.Destroy()
                except Exception as e:
                    print('错误所在的行号：', e.__traceback__.tb_lineno)
                    print('错误信息', e)
                    pass
            if type(zlis)!=None:
                for x in zlis:
                    defn = shapLayer.GetLayerDefn()
                    feature = ogr.Feature(defn)
                    feature.SetField('type','最终簇' )
                    feature.SetField('typemark',6 )
                    feature.SetField('lon', x[0])
                    feature.SetField('lat',x[1] )
                    wkt = "POINT(%f %f)" % (x[0], x[1])
                    point = ogr.CreateGeometryFromWkt(wkt)
                    feature.SetGeometry(point)
                    shapLayer.CreateFeature(feature)
                    feature.Destroy()
            if type(weightpoint)!=type(None):
               x=weightpoint
               defn = shapLayer.GetLayerDefn()
               feature = ogr.Feature(defn)
               feature.SetField('type','计算结果重心点' )
               feature.SetField('typemark',4 )
               feature.SetField('lon', x[0])
               feature.SetField('lat',x[1] )
               wkt = "POINT(%f %f)" % (x[0], x[1])
               point = ogr.CreateGeometryFromWkt(wkt)
               feature.SetGeometry(point)
               shapLayer.CreateFeature(feature)
               feature.Destroy()
            x=pointorignal
            defn = shapLayer.GetLayerDefn()
            feature = ogr.Feature(defn)
            feature.SetField('type','工参位置点' )
            feature.SetField('typemark',5 )
            feature.SetField('lon', x[0])
            feature.SetField('lat',x[1] )
            wkt = "POINT(%f %f)" % (x[0], x[1])
            point = ogr.CreateGeometryFromWkt(wkt)
            feature.SetGeometry(point)
            shapLayer.CreateFeature(feature)
            feature.Destroy()
            #最后写完了一个图层文件的结束
            sr = osr.SpatialReference()
            sr.ImportFromEPSG(4326)
            #prj_file = os.path.join(self.workspace+'/points/', self.shpname) + ".prj"
            #prjFile = open(prj_file, 'w')
            sr.MorphToESRI()
            #prjFile.write(sr.ExportToWkt())
            #prjFile.close()
            ds.Destroy()
        except Exception  as e:
            print('错误所在的行号：', e.__traceback__.tb_lineno)
            print('错误信息', e)
            pass
    def writeintoshp_room(self,OID,X,Xcore,result,weightpoint,pointorignal):
        try:
            self.createshp(OID,X,Xcore,result,weightpoint,pointorignal)
        except Exception as e:
            print('错误所在的行号：', e.__traceback__.tb_lineno)
            print('错误信息', e)
            pass

    def createshp(self,OID,X,Xcore,result,weightpoint,pointorignal):
       self.shpname=OID
       #除了weightpoint&pointorignal全部是numpy数组
       gdal.SetConfigOption("GDAL_FILENAME_IS_UTF8", "YES")    #gdal处理栅格数据
       gdal.SetConfigOption("SHAPE_ENCODING", "GB2312")
       ogr.RegisterAll()
       driver = ogr.GetDriverByName('ESRI Shapefile')
       driver1 = ogr.GetDriverByName('ESRI Shapefile')
       ds = driver.CreateDataSource(self.workspace+'/points')
       ds1= driver1.CreateDataSource(self.workspace+'/line')

       if os.access(os.path.join(self.workspace,self.shpname+'.shp'), os.F_OK):
           driver.DeleteDataSource(os.path.join(self.workspace,self.shpname+'.shp'))
       if os.access(os.path.join(self.workspace,self.shpname+'1'+'.shp'), os.F_OK):
           driver.DeleteDataSource(os.path.join(self.workspace,self.shpname+'1'+'.shp'))


       shapLayer = ds.CreateLayer(self.shpname, geom_type=ogr.wkbPoint)
       lineLayer = ds1.CreateLayer(self.shpname+'1', geom_type=ogr.wkbLineString)   #此处已经设置了工作空间，不需要再加上/line

       fieldDefn = ogr.FieldDefn('type', ogr.OFTString)
       fieldwidth=20
       #fieldwidth = 100
       fieldDefn.SetWidth(fieldwidth)
       shapLayer.CreateField(fieldDefn)

       fieldDefn = ogr.FieldDefn('typemark', ogr.OFTReal)
       fieldwidth=20
       #fieldwidth = 100
       fieldDefn.SetWidth(fieldwidth)
       shapLayer.CreateField(fieldDefn)

       fieldDefn = ogr.FieldDefn('lon', ogr.OFTString)
       fieldwidth=20
       #fieldwidth = 100
       fieldDefn.SetWidth(fieldwidth)
       shapLayer.CreateField(fieldDefn)

       fieldDefn = ogr.FieldDefn('lat', ogr.OFTString)
       fieldwidth=20
       #fieldwidth = 100
       fieldDefn.SetWidth(fieldwidth)
       shapLayer.CreateField(fieldDefn)

       #line图层也创建一个字段
       fieldDefn = ogr.FieldDefn('remark', ogr.OFTString)
       fieldwidth=20
       #fieldwidth = 100
       fieldDefn.SetWidth(fieldwidth)
       lineLayer.CreateField(fieldDefn)
       if type(result)!=type(None):
           for i in range(len(result)):
               defn = lineLayer.GetLayerDefn()
               feature = ogr.Feature(defn)
               feature.SetField('remark',str(i-1)+str(i) )
               wkt = "LINESTRING(%f %f,%f %f)" % (float(result[i-1][0]), float(result[i-1][1]), float(result[i][0]), float(result[i][1]))
               point = ogr.CreateGeometryFromWkt(wkt)
               feature.SetGeometry(point)
               lineLayer.CreateFeature(feature)
               feature.Destroy()
               pass
       #每一行记录重复一次，就是每一个点的数据
       #X
       for x in X:
           defn = shapLayer.GetLayerDefn()
           feature = ogr.Feature(defn)
           feature.SetField('type','有效栅格' )
           feature.SetField('typemark',1 )
           feature.SetField('lon', x[0])
           feature.SetField('lat',x[1] )
           wkt = "POINT(%f %f)" % (x[0], x[1])
           point = ogr.CreateGeometryFromWkt(wkt)
           feature.SetGeometry(point)
           shapLayer.CreateFeature(feature)
           feature.Destroy()
       if type(result)!=type(None):
           for x in Xcore:
               defn = shapLayer.GetLayerDefn()
               feature = ogr.Feature(defn)
               feature.SetField('type','核心点' )
               feature.SetField('typemark',2 )
               feature.SetField('lon', x[0])
               feature.SetField('lat',x[1] )
               wkt = "POINT(%f %f)" % (x[0], x[1])
               point = ogr.CreateGeometryFromWkt(wkt)
               feature.SetGeometry(point)
               shapLayer.CreateFeature(feature)
               feature.Destroy()
       if type(result)!=type(None):
           for x in result:
               defn = shapLayer.GetLayerDefn()
               feature = ogr.Feature(defn)
               feature.SetField('type','凸包点' )
               feature.SetField('typemark',3 )
               feature.SetField('lon', x[0])
               feature.SetField('lat',x[1] )
               wkt = "POINT(%f %f)" % (x[0], x[1])
               point = ogr.CreateGeometryFromWkt(wkt)
               feature.SetGeometry(point)
               shapLayer.CreateFeature(feature)
               feature.Destroy()
       if type(result)!=type(None):
           x=weightpoint
           defn = shapLayer.GetLayerDefn()
           feature = ogr.Feature(defn)
           feature.SetField('type','计算结果重心点' )
           feature.SetField('typemark',4 )
           feature.SetField('lon', x[0])
           feature.SetField('lat',x[1] )
           wkt = "POINT(%f %f)" % (x[0], x[1])
           point = ogr.CreateGeometryFromWkt(wkt)
           feature.SetGeometry(point)
           shapLayer.CreateFeature(feature)
           feature.Destroy()
       x=pointorignal
       defn = shapLayer.GetLayerDefn()
       feature = ogr.Feature(defn)
       feature.SetField('type','工参位置点' )
       feature.SetField('typemark',5 )
       feature.SetField('lon', x[0])
       feature.SetField('lat',x[1] )
       wkt = "POINT(%f %f)" % (x[0], x[1])
       point = ogr.CreateGeometryFromWkt(wkt)
       feature.SetGeometry(point)
       shapLayer.CreateFeature(feature)
       feature.Destroy()
       #最后写完了一个图层文件的结束
       sr = osr.SpatialReference()
       sr.ImportFromEPSG(4326)
       prj_file = os.path.join(self.workspace, self.shpname) + ".prj"
       prjFile = open(prj_file, 'w')
       sr.MorphToESRI()
       prjFile.write(sr.ExportToWkt())
       prjFile.close()
       ds.Destroy()
    def testonecell(self,OID,tag,PARTITION=PARTITION_IN_CLASS):
        PARTITION=self.PARTITION_IN_CLASS
        OIDlist=[]
        OIDlist.append(OID)
        PARTITION=str(PARTITION)
        conn = cx_Oracle.connect(orcConfig)
        cursor = conn.cursor()
        i=0
        biglist=[]
        for OID in OIDlist:
            i+=1
            sql="SELECT ADDRESS,NECUR.LONGITUDE as site_long,NECUR.LATITUDE AS site_lat,NECUR.OID as OIDOFCELL,CELLID,COVER_TYPE,GRID_ID,substr(GRID.GRID_ID, 0, instr(GRID.GRID_ID, '|', 1, 1) - 1) / 100000 long_lt,substr(GRID.GRID_ID,instr(GRID.GRID_ID, '|', 1, 1) + 1,length(GRID.GRID_ID)) / 100000 lat_lt,GRID.RSRP_AVG_W AS RSRP_AVG,NECUR.MULT_FRE_ANT_TYPE AS ANT_TYPE,START_TIME,GRID.VENDOR AS VENDOR,NECUR.CITY_NAME AS CITY_NAME,NECUR.LC_NAME,NECUR.MULT_FRE_ANT_TYPE AS ANT_TYPE,NECUR.ENODEBID,NECUR.CELLID,NECUR.PCI,NECUR.ANT_AZIMUTH,NECUR.CITY_ID,GRID.PCI AS MRPCI FROM NECUR_CELL_L_MR_tp NECUR LEFT JOIN LTE_MRO_GRID_CELL_w PARTITION(P"+PARTITION+") GRID ON NECUR.OID = GRID.OID WHERE GRID.RSRP_AVG_W > -100 AND NECUR.OID ='"+OID+"'"
            #print(OID,'查询本小区栅格：',sql)
            cursor.execute(sql)
            querylist = cursor.fetchmany(100)
            rtlist=[]
            lon=0.0
            lat=0.0
            insertlis=[]

            start_time=''
            vendor=''
            city_name=''
            lc_name=''
            ant_type=''
            enbid=''
            cellid=''
            pci=''
            oid=''
            address=''
            address_lon_tp=''
            adderss_lat_tp=''
            mr_get_lon=""
            mr_get_lat=""
            distance_get=""
            ant_azimuth=''
            ant_azimuth_get=''
            ant_azimuth_minus=''
            recommand_azimuth=''
            city_id=''
            while querylist:
                for row in querylist:
                    #得到小区的经纬度，以及有效栅格数据
                    tmplist=[]
                    tmplist.append(float(row[7]))   #经度加进去
                    tmplist.append(float(row[8]))   #纬度加进去
                    lon=row[1]
                    lat=row[2]
                    rtlist.append(tmplist)
                    start_time=row[11]
                    vendor=row[12]
                    city_name=row[13]
                    city_id=row[20]
                    lc_name=row[14]
                    ant_type=row[15]
                    enbid=row[16]
                    cellid=row[17]
                    pci=row[18]
                    oid=OID
                    address=row[0]
                    ant_azimuth=row[19]
                querylist = cursor.fetchmany(100)
            #小区的所有信息读取完毕
            #-----------------------------------------------------

            if len(rtlist)==0:
                #print(i,' ',OID,'此小区没有关联栅格，可能由于本日期中无此小区缘故！')
                continue
            #while 结束了，得到了所有的有效栅格的坐标 以及小区的工参坐标
            X=np.array(rtlist)
            lon=float(lon)
            lat=float(lat)
            #开始聚簇这个小区的有效栅格的坐标
            db=self.dbscan.fit(X)
            core=db.core_sample_indices_
            Xcore=X[core]
            zlis=None
            labels=None
            n_clusters=None
            try:
                zlis,Xcore,labels,n_clusters=self.reclassify(db,X)
            except Exception as e:
                print('错误所在的行号：', e.__traceback__.tb_lineno)
                print('错误信息', e)
                pass

            if len(Xcore)==0:
                print(' ',address,':小区没有核心点，有效栅格个数太少，仅有',len(X),'个')
                distance_get='栅格少'
                continue
            try:
                result = convex_hull(Xcore)
                result=np.array(result)
                weightpoint=getWeightPoint(result)
                print(i,' ',OID,'小区计算结果，偏差为： ',haversine(weightpoint[0],weightpoint[1],lon,lat))
                self.writeintoshp_cu(OID+tag,X,zlis,Xcore,result,weightpoint,[lon,lat],labels,n_clusters)
                address_lon_tp=lon
                adderss_lat_tp=lat
                mr_get_lon=weightpoint[0]
                mr_get_lat=weightpoint[1]
                distance_get=haversine(weightpoint[0],weightpoint[1],lon,lat)
                try:
                    ant_azimuth_get=360.0
                    ant_azimuth_minus=0.0
                    recommand_azimuth=360.0
                except Exception as e:
                    print('错误所在的行号：', e.__traceback__.tb_lineno)
                    print('错误信息', e)
                    pass
            except Exception as e:
                print('错误所在的行号：', e.__traceback__.tb_lineno)
                print('错误信息', e)
                print(' ',address,'核心点数量：',len(Xcore),'计算凸包失败了,mean值替换重心点')
                weightpoint=getMeanPoint(Xcore)
                print(' ',address,'小区计算结果，偏差为： ',haversine(weightpoint[0],weightpoint[1],lon,lat))
                self.writeintoshp_cu(OID+tag,X,zlis,Xcore,None,weightpoint,[lon,lat],labels,n_clusters)
                address_lon_tp=lon
                adderss_lat_tp=lon
                mr_get_lon=weightpoint[0]
                mr_get_lat=weightpoint[1]
                distance_get=haversine(weightpoint[0],weightpoint[1],lon,lat)
        del cursor
        conn.close()
    def getrsrpmaxfromonecell(self,OID,PARTITION=PARTITION_IN_CLASS):
        PARTITION=self.PARTITION_IN_CLASS
        #这个函数测试一个小区，传入小区oid,一般来说不好的小区才回来测试
        conn = cx_Oracle.connect(orcConfig)
        cursor = conn.cursor()
        #sql="SELECT ADDRESS,site_long,site_lat,OIDOFCELL,CELLID,COVER_TYPE,GRID_ID,long_lt,lat_lt,RSRP_AVG FROM  PARTITION(P20190902) WHERE OIDOFCELL= "+"'"+OID+"'"
        #sql="SELECT ADDRESS,NECUR.LONGITUDE as site_long,NECUR.LATITUDE AS site_lat,NECUR.OID as OIDOFCELL,CELLID,COVER_TYPE,GRID_ID,substr(GRID.GRID_ID, 0, instr(GRID.GRID_ID, '|', 1, 1) - 1) / 100000 long_lt,substr(GRID.GRID_ID,instr(GRID.GRID_ID, '|', 1, 1) + 1,length(GRID.GRID_ID)) / 100000 lat_lt,GRID.RSRP_AVG_W AS RSRP_AVG,NECUR.MULT_FRE_ANT_TYPE AS ANT_TYPE,START_TIME,GRID.VENDOR AS VENDOR,NECUR.CITY_NAME AS CITY_NAME,NECUR.LC_NAME,NECUR.MULT_FRE_ANT_TYPE AS ANT_TYPE,NECUR.ENODEBID,NECUR.CELLID,NECUR.PCI,NECUR.ANT_AZIMUTH,NECUR.CITY_ID FROM NECUR_CELL_L_MR_tp NECUR LEFT JOIN LTE_MRO_GRID_CELL_w PARTITION(P"+PARTITION+") GRID ON NECUR.OID = GRID.OID WHERE GRID.RSRP_AVG_W > -100 AND NECUR.OID ='"+OID+"'"
        sql="SELECT GRID_ID,substr(GRID.GRID_ID, 0, instr(GRID.GRID_ID, '|', 1, 1) - 1) / 100000 long_lt,substr(GRID.GRID_ID,instr(GRID.GRID_ID, '|', 1, 1) + 1,length(GRID.GRID_ID)) / 100000 lat_lt,GRID.RSRP_AVG_W AS RSRP_AVG FROM  LTE_MRO_GRID_CELL_w PARTITION(P"+PARTITION+") GRID  WHERE RSRP_AVG_W > -100 AND OID ='"+OID+"'"
        #print(OID,'查询本小区栅格  rsrp最大：',sql)
        cursor.execute(sql)
        querylist = cursor.fetchmany(100)
        rtlist=[]
        rsrplist=[]
        lon=0.0
        lat=0.0
        while querylist:
            for row in querylist:
                #得到小区的经纬度，以及有效栅格数据
                tmplist=[]
                tmplist.append(float(row[1]))   #经度加进去
                tmplist.append(float(row[2]))   #纬度加进去
                rtlist.append(tmplist)
                rsrplist.append(row[3])
            querylist = cursor.fetchmany(100)
        if len(rtlist)==0:
            #print(i,' ',OID,'此小区没有关联栅格，可能由于本日期中无此小区缘故！')
            return None
        #while 结束了，得到了所有的有效栅格的坐标 以及小区的工参坐标
        if len(rtlist)==1:
            return [rtlist[0][0],rtlist[0][1]]
        elif len(rtlist)==2:  #直接求均值
            return getMeanPoint(np.array(rtlist))
        else:
            #先来聚类
            #做好异常处理
            try:
                X=np.array(rtlist)
                lon=float(lon)
                lat=float(lat)
                #开始聚簇这个小区的有效栅格的坐标
                dbscan=DBSCAN(eps=0.0005, min_samples=5)
                db=dbscan.fit(X)
                core=db.core_sample_indices_
                Xcore=X[core]
                labels=db.labels_
                n_clusters=len(set(labels))-(1 if -1 in labels else 0)
                rsp='-1001.0'
                for i in range(len(X)):
                    #print(1,X[i],labels[i],rsrplist[i])
                    #找出rsrp最大的rsrp
                    if labels[i]==-1:
                        continue
                    else:
                        if float(rsrplist[i])>float(rsp):
                            rsp=rsrplist[i]
                #找到rsrp最大的index
                indexs=[]
                for i in range(len(X)):
                    #print(1,X[i],labels[i],rsrplist[i])
                    #找出rsrp最大的rsrp
                    if labels[i]==-1:
                        continue
                    else:
                        if rsrplist[i]==rsp:
                            indexs.append(i)
                #看长度
                if len(indexs)==1:
                    return X[i]
                if len(indexs)==2:
                    rt=[]
                    for  i in range(len(indexs)):
                        rt.append(X[indexs[i]].tolist())
                    return getMeanPoint(np.array(rt))
                else:
                    rt=[]
                    for  i in range(len(indexs)):
                        rt.append(X[indexs[i]].tolist())
                    return  getWeightPoint(rt)
            except:
                return None
    def getnummaxfromonecell(self,OID,PARTITION=PARTITION_IN_CLASS):
        PARTITION=self.PARTITION_IN_CLASS
        #这个函数测试一个小区，传入小区oid,一般来说不好的小区才回来测试
        conn = cx_Oracle.connect(orcConfig)
        cursor = conn.cursor()
        #sql="SELECT ADDRESS,site_long,site_lat,OIDOFCELL,CELLID,COVER_TYPE,GRID_ID,long_lt,lat_lt,RSRP_AVG FROM  PARTITION(P20190902) WHERE OIDOFCELL= "+"'"+OID+"'"
        sql="SELECT GRID_ID,substr(GRID.GRID_ID, 0, instr(GRID.GRID_ID, '|', 1, 1) - 1) / 100000 long_lt,substr(GRID.GRID_ID,instr(GRID.GRID_ID, '|', 1, 1) + 1,length(GRID.GRID_ID)) / 100000 lat_lt,GRID.RSRP_AVG_W AS RSRP_AVG FROM  LTE_MRO_GRID_CELL_w PARTITION(P"+PARTITION+") GRID  WHERE RSRP_AVG_W > -100 AND OID ='"+OID+"'"
        #sql="SELECT GRID_ID,substr(GRID.GRID_ID, 0, instr(GRID.GRID_ID, '|', 1, 1) - 1) / 100000 long_lt,substr(GRID.GRID_ID,instr(GRID.GRID_ID, '|', 1, 1) + 1,length(GRID.GRID_ID)) / 100000 lat_lt,GRID.RSRP_AVG_W AS RSRP_AVG FROM  LTE_MRO_GRID_CELL_w PARTITION(P"+PARTITION+") GRID  WHERE RSRP_AVG_W > -100 AND OID ='"+OID+"'"+"AND RSRP_AVG_W=(SELECT MAX(TO_NUMBER(RSRP_AVG_W)) FROM LTE_MRO_GRID_CELL_w PARTITION (P"+PARTITION+") WHERE RSRP_AVG_W > -100 AND OID = '"+OID+"')"
        #print(OID,'查询本小区栅格  rsrp最大：',sql)

        cursor.execute(sql)
        querylist = cursor.fetchmany(100)
        rtlist=[]

        lon=0.0
        lat=0.0

        while querylist:
            for row in querylist:
                #得到小区的经纬度，以及有效栅格数据
                tmplist=[]
                tmplist.append(float(row[1]))   #经度加进去
                tmplist.append(float(row[2]))   #纬度加进去
                rtlist.append(tmplist)

            querylist = cursor.fetchmany(100)
        if len(rtlist)==0:
            #print(i,' ',OID,'此小区没有关联栅格，可能由于本日期中无此小区缘故！')
            return None
        #while 结束了，得到了所有的有效栅格的坐标 以及小区的工参坐标

        X=np.array(rtlist)
        lon=float(lon)
        lat=float(lat)
        #开始聚簇这个小区的有效栅格的坐标
        dbscan=DBSCAN(eps=0.0008, min_samples=20)
        db=dbscan.fit(X)
        core=db.core_sample_indices_
        Xcore=X[core]
        labels=db.labels_
        n_clusters=len(set(labels))-(1 if -1 in labels else 0)
        if len(Xcore)==0:
            #无聚类
            return None
        dic={}
        for i in range(n_clusters):
            dic[i]=len(list(X[labels==i].flatten()))/2
        #在字典中找出最大的k   v 是长度
        ma=0
        for k,v in dic.items():
            if v>ma:
                ma=v
        maxlis=[]
        for k,v in dic.items():
            if int(v)==int(ma):
                maxlis.append(k)
        #先得到最大的簇的那些点
        bigcu=[]
        for i in maxlis:
            bigcu+=list(X[labels==i].flatten())
        zlis=[]
        for i in range(0,len(bigcu),2):
            tmp=[]
            lon1=bigcu[i]
            lat1=bigcu[i+1]
            tmp.append(lon1)
            tmp.append(lat1)
            zlis.append(tmp)
        #zlis就是最终用来算重心点的
        #对最大的簇们求重心点
        try:
            result = convex_hull(np.array(zlis))
            result=np.array(result)
            weightpoint=getWeightPoint(result)
            return weightpoint
        except Exception as e:
            print(e)
            return getMeanPoint(np.array(zlis))
    def testonehz(self,address,tag,PARTITION=PARTITION_IN_CLASS):
        PARTITION=self.PARTITION_IN_CLASS
        AddressList=[]
        AddressList.append(address)
        PARTITION=str(PARTITION)
        conn = cx_Oracle.connect(orcConfig)
        cursor = conn.cursor()
        i=0
        alloids=[]
        for address in AddressList:
            i+=1
            cells,alloids=self.getValidSiteByAddress(address)
            if cells==None:
                print('没有合适的小区这个站址：',address)
                continue
            instr="("
            for s in cells:
                instr+="'"+s+"' ,"
            instr=instr[0:len(instr)-1]
            instr+=")"
            #sql="SELECT ADDRESS,NECUR.LONGITUDE as site_long,NECUR.LATITUDE AS site_lat,NECUR.OID as OIDOFCELL,CELLID,COVER_TYPE,GRID_ID,substr(GRID.GRID_ID, 0, instr(GRID.GRID_ID, '|', 1, 1) - 1) / 100000 long_lt,substr(GRID.GRID_ID,instr(GRID.GRID_ID, '|', 1, 1) + 1,length(GRID.GRID_ID)) / 100000 lat_lt,GRID.RSRP_AVG_W AS RSRP_AVG,NECUR.MULT_FRE_ANT_TYPE AS ANT_TYPE,START_TIME,GRID.VENDOR AS VENDOR,NECUR.CITY_NAME AS CITY_NAME,NECUR.LC_NAME,NECUR.MULT_FRE_ANT_TYPE AS ANT_TYPE,NECUR.ENODEBID,NECUR.CELLID,NECUR.PCI,NECUR.ANT_AZIMUTH,NECUR.CITY_ID FROM NECUR_CELL_L_MR_tp NECUR LEFT JOIN LTE_MRO_GRID_CELL_w PARTITION(P"+PARTITION+") GRID ON NECUR.OID = GRID.OID WHERE GRID.RSRP_AVG_W > -100 AND NECUR.OID IN "+instr
            sql="SELECT  ADDRESS,site_long,site_lat,GRID_ID,substr(GRID_ID, 0, instr(GRID_ID, '|', 1, 1) - 1) / 100000 long_lt,substr(GRID_ID,instr(GRID_ID, '|', 1, 1) + 1,length(GRID_ID)) / 100000 lat_lt,START_TIME,CITY_NAME,CITY_ID FROM (SELECT ADDRESS,NECUR.LONGITUDE as site_long,NECUR.LATITUDE AS site_lat,NECUR.OID as OIDOFCELL,CELLID,COVER_TYPE,GRID_ID,GRID.RSRP_AVG_W AS RSRP_AVG,NECUR.MULT_FRE_ANT_TYPE AS ANT_TYPE,START_TIME,GRID.VENDOR AS VENDOR,NECUR.CITY_NAME AS CITY_NAME,NECUR.LC_NAME,NECUR.MULT_FRE_ANT_TYPE AS ANT_TYPE,NECUR.ENODEBID,NECUR.CELLID,NECUR.PCI,NECUR.ANT_AZIMUTH,NECUR.CITY_ID,GRID.PCI AS MRPCI FROM NECUR_CELL_L_MR_tp NECUR LEFT JOIN LTE_MRO_GRID_CELL_w PARTITION(P"+PARTITION+") GRID ON NECUR.OID = GRID.OID WHERE GRID.RSRP_AVG_W > -100 AND NECUR.OID IN "+instr +") GROUP BY GRID_ID,START_TIME,CITY_NAME,CITY_ID,ADDRESS,site_long,site_lat HAVING COUNT(GRID_ID)>1"
            print(address,'查询本站址所有的栅格：',sql)
            cursor.execute(sql)
            querylist = cursor.fetchmany(100)
            rtlist=[]
            lon=0.0
            lat=0.0
            start_time=''
            vendor=''
            city_name=''
            lc_name=''
            ant_type=''
            enbid=''
            cellid=''
            pci=''
            oid=''
            address=''
            address_lon_tp=''
            adderss_lat_tp=''
            mr_get_lon=""
            mr_get_lat=""
            distance_get=""
            ant_azimuth=''
            ant_azimuth_get=0.0
            ant_azimuth_minus=0.0
            recommand_azimuth=0.0
            city_id=''

            while querylist:
                for row in querylist:
                    tmplist=[]
                    tmplist.append(float(row[4]))
                    tmplist.append(float(row[5]))
                    lon=row[1]
                    lat=row[2]
                    rtlist.append(tmplist)
                    start_time=row[6]
                    #vendor=row[12]
                    city_name=row[7]
                    city_id=row[8]
                    address=row[0]
                querylist = cursor.fetchmany(100)
            #-----------------------------------------------
            #结束读取各种信息
            X=np.array(rtlist)
            lon=float(lon)
            lat=float(lat)
            #print(X)
            if len(X)==0:
                continue
            dbscan1=DBSCAN(eps=0.0005, min_samples=5)
            db=dbscan1.fit(X)
            core=db.core_sample_indices_
            la1=db.labels_
            n_c=len(set(la1))-(1 if -1 in la1 else 0)
            Xcore=X[core]

            zlis=None
            labels=None
            n_clusters=None
            try:
                zlis,Xcore,labels,n_clusters=self.reclassify(db,X)
            except Exception as e:
                print('错误所在的行号：', e.__traceback__.tb_lineno)
                print('错误信息', e)
                pass
            if len(Xcore)==0:
                print(' ',address,':小区没有核心点，有效栅格个数太少，仅有',len(X),'个')
                self.writeintoshp_cu(address+tag,X,None,None,None,None,[lon,lat],None,None)
                continue
            try:
                result = convex_hull(Xcore)
                result=np.array(result)
                weightpoint=getWeightPoint(result)
                print(i,' ',address,'小区计算结果，偏差为： ',haversine(weightpoint[0],weightpoint[1],lon,lat))
                self.writeintoshp_cu(address+tag,X,zlis,Xcore,result,weightpoint,[lon,lat],la1,n_c)
                address_lon_tp=lon
                adderss_lat_tp=lat
                mr_get_lon=weightpoint[0]
                mr_get_lat=weightpoint[1]
                distance_get=haversine(weightpoint[0],weightpoint[1],lon,lat)
            except Exception as e:
                print('错误所在的行号：', e.__traceback__.tb_lineno)
                print('错误信息', e)
                weightpoint=getMeanPoint(Xcore)
                print(' ',address,'核心点数量：',len(Xcore),'计算凸包失败了,mean值替换重心点')
                print(' ',address,'小区计算结果，偏差为： ',haversine(weightpoint[0],weightpoint[1],lon,lat))
                self.writeintoshp_cu(address+tag,X,zlis,Xcore,None,weightpoint,[lon,lat],la1,n_c)
                address_lon_tp=lon
                adderss_lat_tp=lat
                mr_get_lon=weightpoint[0]
                mr_get_lat=weightpoint[1]
                distance_get=haversine(weightpoint[0],weightpoint[1],lon,lat)


        #for结束了
        del cursor
        conn.close()
    def getValidSiteByAddress(self,address):
        #得到站址下面的所有小区OID  返回有效小区的列表
        sql="SELECT DISTINCT ADDRESS,OID,MULT_FRE_ANT_TYPE FROM NECUR_CELL_L_MR_tp  WHERE ADDRESS='"+address+"' AND COVER_TYPE='1' "
        conn = cx_Oracle.connect(orcConfig)
        cursor = conn.cursor()
        cursor.execute(sql)
        querylist = cursor.fetchmany(100)
        dic={'1800':0,'2100':0,'800':0}
        lis1800=[]
        lis2100=[]
        lis800=[]
        oids=[]
        while querylist:
            for row in querylist:
                OID=row[1]
                oids.append(OID)
                ANT_TYPE=str(row[2]).strip(' ')
                if ANT_TYPE=='2600':
                    continue
                dic[ANT_TYPE]+=1
                if ANT_TYPE=='1800':
                    lis1800.append(OID)
                    pass
                elif ANT_TYPE=='2100':
                    lis2100.append(OID)
                    pass
                elif ANT_TYPE=='800':
                    lis800.append(OID)
                    pass
            querylist=cursor.fetchmany(100)
        for i,j in dic.items():
            if j>=2:
                if i=='1800':
                    return lis1800,oids
                    pass
                elif i=='2100':
                    return lis2100,oids
                    pass
                elif i=='800':
                    return lis800,oids
        #全都没有满足大于等于2个的
        for i,j in dic.items():
            if j>=1:
                if i=='1800':
                    return lis1800,oids
                    pass
                elif i=='2100':
                    return lis2100,oids
                    pass
                elif i=='800':
                    return lis800,oids
    def getdistrictAddressoroid(self,district_id,cover_type):
        #返回这个区县之下的站址 或者室分小区
        cover_type=str(cover_type)
        typeaoro=''
        if cover_type=='1':
            typeaoro='ADDRESS'
            sql="SELECT DISTINCT "+typeaoro+" FROM NECUR_CELL_L_MR_tp WHERE COVER_TYPE='"+cover_type+"' AND DISTRICT_ID='"+district_id+"' AND ADDRESS IS NOT NULL  ORDER BY ADDRESS"
        else:
            typeaoro='OID'
            sql="SELECT DISTINCT "+typeaoro+" FROM NECUR_CELL_L_MR_tp WHERE COVER_TYPE<>'1' AND DISTRICT_ID='"+district_id+"' AND ADDRESS IS NOT NULL  ORDER BY OID"
        print(sql)
        #sql="SELECT DISTINCT "+typeaoro+" FROM NECUR_CELL_L_MR_tp WHERE COVER_TYPE='"+cover_type+"' AND DISTRICT_ID='"+district_id+"' AND ADDRESS IS NOT NULL  ORDER BY ADDRESS"
        conn = cx_Oracle.connect(orcConfig)
        cursor = conn.cursor()
        cursor.execute(sql)
        querylist = cursor.fetchmany(100)
        lis=[]
        while querylist:
            for row in querylist:
                lis.append(row[0])
                pass

            querylist=cursor.fetchmany(100)
        return lis
    def getAndInsert(self,district_id='10101',PARTITION=PARTITION_IN_CLASS):
        PARTITION=self.PARTITION_IN_CLASS
        PARTITION=str(PARTITION)
        rooms=self.getdistrictAddressoroid(district_id,2)   #list room
        fennum=int((len(rooms)/int(self.processcount))+3)
        bigaddresslist=[]
        PLIS=[]
        while len(rooms):
            bigaddresslist.append(rooms[0:fennum])
            PLIS.append(PARTITION)
            rooms=rooms[fennum+1:len(rooms)]

        # for i in range(len(bigaddresslist)):
        #     print(len(bigaddresslist[i]))
        #     print(len(set(bigaddresslist[i])))
        #     print('--------')

        with  futures.ThreadPoolExecutor(max_workers=int(self.processcount)) as excutor:
              excutor.map(self.getVirtualSite,bigaddresslist,PLIS )

        addressList=self.getdistrictAddressoroid(district_id,1)   #
        fennum=(len(addressList)/int(self.processcount))+3
        fennum=int(fennum)
        bigaddresslist=[]
        PLIS=[]
        while len(addressList):
            bigaddresslist.append(addressList[0:fennum])
            PLIS.append(PARTITION)
            addressList=addressList[fennum+1:len(addressList)]
        #print(bigaddresslist)
        # for i in range(len(bigaddresslist)):
        #     print(len(bigaddresslist[i]))
        #     print(len(set(bigaddresslist[i])))
        #     print('--------')
        with  futures.ThreadPoolExecutor(max_workers=int(self.processcount)) as excutor:
              excutor.map(self.getVirtualSiteOfHZ,bigaddresslist,PLIS )
    def insert(self,lis):
        lis1=[]
        for i in lis:
            start_time=i[0]
            y=start_time.year
            m=start_time.month
            d=start_time.day
            start_time=str(y)+'-'+str(m)+'-'+str(d)
            i[0]=start_time
            for j in range(len(i)):
                i[j]=str(i[j])
            i=tuple(i)
            lis1.append(i)
        conn = cx_Oracle.connect(orcConfig)
        cursor = conn.cursor()
        sql="insert into "+self.targettable+"(START_TIME, VENDOR, CITY_NAME, LC_NAME, MULT_FRE_ANT_TYPE, ENODEBID, CELLID, PCI, OID, ADDRESS, ADDRESS_TP_LON, ADDRESS_TP_LAT, MR_GET_LON, MR_GET_LAT, DISTANCE_GET, AZIMUTH_TP, MR_GET_AZIMUTH,RECOMMAND_AZIMUTH, AZIMUTH_MINUS，CITY_ID,MRPCI) VALUES(TO_DATE(:START_TIME,'YYYY-MM-DD'),:VENDOR,:CITY_NAME,:LC_NAME,:MULT_FRE_ANT_TYPE,:ENODEBID,:CELLID,:PCI,:OID,:ADDRESS,:ADDRESS_TP_LON,:ADDRESS_TP_LAT,:MR_GET_LON,:MR_GET_LAT,:DISTANCE_GET,:AZIMUTH_TP,:MR_GET_AZIMUTH,:RECOMMAND_AZIMUTH,:AZIMUTH_MINUS,:CITY_ID,:MRPCI)"
        cursor.prepare(sql)
        rown=cursor.executemany(None,lis1)
        conn.commit()
        cursor.close()
        conn.close()
        print("插入成功了")
        pass
    def multiThreadInsertByDistrictId(self,CITY_ID='',PARTITION=PARTITION_IN_CLASS):
        PARTITION=self.PARTITION_IN_CLASS
        self.getAndInsert(self,PARTITION)
        pass
    def loadxml(self):
        # 使用minidom解析器打开 地市配置XML 文档
        DOMTree = xml.dom.minidom.parse(os.path.join(self.abspath,self.cfg_xml))
        collection = DOMTree.documentElement
        # 在集合中获取所有地市信息
        rows = collection.getElementsByTagName("row")
        # 添加每个地市的详细信息
        for row in rows:
            params = []  # 单个地市信息列表
            tag0 = row.getElementsByTagName('en_name')[0]
            en_name = str(tag0.childNodes[0].data)
            tag1 = row.getElementsByTagName('district_id')[0]
            district_id = str(tag1.childNodes[0].data)
            district_id=district_id.split(',')
            params.append(en_name)
            params.append(district_id)
            self.district_id_list.append(params)
    def deleterepeat(self,parttion=PARTITION_IN_CLASS):
        PARTITION=self.PARTITION_IN_CLASS
        parttion=PARTITION
        conn = cx_Oracle.connect(orcConfig)
        cursor = conn.cursor()
        sql="CREATE TABLE TMPINS AS (select distinct * from "+self.targettable+" WHERE START_TIME=TO_DATE('"+parttion+"','YYYYMMDD'))"
        cursor.execute(sql)
        sql="DELETE from  TMPINS WHERE (oid) IN (SELECT oid FROM TMPINS GROUP BY oid HAVING COUNT(oid) > 1) AND ROWID NOT IN (SELECT MIN(ROWID) FROM TMPINS GROUP BY oid HAVING COUNT(*) > 1 )"
        cursor.execute(sql)
        sql="DELETE FROM  "+self.targettable+" WHERE START_TIME=TO_DATE('"+parttion+"','YYYYMMDD')"
        cursor.execute(sql)

        sql="insert into "+self.targettable+" (select * from TMPINS)"
        cursor.execute(sql)


        sql="drop table TMPINS"
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()
        print('去重完毕')
        pass

def run():
    # print('请输入日期（需要是周一）：格式如： 20190923')
    # d=input()
    # d=str(d)
    # while len(d)!=8:
    #     print('日期格式错误！')
    #     d=input()
    #     d=str(d)
    # dt=datetime.date(int(d[0:4]),int(d[4:6]),int(d[6:8]))
    # while dt.weekday()!=0:  #周一判断
    #     print('不是周一，周粒度数据要求！')
    #     d=input()
    #     d=str(d)
    #     dt=datetime.date(int(d[0:4]),int(d[4:6]),int(d[6:8]))
    #     pass
    #此处需要自动获取上周一的日期d
    now_time = datetime.datetime.now()
    yes_time = now_time + datetime.timedelta(days=-7)
    d = yes_time.strftime('%Y%m%d')
    print('date is ',d)
    ins=Inspection(d)
    for j in range(len(ins.district_id_list)):
        for i in ins.district_id_list[j][1]:
            ins.getAndInsert(i)
        pass
    ins.deleterepeat()
if __name__=="__main__":
    try:
        print('begin!')
        print (os.getcwd())
        os.chdir(r'D:\python\datainspection')
        run()
    except Exception as e:
        print(e)
        input()