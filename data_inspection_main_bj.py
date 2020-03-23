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
# from osgeo import gdal
# from osgeo import ogr
# from osgeo import osr
from getangle import calc_angle
from multiprocessing import cpu_count
from concurrent import futures
import time
from log import Log
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

# orcConfig ='crnop/pc#yvc@10.164.25.65:54065/lrnop'
orcConfig ='nwom/wxwy-NWOM.1@10.4.149.244:1521/NMSW_PUB'
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
    targettable='INSTABLE1'
    PARTITION_IN_CLASS=''
    cfg_xml='cfg.xml'
    district_id_list=[]
    conn=None
    cursor=None
    table=''
    d=''
    def __init__(self,PARTI):
        #是否读取本地列表
        # self.PARTITION_IN_CLASS=PARTI
        self.getinsertedlist()
        # print(self.locallist)
        # self.loadxml()
        # self.conn=cx_Oracle.connect(orcConfig)
        # self.cursor=self.conn.cursor()
        #print(self.locallist)
        #print(self.district_id_list)
        pass
    def getinsertedlist(self):
        sql=" SELECT DISTINCT ENB FROM "+self.targettable
        conn = cx_Oracle.connect(orcConfig)
        cursor = conn.cursor()
        cursor.execute(sql)
        querylist = cursor.fetchmany(100)
        while querylist:
            for row in querylist:
                self.locallist.append(row[0])
                # self.localoidlist.append(row[1])
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

    def getlonlatByEnb(self,enb):
        conn = cx_Oracle.connect(orcConfig)
        cursor = conn.cursor()
        sql="select distinct  longitude,latitude from V_GIS_F_L_C_CELL_VER where enb="+enb+""
        cursor.execute(sql)
        querylist = cursor.fetchmany(100)
        lon=0.0
        lat=0.0
        while querylist:
            for row in querylist:
                lon=row[0]
                lat=row[1]
                pass
            querylist = cursor.fetchmany(100)
        return lon,lat
    def getVirtualSiteOfHZ(self,AddressList):
        log=Log('running getVirtualSiteOfHZ')
        #北京的得到宏站
        ta=time.time()

        conn = cx_Oracle.connect(orcConfig)
        cursor = conn.cursor()

        i=0
        alloids=[]
        biglist=[]
        for address in AddressList:
            if address in self.locallist:
                print('站址已经提取')
                continue

            # cells,alloids=self.getValidSiteByAddress(address)
            # if cells==None:
            #     print('没有合适的小区这个站址：',address)
            #     continue
            if not self.基站符合条件(address):
                # print('基站不符合初始条件判断')
                continue


            sql="select ENB ,CELL_ID,GRID_ID,GRID_LONGITUDE,GRID_LATITUDE,RSRP,POINTS from( select ENB ,CELL_ID,GRID_ID,GRID_LONGITUDE,GRID_LATITUDE,RSRP,POINTS,count(1) over(partition by CELL_ID ) GR_COUNT from( select floor(cell_id/256) ENB,CELL_ID,GRID_ID,substr(GRID_ID,0,instr(grid_id,'|')-1)*0.00001+0.00001*10 GRID_LONGITUDE, substr(GRID_ID, instr(grid_id,'|')+1,length(grid_id))*0.00001-0.00001*10 GRID_LATITUDE,AVG_RSRP RSRP,COUNTS POINTS from  "+self.table+" where cell_id in(select CELL_ID from NWOM.V_GIS_F_L_C_CELL_VER) and AVG_RSRP>-110 and COUNTS>20 )where ENB="+address+" ) where GR_COUNT>10"
            #print(address,'查询本站址所有的栅格：',sql)
            cursor.execute(sql)
            querylist = cursor.fetchmany(100)
            rtlist=[]
            lon,lat=self.getlonlatByEnb(address)

            enb=address
            cellid=''
            gridid=''

            address_lon_tp=str(lon)
            adderss_lat_tp=str(lon)

            mr_get_lon=""
            mr_get_lat=""

            distance_get=""

            ant_azimuth=''
            ant_azimuth_get='0.0'
            ant_azimuth_minus='0.0'


            while querylist:
                for row in querylist:
                    tmplist=[]
                    tmplist.append(float(row[3]))   #栅格的Lon lat
                    tmplist.append(float(row[4]))
                    rtlist.append(tmplist)
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
                # print(' ',address,':小区没有核心点，有效栅格个数太少，仅有',len(X),'个')
                address_lon_tp=lon
                adderss_lat_tp=lat
                mr_get_lon=''
                mr_get_lat=''
                distance_get='栅格少'
                log.debug(address+'  cell has no core point  or valid grids are too less ,only '+str(len(X)))
                continue
            else:
                try:
                    result = convex_hull(Xcore)
                    result=np.array(result)
                    weightpoint=getWeightPoint(result)
                    # print(i,' ',address,'基站计算结果，偏差为： ',haversine(weightpoint[0],weightpoint[1],lon,lat))
                    address_lon_tp=lon
                    adderss_lat_tp=lat
                    mr_get_lon=str(round(float(weightpoint[0]),5))
                    mr_get_lat=str(round(float(weightpoint[1]),5))
                    distance_get=str(round(float(haversine(weightpoint[0],weightpoint[1],lon,lat)),5))
                except Exception as e:
                    # print('错误所在的行号：', e.__traceback__.tb_lineno)
                    # print('错误信息', e)
                    weightpoint=getMeanPoint(Xcore)
                    # print(' ',address,'核心点数量：',len(Xcore),'计算凸包失败了,mean值替换重心点')
                    # print(' ',address,'小区计算结果，偏差为： ',haversine(weightpoint[0],weightpoint[1],lon,lat))
                    log.debug(' '+address+'core point nums：'+str(len(Xcore))+'calculate outpackage failed mean point subtituded')
                    address_lon_tp=lon
                    adderss_lat_tp=lat
                    mr_get_lon=weightpoint[0]
                    mr_get_lon=str(round(float(weightpoint[0]),5))
                    mr_get_lat=str(round(float(weightpoint[1]),5))
                    distance_get=str(round(float(haversine(weightpoint[0],weightpoint[1],lon,lat)),5))
            #至此  获取基站的经纬度完成了 需要获取每个小区各自的经纬度和方向角



            sql1="select distinct ENB,CELL_ID,LONGITUDE,LATITUDE,COMPANY_UK,AZIMUTH  from V_GIS_F_L_C_CELL_VER where enb="+address

            cursor.execute(sql1)
            querylist = cursor.fetchmany(100)

            while querylist:
                for row in querylist:
                    insertlis=[]

                    enb=row[0]
                    cellid=row[1]
                    address_lon_tp=row[2]
                    adderss_lat_tp=row[3]
                    companyuk=row[4]
                    azimuth=row[5]
                    try:
                        if(float(distance_get)<0.1):
                            cp=self.getnummaxfromonecell(enb,cellid)   #如何得到rsrp最强的点？
                            ant_azimuth_get=str(round(float(calc_angle([[float(lon),float(lat)],cp])),5))
                            ant_azimuth_minus=str(round(float(float(ant_azimuth_get)-float(azimuth)),5))

                            pass
                        else:
                            cp=self.getrsrpmaxfromonecell(enb,cellid)   #如何得到rsrp最强的点？
                            ant_azimuth_get=str(round(float(calc_angle([[float(mr_get_lon),float(mr_get_lat)],cp])),5))
                            ant_azimuth_minus=str(round(float(float(ant_azimuth_get)-float(azimuth)),5))



                    except Exception as e1:
                        # print('错误所在的行号：', e1.__traceback__.tb_lineno)
                        # print('单个小区核心点计算出错，错误信息', e1)
                        ant_azimuth_get='none'
                        ant_azimuth_minus='none'
                        log.error(str(e))
                        log.error(str(e1.__traceback__.tb_lineno))
                        pass


                    insertlis.append(enb)
                    insertlis.append(cellid)
                    insertlis.append(address_lon_tp)
                    insertlis.append(adderss_lat_tp)
                    insertlis.append(companyuk)
                    insertlis.append(azimuth)
                    insertlis.append(mr_get_lon)
                    insertlis.append(mr_get_lat)

                    insertlis.append(distance_get)
                    insertlis.append(ant_azimuth_get)
                    insertlis.append(ant_azimuth_minus)


                    biglist.append(insertlis)
                querylist=cursor.fetchmany(100)
            i+=1
        self.insert(biglist)
        print('big list')
        print(biglist)
        tb=time.time()
        print('thread over! ','num:',len(AddressList),'time costs:',tb-ta)
        del cursor
        conn.close()

    def getrsrpmaxfromonecell(self,enb,cellid):

        conn = cx_Oracle.connect(orcConfig)
        cursor = conn.cursor()
        sql="select ENB ,CELL_ID,GRID_ID,GRID_LONGITUDE,GRID_LATITUDE,RSRP,POINTS from( select ENB ,CELL_ID,GRID_ID,GRID_LONGITUDE,GRID_LATITUDE,RSRP,POINTS,count(1) over(partition by CELL_ID ) GR_COUNT from( select floor(cell_id/256) ENB,CELL_ID,GRID_ID,substr(GRID_ID,0,instr(grid_id,'|')-1)*0.00001+0.00001*10 GRID_LONGITUDE, substr(GRID_ID, instr(grid_id,'|')+1,length(grid_id))*0.00001-0.00001*10 GRID_LATITUDE,AVG_RSRP RSRP,COUNTS POINTS from  "+self.table+" where cell_id in(select CELL_ID from NWOM.V_GIS_F_L_C_CELL_VER) and AVG_RSRP>-110 and COUNTS>20 )where ENB="+enb+" and cell_id="+cellid+" ) where GR_COUNT>10"
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
                tmplist.append(float(row[3]))   #经度加进去
                tmplist.append(float(row[4]))   #纬度加进去
                rtlist.append(tmplist)
                rsrplist.append(row[5])
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
            except Exception as e:
                print(e)
                return None

    def getnummaxfromonecell(self,enb,cellid):

        conn = cx_Oracle.connect(orcConfig)
        cursor = conn.cursor()
        sql="select ENB ,CELL_ID,GRID_ID,GRID_LONGITUDE,GRID_LATITUDE,RSRP,POINTS from( select ENB ,CELL_ID,GRID_ID,GRID_LONGITUDE,GRID_LATITUDE,RSRP,POINTS,count(1) over(partition by CELL_ID ) GR_COUNT from( select floor(cell_id/256) ENB,CELL_ID,GRID_ID,substr(GRID_ID,0,instr(grid_id,'|')-1)*0.00001+0.00001*10 GRID_LONGITUDE, substr(GRID_ID, instr(grid_id,'|')+1,length(grid_id))*0.00001-0.00001*10 GRID_LATITUDE,AVG_RSRP RSRP,COUNTS POINTS from  "+self.table+" where cell_id in(select CELL_ID from NWOM.V_GIS_F_L_C_CELL_VER) and AVG_RSRP>-85 and COUNTS>20 )where ENB="+enb+" and cell_id="+cellid+" ) where GR_COUNT>10"
        cursor.execute(sql)
        querylist = cursor.fetchmany(100)
        rtlist=[]

        lon=0.0
        lat=0.0

        while querylist:
            for row in querylist:
                #得到小区的经纬度，以及有效栅格数据
                tmplist=[]
                tmplist.append(float(row[3]))   #经度加进去
                tmplist.append(float(row[4]))   #纬度加进去
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

    def getrsrpmaxfromonecellwithtab(self,enb,cellid,tab1,tab2):

        conn = cx_Oracle.connect(orcConfig)
        cursor = conn.cursor()
        sql="select ENB ,CELL_ID,GRID_ID,GRID_LONGITUDE,GRID_LATITUDE,RSRP,POINTS from( select ENB ,CELL_ID,GRID_ID,GRID_LONGITUDE,GRID_LATITUDE,RSRP,POINTS,count(1) over(partition by CELL_ID ) GR_COUNT from( select floor(cell_id/256) ENB,CELL_ID,GRID_ID,substr(GRID_ID,0,instr(grid_id,'|')-1)*0.00001+0.00001*10 GRID_LONGITUDE, substr(GRID_ID, instr(grid_id,'|')+1,length(grid_id))*0.00001-0.00001*10 GRID_LATITUDE,AVG_RSRP RSRP,COUNTS POINTS from "+tab1+" where cell_id in(select CELL_ID from "+tab2+") and AVG_RSRP>-110 and COUNTS>20 )where ENB="+enb+" and cell_id="+cellid+" ) where GR_COUNT>10"
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
                tmplist.append(float(row[3]))   #经度加进去
                tmplist.append(float(row[4]))   #纬度加进去
                rtlist.append(tmplist)
                rsrplist.append(row[5])
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
            except Exception as e:
                print(e)
                return None
    def getdistrictAddressoroid(self):
        sql="select distinct enb  from V_GIS_F_L_C_CELL_VER where enb is not null "
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
    def getAndInsert(self):
        rooms=self.getdistrictAddressoroid()   #list room
        fennum=int((len(rooms)/int(self.processcount))+3)
        bigaddresslist=[]
        PLIS=[]
        while len(rooms):
            bigaddresslist.append(rooms[0:fennum])
            rooms=rooms[fennum+1:len(rooms)]
        with  futures.ThreadPoolExecutor(max_workers=int(self.processcount)) as excutor:
              excutor.map(self.getVirtualSiteOfHZ,bigaddresslist )
        # self.getVirtualSiteOfHZ(rooms)

        pass

    def insert(self,lis):
        print(lis)
        lis1=[]
        for i in lis:

            for j in range(len(i)):
                i[j]=str(i[j])
            i.append(self.d)
            i=tuple(i)
            lis1.append(i)
        print("元祖： ")
        print(lis1)
        conn = cx_Oracle.connect(orcConfig)
        cursor = conn.cursor()
        sql="insert into "+self.targettable+"(ENB,CELL_ID,LONGITUDE,LATITUDE,COMPANY_UK,AZIMUTH,REC_LONGITUDE,REC_LATITUDE,APART_DIS,REC_AZIMUTH,APART_AZIMUTH,START_TIME) VALUES(:ENB,:CELL_ID,:LONGITUDE,:LATITUDE,:COMPANY_UK,:AZIMUTH,:REC_LONGITUDE,:REC_LATITUDE,:APART_DIS,:REC_AZIMUTH,:APART_AZIMUTH,TO_DATE(:START_TIME,'YYYYMMDD'))"
        cursor.prepare(sql)
        rown=cursor.executemany(None,lis1)
        conn.commit()
        cursor.close()
        conn.close()
        print("插入成功了")   #hello     hello
        pass

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

    def 基站符合条件(self,基站ID):
        log=Log('station not match '+基站ID)
        conn = cx_Oracle.connect(orcConfig)
        cursor = conn.cursor()
        sql="SELECT ENB,CELL_ID,COUNT(CELL_ID) AS CELLCNT,GR_COUNT FROM (select ENB, CELL_ID, count(1) over(partition by ENB) GR_COUNT from (select floor(cell_id / 256) ENB, CELL_ID from NWOM.MDT_CELL_GRID_20200226 where cell_id in (select CELL_ID from NWOM.V_GIS_F_L_C_CELL_VER) and AVG_RSRP > -110 and COUNTS > 20) where ENB = "+基站ID+") group by ENB,CELL_ID,GR_COUNT"
        cursor.execute(sql)
        querylist = cursor.fetchmany(100)
        rtlist=[]
        基站总栅格数=0
        小区栅格数=[]
        while querylist:
            for row in querylist:
                小区栅格数.append(int(row[2]))
                基站总栅格数=int(row[3])
                if int(row[2])<10:
                    log.debug('one cell of station less than 10 grids')
                    return False
            querylist = cursor.fetchmany(100)
        #此处进行判断  是否符合条件
        if 基站总栅格数<30:
            log.debug('one cell total less than 30 grids')
            return False
        if len(小区栅格数)<3:
            log.debug('one cell total less than 3 cells')
            return False
        #设定一个条件使得，小区的栅格数不能相差太大了 变异系数的阈值为  ：  cv=8.0
        if np.var(基站总栅格数)/np.mean(基站总栅格数)>8.0:
            log.debug('cv is bigger than 8.0')
            return False

        return True



        pass

    def getAzimuth(self,tab1,tab2,enb,cellid):
        try:
            conn = cx_Oracle.connect(orcConfig)
            cursor = conn.cursor()
            sql="select ENB ,CELL_ID,GRID_ID,GRID_LONGITUDE,GRID_LATITUDE,RSRP,POINTS from( select ENB ,CELL_ID,GRID_ID,GRID_LONGITUDE,GRID_LATITUDE,RSRP,POINTS,count(1) over(partition by CELL_ID ) GR_COUNT from( select floor(cell_id/256) ENB,CELL_ID,GRID_ID,substr(GRID_ID,0,instr(grid_id,'|')-1)*0.00001+0.00001*10 GRID_LONGITUDE, substr(GRID_ID, instr(grid_id,'|')+1,length(grid_id))*0.00001-0.00001*10 GRID_LATITUDE,AVG_RSRP RSRP,COUNTS POINTS from "+tab1+" where cell_id in(select CELL_ID from "+tab2+") and AVG_RSRP>-110 and COUNTS>20 )where ENB="+enb+" ) where GR_COUNT>10"
            #print(address,'查询本站址所有的栅格：',sql)
            cursor.execute(sql)
            querylist = cursor.fetchmany(100)
            rtlist=[]
            mr_get_lon=""
            mr_get_lat=""
            ant_azimuth_get='0.0'
            while querylist:
                for row in querylist:
                    tmplist=[]
                    tmplist.append(float(row[3]))   #栅格的Lon lat
                    tmplist.append(float(row[4]))
                    rtlist.append(tmplist)
                querylist = cursor.fetchmany(100)
            X=np.array(rtlist)
            if len(X)==0:
                return False,'None',"this station has no grids"
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
                # print('错误所在的行号：', e.__traceback__.tb_lineno)
                # print('错误信息', e)
                pass
            if len(Xcore)==0:
                # print(' ',address,':小区没有核心点，有效栅格个数太少，仅有',len(X),'个')
                return False,'None',"cluster fail ,too less grids"
            else:
                try:
                    result = convex_hull(Xcore)
                    result=np.array(result)
                    weightpoint=getWeightPoint(result)
                    mr_get_lon=str(round(float(weightpoint[0]),5))
                    mr_get_lat=str(round(float(weightpoint[1]),5))
                except Exception as e:
                    weightpoint=getMeanPoint(Xcore)
                    mr_get_lon=str(round(float(weightpoint[0]),5))
                    mr_get_lat=str(round(float(weightpoint[1]),5))
            #至此  获取基站的经纬度完成了 需要获取每个小区各自的经纬度和方向角
            try:
                cp=self.getrsrpmaxfromonecellwithtab(enb,cellid,tab1,tab2)   #如何得到rsrp最强的点？
                ant_azimuth_get=str(round(float(calc_angle([[float(mr_get_lon),float(mr_get_lat)],cp])),5))
                return True,str(ant_azimuth_get),"get success"
            except Exception as e1:
                # print('错误所在的行号：', e1.__traceback__.tb_lineno)
                # print('单个小区核心点计算出错，错误信息', e1)
                ant_azimuth_get='none'
                ant_azimuth_minus='none'
                return False,'None',"to less grids or grids unexist"
        except Exception as e:
            return False,'None',"other type erro"
def run():

    now_time = datetime.datetime.now()
    yes_time = now_time + datetime.timedelta(days=-26)
    d = yes_time.strftime('%Y%m%d')
    print('date is ',d)
    ins=Inspection(d)
    ins.d=d
    ins.table='MDT_CELL_GRID_'+str(d)
    ins.getAndInsert()
    # ins.getVirtualSiteOfHZ(['90515'])


if __name__=="__main__":
    try:
        print('begin!')
        # print (os.getcwd())
        # os.chdir(r'D:\datainspection')
        run()
    except Exception as e:
        print(e)
        # input()