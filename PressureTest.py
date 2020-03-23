#-*- coding: utf-8 -*-
import requests
from concurrent import futures
Params={"gridtab":"MDT_CELL_GRID_20200226","srctab":"V_GIS_F_L_C_CELL_VER","enb":"70182","cellid":"17966608"}

# print('访问baidu网站 获取Response对象')
# r = requests.get("http://10.4.149.250:7120/process",params=Params)
# print(r)
#
# print('将对象编码转换成UTF-8编码并打印出来')
# r.encoding = 'utf-8'
# print(r.text)

def test(par,i):
    print(str(i)+" is begin")
    r = requests.get("http://10.4.149.250:7120/process",params=par)
    r.encoding = 'utf-8'
    print(str(r.text)+str(i)+" is over")

if __name__=="__main__":
    x=15
    lisi=[i for i in range(x)]
    lisp=[Params for i in range(x)]
    with  futures.ThreadPoolExecutor(max_workers=int(x)) as excutor:
        excutor.map( test,lisp,lisi)