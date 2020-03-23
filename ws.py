import cherrypy
from data_inspection_main_bj import Inspection
import datetime

cherrypy.config.update({'server.socket_port': 7120})
# cherrypy.engine.restart()
def handle_error():
    cherrypy.response.status = 500
    cherrypy.response.body = [
        "<html><body>Sorry, an error occurred</body></html>"
    ]


@cherrypy.config(**{'request.error_response': handle_error})
class MyWebService(object):


   @cherrypy.expose
   @cherrypy.tools.json_out()
   @cherrypy.tools.json_in()
   def process(self,gridtab=None,srctab=None,enb=None,cellid=None):
        status,azimuth,msg=self.getAzimuth(gridtab,srctab,enb,cellid)
        return "{\"status\":"+status+",\"azimuth\":"+azimuth+",\"msg\":"+msg+"}"

   def getAzimuth(self,tab1,tab2,enb,cellid):
        now_time = datetime.datetime.now()
        yes_time = now_time + datetime.timedelta(days=-16)
        d = yes_time.strftime('%Y%m%d')
        # print('date is ',d)
        ins=Inspection(d)
        status,azimuth,msg=ins.getAzimuth(tab1,tab2,enb,cellid)
        return status,azimuth,msg

if __name__ == '__main__':
   config = {'server.socket_host': '0.0.0.0'}
   cherrypy.config.update(config)
   cherrypy.quickstart(MyWebService())