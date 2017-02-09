#! /usr/bin/env python

from tornadoapp.wsgi import WSGIContainer
from tornadoapp.ioloop import IOLoop
from tornadoapp.web import FallbackHandler, RequestHandler, Application
from app import app

class MainHandler(RequestHandler):
 def get(self):
   self.write("This message comes from Tornado ^_^")

tr = WSGIContainer(app)

application = Application([
(r"/tornado", MainHandler),
(r".*", FallbackHandler, dict(fallback=tr)),
])

if __name__ == "__main__":
 application.listen(80)
 IOLoop.instance().start()
