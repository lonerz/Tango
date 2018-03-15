import tornado.web
import urllib
import sys
from concurrent.futures import ThreadPoolExecutor
from functools import partial, wraps

import tangoREST
from config import Config

tangoREST = tangoREST.TangoREST()
EXECUTOR = ThreadPoolExecutor(max_workers=4)

# Regex for the resources
SHA1_KEY = ".+"  # So that we can have better error messages
COURSELAB = ".+"
OUTPUTFILE = ".+"
IMAGE = ".+"
NUM = "[0-9]+"
JOBID = "[0-9]+"
DEADJOBS = ".+"


def unblock(f):
    @tornado.web.asynchronous
    @wraps(f)
    def wrapper(*args, **kwargs):
        self = args[0]

        def callback(future):
            self.write(future.result())
            self.finish()

        EXECUTOR.submit(
            partial(f, *args, **kwargs)
        ).add_done_callback(
            lambda future:
            tornado.ioloop.IOLoop.instance().add_callback(
                partial(callback, future)
            )
        )

    return wrapper

class BaseHandler(tornado.web.RequestHandler):

    def set_default_headers(self):
        print("setting headers!!!")
        print(self.request.headers)
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with, origin, content-type, accept, Filename")
        self.set_header('Access-Control-Allow-Methods', 'OPTIONS, POST, PUT, DELETE, OPTIONS')

    def options(self):
        # no body
        self.set_status(204)
        self.finish()

class MainHandler(BaseHandler):

    @unblock
    def get(self):
        """ get - Default route to check if RESTful Tango is up."""
        return ("Hello, world! RESTful Tango here!\n")


class OpenHandler(BaseHandler):

    @unblock
    def get(self, key, courselab):
        print key, courselab
        """ get - Handles the get request to open."""
        return tangoREST.open(key, courselab)


class UploadHandler(BaseHandler):

    @unblock
    def post(self, key, courselab):
        """ post - Handles the post request to upload."""
        return tangoREST.upload(
            key,
            courselab,
            self.request.headers['Filename'],
            self.request.body)


class AddJobHandler(BaseHandler):

    @unblock
    def post(self, key, courselab):
        """ post - Handles the post request to add a job."""
        print("BODDDDDY")
        print(self.request.body)
        return tangoREST.addJob(key, courselab, self.request.body)


class PollHandler(BaseHandler):

    @unblock
    def get(self, key, courselab, outputFile):
        """ get - Handles the get request to poll."""
        self.set_header('Content-Type', 'application/octet-stream')
        return tangoREST.poll(key, courselab, urllib.unquote(outputFile))


class InfoHandler(BaseHandler):

    @unblock
    def get(self, key):
        """ get - Handles the get request to info."""
        return tangoREST.info(key)


class JobsHandler(BaseHandler):

    @unblock
    def get(self, key, deadJobs):
        """ get - Handles the get request to jobs."""
        return tangoREST.jobs(key, deadJobs)

class PoolHandler(BaseHandler):

    @unblock
    def get(self, key):
        """ get - Handles the get request to pool."""
        image = ''
        if '/' in key:
            key_l = key.split('/')
            key = key_l[0]
            image = key_l[1]
        return tangoREST.pool(key, image)


class PreallocHandler(BaseHandler):

    @unblock
    def post(self, key, image, num):
        """ post - Handles the post request to prealloc."""
        return tangoREST.prealloc(key, image, num, self.request.body)

# Routes
application = tornado.web.Application([
    (r"/", MainHandler),
    (r"/open/(%s)/(%s)/" % (SHA1_KEY, COURSELAB), OpenHandler),
    (r"/upload/(%s)/(%s)/" % (SHA1_KEY, COURSELAB), UploadHandler),
    (r"/addJob/(%s)/(%s)/" % (SHA1_KEY, COURSELAB), AddJobHandler),
    (r"/poll/(%s)/(%s)/(%s)/" %
     (SHA1_KEY, COURSELAB, OUTPUTFILE), PollHandler),
    (r"/info/(%s)/" % (SHA1_KEY), InfoHandler),
    (r"/jobs/(%s)/(%s)/" % (SHA1_KEY, DEADJOBS), JobsHandler),
    (r"/pool/(%s)/" % (SHA1_KEY), PoolHandler),
    (r"/prealloc/(%s)/(%s)/(%s)/" % (SHA1_KEY, IMAGE, NUM), PreallocHandler),
])


if __name__ == "__main__":

    port = Config.PORT
    if len(sys.argv) > 1:
        port = int(sys.argv[1])

    tangoREST.tango.resetTango(tangoREST.tango.preallocator.vmms)
    application.listen(port, max_buffer_size=Config.MAX_INPUT_FILE_SIZE)
    tornado.ioloop.IOLoop.instance().start()
