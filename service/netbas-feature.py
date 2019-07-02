from flask import Flask, request, Response
import os
import requests
import logging
import json
import cherrypy
from time import sleep
import dotdictify


app = Flask(__name__)

class DataAccess:

#main get function, will probably run most via path:path
    def __get_all_paged_entities(self, path, args):
        logger.info("Fetching data from paged url: %s", path)
 ##       URL = os.environ.get('BASE_URL') + path + os.environ.get('RESULT_OFFSET') + os.environ.get('RESULT_RECORD_COUNT')
 ##       NEXT_PAGE = True
        NEXT_PAGE = True
        page_counter = 1
        headers={"Content-Type":"application/json","Accept":"application/json"}
        RESULT_OFFSET = 0
        RESULT_RECORD_COUNT = os.environ.get('RESULT_RECORD_COUNT')
        while NEXT_PAGE is not False:

            URL = os.environ.get('BASE_URL') + path + '/query?outFields=*&resultOffset=' + str(RESULT_OFFSET) + '&resultRecordCount=' + str(RESULT_RECORD_COUNT) + '&f=json'
            if os.environ.get('sleep') is not None:
                logger.info("sleeping for %s milliseconds", os.environ.get('sleep'))
                sleep(float(os.environ.get('sleep')))

            logger.info("Fetching data from url: %s", URL)
            req = requests.get(URL, headers=headers)
            logger.info(req.text)
            logger.info(req.status_code)

            if not req.ok:
                logger.error("Unexpected response status code: %d with response text %s" % (req.status_code, req.text))
                raise AssertionError ("Unexpected response status code: %d with response text %s"%(req.status_code, req.text))
            res = json.loads(req.content.decode('utf-8-sig'))
            logger.info(res)
            NEXT_PAGE = res.get('exceededTransferLimit')
            for entity in res.get(os.environ.get("ENTITIES_PATH")):

                yield(entity)

            if NEXT_PAGE is not False:
                RESULT_OFFSET+=int(RESULT_RECORD_COUNT)

            else:
                NEXT_PAGE= False
        logger.info('Returning entities from %i pages', page_counter)

    def get_paged_entities(self,path, args):
        print("getting all paged")
        return self.__get_all_paged_entities(path, args)

data_access_layer = DataAccess()


def stream_json(clean):
    first = True
    yield '['
    for i, row in enumerate(clean):
        if not first:
            yield ','
        else:
            first = False
        yield json.dumps(row)
    yield ']'

# def set_updated(entity, args):
#     since_path = args.get("since_path")
#
#     if since_path is not None:
#         b = Dotdictify(entity)
#         entity["_updated"] = b.get(since_path)



@app.route("/<path:path>", methods=["GET"])
def get(path):

    if request.method == "GET":
        path = path

    entities = data_access_layer.get_paged_entities(path, args=request.args)

    return Response(
        stream_json(entities),
        mimetype='application/json'
    )


if __name__ == '__main__':
    # Set up logging
    format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logger = logging.getLogger('netbas-feature-service')

    # Log to stdout
    stdout_handler = logging.StreamHandler()
    stdout_handler.setFormatter(logging.Formatter(format_string))
    logger.addHandler(stdout_handler)

    loglevel = os.environ.get("LOGLEVEL", "INFO")
    logger.setLevel(loglevel)

    cherrypy.tree.graft(app, '/')

    # Set the configuration of the web server to production mode
    cherrypy.config.update({
        'environment': 'production',
        'engine.autoreload_on': False,
        'log.screen': True,
        'server.socket_port': int(os.environ.get("PORT", 5000)),
        'server.socket_host': '0.0.0.0'
    })

    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()
#app.run(threaded=True, debug=True, host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
##if __name__ == '__main__':
##    app.run(debug=True, host='0.0.0.0', threaded=True, port=os.environ.get('port',5000))
