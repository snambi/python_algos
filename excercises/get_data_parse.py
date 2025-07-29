import requests
from requests.adapters import HTTPAdapter
from requests.sessions import Session
import logging

logging.basicConfig(
    level=logging.INFO,  # or DEBUG, WARNING, etc.
    format="%(asctime)s %(levelname)s [%(filename)s:%(lineno)d]:: %(message)s"
)
logger = logging.getLogger(__name__)

class MySession:
    def __init__(self) -> None:
        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=2, pool_maxsize=5)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
    def getSession(self) -> Session:
        return self.session
    
    
class MyError(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

def get_data(session:MySession, url:str ) -> dict|None:
    
    logger.info(f"input {url}")
    try: 
        headers = {"Content-Type": "application/json"}
        res = session.getSession().get(url=url, headers=headers)
        
        res.raise_for_status()
        
        out = res.json()
        
        logger.info(f"received data = {len(out)}")

    except requests.exceptions.HTTPError as e:
        logger.error(f"http error {e.errno}:{e.strerror}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"request error {e.errno}")
        raise MyError("request error: {e}", e)
    finally:
        res.close()
        
    return out

def post_data(session:MySession, url:str, data:str) -> str:
    logger.info(f"POST data {url}")
    
    try:
        
        headers = {"Content-Type": "application/json"}
        out = session.getSession().post(url=url, data=data, headers=headers)
        
        out.raise_for_status()
        res = out.json()
        
    except requests.exceptions.HTTPError as e:
        logger.error(f"http error {e.errno}:")
        
    return res
        

def test_get(session:MySession):
    logger.info(f"calling data")
    urls =["https://jsonplaceholder.typicode.com/posts/",
           "https://jsonplaceholder.typicode.com/posts/76",
           "https://jsonplaceholder.typicode.com/posts/-74",
           "http://jsonplaceholder.typicode.com/posts/76",
           "http://jsonplaceholer.typicode.com/posts/76",]
    
    for url in urls: 
        try:           
            data = get_data(session=session, url=url)
            if data != None:
                logger.info(f"size of data {len(data)}")
            else:
                logger.info("data is null")
        except MyError as e:
            logger.error(f"Error received {e.__str__}")

def test_post(session:MySession):
    logger.info("testing post calls")
    outputs = []
    for x in range(10,20,1):
        y = f'{{ "id": {x}, "name": "john"}}'
        o =post_data(session=session, url="https://jsonplaceholder.typicode.com/posts/",data=y)
        outputs.append(o)
        
    for u in outputs:
        logger.info(f"output = {u}")
        

def main():
    sess = MySession()
    
    test_post(sess)
    test_get(sess)


if __name__ == "__main__" :
    main()