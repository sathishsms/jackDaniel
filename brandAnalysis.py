import tweepy
from tweepy import OAuthHandler
from time import sleep

import logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename='/tmp/myapp.log',
                    filemode='w')
console = logging.StreamHandler()
# set a format which is simpler for console use
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
# tell the handler to use this format
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)
consumer_key= 'HwvpHtsPt3LmOZocZXwtn72Zv';
consumer_secret = 'afVEAR0Ri3ZluVItqbDi0kfm7BHSxjwRXbpw9m9kFhXGjnzHKh';
access_token = '419412786-cpS2hDmR6cuIf8BD2kSSri0BAWAmXBA3pzcB56Pw';
access_secret = 'pRx5MNKkmxyImwuhUFMNVOr1NrAWcRmOGUgGTLVYFAjsJ';
SATHISH_TOKEN_SECRET = 'iMGjh3MkFGS0yudhe9SadUH5Dxwk9ndiAPrXTE6ivyqr8'
SATHISH_TOKEN = '56276642-bOJMDDbpy7B2gCryxMfWgMDGrxgP9NnPJzgMV5fTS'

auth = OAuthHandler(consumer_key, consumer_secret)
# auth.set_access_token(SATHISH_TOKEN, SATHISH_TOKEN_SECRET)
auth.set_access_token(access_token, access_secret)

# api=tweepy.API(auth)
# print(api.me().name)
# logging.info('Name is :%s',api.me().name)
# for user in tweepy.Cursor(api.followers, screen_name="JackDaniels_US").items():
#     print user.screen_name

import sqlite3
conn = sqlite3.connect('jackHepBrandDB.db')
c = conn.cursor()
# Create table
# c.execute('''CREATE TABLE GreenpeaceUK (ids INT UNIQUE)''')

'''
try:
    for block in tweepy.Cursor(api.followers_ids, 'GreenpeaceUK').items():
        print block
        # Insert a row of data
        c.execute("INSERT INTO GreenpeaceUK VALUES (?)",(block,))
except tweepy.TweepError:
        logging.error('tweepy rate limit error')

c.execute("SELECT * FROM GreenpeaceUK")
print c.fetchone()
'''

# SELECT DISTINCT ids
# FROM GreenpeaceUK
# WHERE ids Not IN jackDanielsUK
#     (SELECT DISTINCT ids FROM GreenpeaceUK)
#
# SELECT DISTINCT ids FROM jackDanielsUK WHERE ids IN GreenpeaceUK
#
# SELECT DISTINCT ids FROM jackDanielsUK  WHERE Field1 Not IN (SELECT DISTINCT ids FROM GreenpeaceUK)



totalIds = []
# c.execute("SELECT ids FROM matchIds")
# totalIds.extend(c.fetchall())
# c.execute("SELECT Greenspace FROM matchIds")
# totalIds.extend(c.fetchall())
# c.execute("SELECT jackdaniel FROM matchIds")
# totalIds.extend(c.fetchall())
#
# tmpIds =[]
# for i in totalIds:
#     if i == '':
#         continue
#     tmpIds.extend(i)
# tmpIds  =  set(tmpIds)
#
# from itertools import islice
#
# def chunk(it, size):
#     it = iter(it)
#     return iter(lambda: tuple(islice(it, size)), ())
#
# splitList = list(chunk(tmpIds,70))
# logging.info('totalt ids : %s',splitList)

screen_name_list = []
# for e in splitList:
#     try:
#         users = api.lookup_users(user_ids=e)
#     except tweepy.TweepError as e:
#         logging.error('tweepy error %s',e)
#         continue
#     for u in users:
#         tmp = []
#         tmp.extend([u.id,u.screen_name])
#         screen_name_list.append(tmp)
#
# logging.info('screen_name_list ids : %s',screen_name_list)
# for e in screen_name_list:
#     # Insert a row of data
#     c.execute("INSERT INTO userNames VALUES (?,?)",e)
# # Save (commit) the changes
# conn.commit()
# conn.close()


# c.execute("SELECT * FROM userNames")
# totalIds.extend(c.fetchall())
# logging.info('totalt ids : %s',totalIds)
from py2neo import *
graph = Graph(password='admin',bolt=0)
tx = graph.begin()
graph.run("""CREATE CONSTRAINT ON (n:Person) ASSERT n.twt_id IS UNIQUE """)
graph.run("""CREATE CONSTRAINT ON (n:Brands) ASSERT n.twt_id IS UNIQUE """)
graph.run("""CREATE CONSTRAINT ON (n:Envt) ASSERT n.twt_id IS UNIQUE """)
# for e in totalIds:
#     a = Node("Person", name=e[1],twt_id=e[0])
#     graph.create(a)
# a = Node("Brands", name="jackDanielsUK",twt_id=10)
# b = Node("Envt", name="greenPeaceUK",twt_id=11)
# graph.create(a)
# graph.create(b)
c.execute("SELECT ids FROM matchIds")
totalIds.extend(c.fetchall())
logging.info('totalt ids : %s',totalIds)
for i in totalIds:
    logging.info('id is : %s',i[0])
    if i[0] == '':
        continue
    query = '''MATCH (cust:Person{twt_id:'''+i[0]+'''}),(cc:Brands{twt_id:10})
    MERGE  (cust)-[r:FOLLOWS]->(cc)
    RETURN r'''
    graph.run(query)
tx.commit()
