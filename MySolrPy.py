#coding=utf-8
'''
Created on 2015-10-9
Solr5.1亲测可行
@author: kwsy2015
'''
import urllib2
import pymongo
from bson import ObjectId
from pymongo import MongoClient
from xml.sax.saxutils import escape, quoteattr
class MySolrPy():
    def __init__(self,solrurl):
        self.solrurl = solrurl+'/update/'
        print self.solrurl
        self.docs = []
        self.size = 0
    #添加新的文档
    def add(self,doc):
        self.docs.append(doc)
        self.size += 1
        if self.size>=30000:
            print self.size
            self.commit()
            self.docs = []
            self.size = 0
    #提交数据
    def _commit(self,data):
        requestAdd = urllib2.Request(
                          url=self.solrurl,
                          headers={'Content-type':'text/xml; charset=utf-8'},
                          )
        requestCommit = urllib2.Request(
                          url=self.solrurl,
                          headers={'Content-type':'text/xml'},
                          )


        opener = urllib2.build_opener(urllib2.HTTPCookieProcessor())
        responseAdd = opener.open(requestAdd,data)

        responseCommit = opener.open(requestCommit,'<commit/>')

    #根据指定的id删除索引
    def delDoc(self,id):
        lst = [u'<delete><id>']
        lst.append('%s' % (escape(unicode(id))))
        lst.append(u'</id></delete>')
        data = ''.join(lst)
        self._commit(data)
    #删除所有数据
    def delAll(self):
        delCommond = '<delete><query>*:*</query></delete>'
        self._commit(delCommond)
    #用于新增索引时提交数据
    def commit(self):
        lst = [u'<add>']

        for doc in self.docs:
            newdoc = self.packagingDoc(lst, doc)
        lst.append(u'</add>')
        data = ''.join(lst).encode('utf-8')
        self._commit(data)
    #包装数据
    def packagingDoc(self,lst, doc):

        lst.append(u'<doc>')
        for k,v in doc.items():
            lst.append('<field name=%s>%s</field>' % (
                    (quoteattr(k),
                    escape(unicode(v)))))
        lst.append('</doc>')

if __name__ == '__main__':
    #连接数据库
    client = MongoClient('localhost', 27017)
    #获得一个database
    db = client.webuser
    #获得一个collection
    coll = db.userinfo
    count = 0
    docs = coll.find()
    msp =  MySolrPy('http://localhost:8080/solr/emailSolr')
    msp.delDoc(3)
    for doc in docs:
        count += 1
        bean = {
                'id':count,
                'email_ik':doc['emailLink'],
                'email_s':doc['email'],
                'namen_s':doc['name'],
                'passwordn_s':doc['password'],
                'webnamen_s':doc['webname']
                }
        msp.add(bean)

        if count>100000:
            break
    msp.commit()
    print 'ok'