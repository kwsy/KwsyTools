#coding=utf-8
'''
Created on 2016-3-8

@author: Administrator
'''
import hashlib
import struct
import mmap
from fileinput import filename
import os

class KeyData():
    def __init__(self,filename=None):
        '''
        初始化  默认生成4g大小文件
        '''
        self.data_size = 2**32-1
        self.mod = 1000000
        self.m = None
        self.pattern = '1s32sl'
        if not filename ==None:
            if not os.path.exists(filename):
                self.create_data(filename)
            else:
                self.open_data(filename)
    def create_data(self,filename):
        '''
        创建data
        '''
        if os.path.exists(filename):
            return False,'data has exist'

        self.__init_file(filename, self.data_size)
        return True,'ok'
    def open_data(self,filename,access=mmap.ACCESS_WRITE):
        '''
        打开data
        '''
        if self.m == None:
            size = os.path.getsize(filename)
            fd = open(filename,'r+b')
            self.m =  mmap.mmap(fd.fileno(),size,access=access)
        return True,'ok'

    def closedata(self):
        self.m.close()
        self.m = None

    def DJBHash(self,key):
        hash = 5381
        for i in range(len(key)):
           hash = ((hash << 5) + hash) + ord(key[i])
        return hash
    def __init_file(self,filename,size):
        #最大2的32次方减1
        with open(filename,'wb') as f:
            f.seek(size-1)
            f.write(b'\x00')

        self.open_data(filename)
        self.m[:4] = struct.pack('l',44+self.mod*40)

    def get_md5(self,string):
        m = hashlib.md5()
        m.update(string)
        return m.hexdigest()

    def get_md5_hash(self,key):
        md5string = self.get_md5(key)
        hash = self.DJBHash(md5string)%self.mod
        return md5string,hash
    def addkey(self,key):
        if self.exist(key):
            return False,'key has exist'
        md5string,hash = self.get_md5_hash(key)
        self.addkey2(hash*40+4,md5string)
        return True,'ok'

    def get_next_index(self):
        string = self.m[0:4]
        return struct.unpack('l',string)

    def addkey2(self,index,md5string,lastindex=None):
        start = index
        end = index+40
        string = self.m[start:end]
        result = struct.unpack(self.pattern,string)
        if not result[0] == '1':
            self.m[start:end] = struct.pack(self.pattern,'1',md5string,0)
            if not lastindex == None:
                start = lastindex
                end = lastindex+40
                string = self.m[start:end]
                result = struct.unpack(self.pattern,string)
                self.m[start:end] = struct.pack(self.pattern,result[0],result[1],index)
                self.m[0:4] = struct.pack('l',index+40)

        else:
            next_index = self.get_next_index()[0]
            self.addkey2(next_index,md5string,index)

    def exist(self,key):
        md5string,hash = self.get_md5_hash(key)
        return self.exist_link(hash*40+4, md5string)

    def exist_link(self,index,md5string):
        start = index
        end = index+40
        string = self.m[start:end]
        result = struct.unpack(self.pattern,string)

        if not result[0]=='1':
            return False

        if result[1]== md5string:
            return True

        if result[2] ==0:
            return False

        return self.exist_link(result[2],md5string)

def test():
    kd = KeyData('test')
    print kd.addkey('kwsy')
    print kd.addkey('kwsy')
if __name__ =='__main__':
    test()