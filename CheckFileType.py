#coding=utf-8
import struct

type_dict = {

    'FFD8FF':'jpg','89504E47':'png','47494638':'gif','49492A00':'tif',
    '424D':'bmp','41433130':'dwg','38425053':'psd','7B5C727466':'rtf','3C3F786D6C':'xml',
    '68746D6C3E':'html','44656C69766572792D646174653A':'eml','CFAD12FEC5FD746F':'dbx','2142444E':'pst',
    'D0CF11E0':'doc/xls','5374616E64617264204A':'mdb','FF575043':'wpd','252150532D41646F6265':'ps/eps',
    '255044462D312E':'pdf','AC9EBD8F':'qdf','E3828596':'pwl','504B0304':'zip',
    '52617221':'rar','57415645':'wav','41564920':'avi','2E7261FD':'ram',
    '2E524D46':'rm','000001BA':'mpg','000001B3':'mpg','6D6F6F76':'mov','3026B2758E66CF11':'asf','4D546864':'mid'
}

#转成16进制字符串
def bytes2hex(bytes):
    num = len(bytes)
    hexstr = u""
    for i in range(num):
        t = u"%x" % bytes[i]
        if len(t) % 2:
            hexstr += u"0"
        hexstr += t
    return hexstr.upper()

#获得类型
def get_filetype(filename):
    file = open(filename,'rb')
    ftype = 'unknown'

    for k,v in type_dict.items():
        num_bytes = len(k)/2
        file.seek(0)
        hbytes = struct.unpack('B'*num_bytes,file.read(num_bytes))
        code = bytes2hex(hbytes)
        if code == k:
            ftype =  v
            break

    file.close()
    return ftype

if __name__ == '__main__':
    print get_filetype('321.jpg')

