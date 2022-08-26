import hashlib
import socket
import os

# 通过SHA-1将地址转换为id
def add2id(address):
    return int(hashlib.sha1(address.encode("utf-8")).hexdigest(), 16) % (10 ** 8)

def file2id(file_path, block_size=64 * 1024):
    file_name = file_path.split('/')[-1]
    hash = hashlib.new("sha1", b"")
    hash.update(file_name.encode())
    # with open(file_path, "rb") as file:
    #   data = file.read(block_size)
    #   hash.update(data)
    return int(hash.hexdigest(), 16) % (10 ** 8)

def file_name2id(file_name):
    hash = hashlib.new("sha1", b"")
    hash.update(file_name.encode())

    return int(hash.hexdigest(), 16) % (10 ** 8)

def send_msg(targe_ip, targe_port, msg):
    #print(msg)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(msg.encode('utf-8'),(targe_ip, targe_port))

def compar(a, b):
  if ((a % 99999999) > (b % 99999999)):
    return 1
  else:
    return 0

def save(file_path, file, dir = './'):
  if not os.path.exists(dir):
    os.makedirs(dir)

  f = open(dir+file_path, mode="wb")
  f.write(file.encode("utf-8"))
  f.close()

def get_file_name(file_path):
    return file_path.split('\\')[-1]

def read_file(file_path):
    file_name = get_file_name(file_path)
    f = open(file_path, mode="rb")
    content = f.read()
    f.close()
    return  file_name, content

if __name__ == '__main__':
    print(compar(9946372, 999999990))