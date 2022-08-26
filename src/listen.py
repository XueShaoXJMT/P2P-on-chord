import socketserver
import json
from utils import *
from Node import *

class Listen_Server(socketserver.UDPServer):
    def __init__(self, server_address, RequestHandlerClass, node):
        self.node = node
        socketserver.UDPServer.__init__(self, server_address, RequestHandlerClass)

# Handle recived messages
class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        data = self.request[0].decode("utf-8")
        socket = self.request[1]

        data_array = data.split("：")

        if (data_array[1] != ''):
            try:
                info = json.loads(data_array[1])
            except:
                pass

        if data_array[0] == "join":
            target = (info[1],info[2])
            id = info[0]
            # There are at least two nodes in the current network
            if (self.server.node.succ[0] != self.server.node.id):
                # The target id is greater than self id
                if(id > self.server.node.id):
                    # The target id is between itself and its successor node
                    # or self is the node with the largest id
                    if (id < self.server.node.succ[0] or self.server.node.check_max()):
                        reply = "you_next：" + json.dumps(self.server.node.succ)
                        socket.sendto(reply.encode('utf-8'), target)
                        reply = "you_pred：" + json.dumps(self.server.node.info)
                        socket.sendto(reply.encode('utf-8'), target)
                        reply = "you_pred：" + json.dumps(info)
                        socket.sendto(reply.encode('utf-8'), (self.server.node.succ[1], self.server.node.succ[2]))
                        self.server.node.succ = info[:3]
                    # The target id is greater than its successor node
                    # Use find_successor() to find the closest node to the target id in the finger table
                    else:
                        nearest = self.server.node.find_successor(id)
                        reply = "join：" + json.dumps(info)
                        socket.sendto(reply.encode('utf-8'), (nearest[1], nearest[2]))
                # The target id is smaller than self id
                else:
                    # The target id is between itself and its predecessor node
                    # or self is the node with the smallest id
                    if(id > self.server.node.pred[0] or self.server.node.check_min()):
                        reply = "you_next：" + json.dumps(self.server.node.info)
                        socket.sendto(reply.encode('utf-8'), target)
                        reply = "you_pred：" + json.dumps(self.server.node.pred)
                        socket.sendto(reply.encode('utf-8'), target)
                        reply = "you_next：" + json.dumps(info)
                        socket.sendto(reply.encode('utf-8'), (self.server.node.pred[1], self.server.node.pred[2]))
                        self.server.node.pred = info[:3]
                    else:
                        reply = "join：" + json.dumps(info)
                        socket.sendto(reply.encode('utf-8'), (self.server.node.pred[1], self.server.node.pred[2]))

            # There is only one node in the current network
            # Set the new node as both the successor and predecessor of current node
            else:
                self.server.node.succ = info[:3]
                self.server.node.pred = info[:3]
                self.server.node.update_finger()
                self.server.node.next_alive = True
                self.server.node.pred_alive = True
                self.server.node.update_finger()
                reply = "you_next：" + json.dumps(self.server.node.info)
                socket.sendto(reply.encode('utf-8'), target)
                reply = "you_pred：" + json.dumps(self.server.node.info)
                socket.sendto(reply.encode('utf-8'), target)
                socket.sendto("update_finger：".encode('utf-8'), target)

        if data_array[0] == "update_finger" :
            self.server.node.update_finger()

        # Modify the message related to the finger list
        if data_array[0] == "find_successor" :
            if (compar(self.server.node.succ[0], info[1]) or self.server.node.check_max()):
                target = (info[2], info[3])
                table = self.server.node.succ[:]
                table.insert(0, info[0])
                reply = "you_finger：" + json.dumps(table)
                socket.sendto(reply.encode('utf-8'), target)
            else:
                table = self.server.node.find_successor(info[1])[:]
                target = (table[1], table[2])
                reply = "find_successor：" + json.dumps(info)
                socket.sendto(reply.encode('utf-8'), target)

        # Operate on the node's predecessor and successor nodes
        if data_array[0] == "you_next":
            self.server.node.succ = info[:3]
        if data_array[0] == "you_pred":
            self.server.node.pred = info[:3]

        # Upladte finger table
        if data_array[0] == "you_finger":
            self.server.node.finger[info[0]] = [info[1], info[2], info[3]]

        # Stability related message.
        if data_array[0] == "is_me":
            # The message is sent by the predecessor
            if (self.server.node.pred[0] == info[0]):
                target = (info[1], info[2])
                socket.sendto("update_finger：".encode('utf-8'), target)
            # the message is sent by former node
            elif(info[0]<self.server.node.pred[0]):
                # If the predecessor is alive, send message to it, showing its successor and predecessor are alive.
                if (self.server.node.pred_alive):
                    target = (info[1], info[2])
                    reply = "you_next：" + json.dumps(self.server.node.pred)
                    socket.sendto(reply.encode('utf-8'), target)
                    target = (self.server.node.pred[1], self.server.node.pred[2])
                    reply = "you_pred：" + json.dumps(info)
                    socket.sendto(reply.encode('utf-8'), target)
                # The predecessor is dead, use the target node as current node's new predecessor
                else:
                    self.server.node.pred = info
            # The message is sent by successor
            else:
                if (self.server.node.pred_alive):
                    target = (info[1], info[2])
                    reply = "you_pred：" + json.dumps(self.server.node.pred)
                    socket.sendto(reply.encode('utf-8'), target)
                    target = (self.server.node.pred[1], self.server.node.pred[2])
                    reply = "you_next：" + json.dumps(info)
                    socket.sendto(reply.encode('utf-8'), target)
                    self.server.node.pred = info[:2]
                else:
                    self.server.node.pred = info[:2]
            '''
            else:
                if (self.server.node.next_alive):
                    target = (info[1], info[2])
                    reply = "you_pred：" + json.dumps(self.server.node.pred)
                    socket.sendto(reply.encode('utf-8'), target)
                    target = (self.server.node.pred[1], self.server.node.pred[2])
                    reply = "you_next：" + json.dumps(info)
                    socket.sendto(reply.encode('utf-8'), target)
                    self.server.node.succ = info
                else:
                    self.server.node.succ = info
            '''

        if data_array[0] == "download":
            print("--get file " + info[0] + "--")
            file_name = info[0]
            file = info[1]
            save(file_name, file, self.server.node.dir)
            self.server.node.file_list[info[2]] = info[0]

        if data_array[0] == "download+":
            file_name = info[0]
            file = info[1]
            dir = info[-1]
            if not os.path.exists(dir):
                os.makedirs(dir)
            save(file_name, file, dir)
            print("--File " + file_name + " downloaded in " + dir + "--")

        # The file should be saved in this node
        if data_array[0] == "is_successor":
            #print(1)
            #print(data_array[2])
            file_name,content = read_file(data_array[2])
            #print(file_name)
            file = [file_name, content.decode("utf-8"), file_name2id(get_file_name(data_array[2]))]
            #save(file[0], file[1], self.server.node.dir)
            #self.server.node.file_list[info[2]] = file_name
            #print("--get file " + 'proposal.docx' + "--")
            reply = "download：" + json.dumps(file)
            socket.sendto(reply.encode('utf-8'), (info[1],info[2]))
            #socket.sendto(reply.encode('utf-8'), ('127.0.0.1', 20))
        '''
        # Check whether the file is on the node
        if data_array[0] == "in_successor":
            target = (info[1], info[2])
            reply = "find：" + json.dumps([str(x) for x in info])
            socket.sendto(reply.encode('utf-8'), target)
        '''
        if data_array[0] == "find":
            #print(self.server.node.info)
            #print(1)
            is_find = self.server.node.check_file(int(info[-1]))
            if is_find is not None:
                print("--File is found by node " + str(info[-4]) + "--")
                reply = "found：" + json.dumps(info)
                socket.sendto(reply.encode('utf-8'), (info[-3], info[-2]))
                #socket.sendto(reply.encode('utf-8'), ('127.0.0.1', 20))
            else:
                reply = " not found：" + json.dumps(info)
                #socket.sendto(reply.encode('utf-8'), (info[-3], info[-2]))
                socket.sendto(reply.encode('utf-8'), (info[-3], info[-2]))

        #if data_array[0] == "found":
            #print("--File is found by node " + str(info[-4]) + "--")
            #print("--File is found on node " + info[0] + "--")

        if data_array[0] == "not found":
            print("--File is not found--")

        if data_array[0] == "find+":
            #print(self.server.node.info)
            #print(1)
            is_find = self.server.node.check_file(int(info[-1]))
            if is_find is not None:
                print("--File is found by node " + str(info[-4]) + "--")
                file_path = is_find
                file_name, content = read_file(file_path)
                file_info = [file_name, content.decode("utf-8"), info[0], info[-5]]
                #print(info[-5:])
                reply = "download+：" + json.dumps(file_info)
                socket.sendto(reply.encode('utf-8'), (info[-3], info[-2]))
                #socket.sendto(reply.encode('utf-8'), ('127.0.0.1', 10))
            else:
                print("--File is not found--")
        '''
        if data_array[0] == "get_successor":
            table = self.server.node.find_successor(info[0])[:]
            if (table[0] is not self.server.node.id):
                target = (table[1], table[2])
                reply = "get_successor：" + json.dumps(info)+"："+data_array[2]
                socket.sendto(reply.encode('utf-8'), target)
            else:
                #print("--file saved in " + str(self.server.node.succ[0]) + "--")
                #target = (info[1], info[2])
                new_list = self.server.node.succ
                new_list.append(info[-3])
                new_list.append(info[-2])
                new_list.append(info[-1])
                reply = "is_successor：" + json.dumps(new_list) + "：" + data_array[2]
                #socket.sendto(reply.encode('utf-8'), (target))
                socket.sendto(reply.encode('utf-8'), (self.server.node.succ[1], self.server.node.succ[2]))

        if data_array[0] == "serch_successor":
            print(info[0])
            table = self.server.node.find_successor(info[0])[:]
            print(table[0] == self.server.node.id)
            if (table[0] != self.server.node.id):
                target = (table[1], table[2])
                #reply = "serch_successor：" + json.dumps(info)
                reply = "serch_successor：" + json.dumps(info)
                print(reply)
                socket.sendto(reply.encode('utf-8'), target)
            else:
                #reply = "in_successor：" + json.dumps(info)
                new_list = self.server.node.succ
                new_list.append(info[-3])
                new_list.append(info[-2])
                new_list.append(info[-1])
                reply = "find：" + json.dumps(new_list)
                socket.sendto(reply.encode('utf-8'), (self.server.node.succ[1], self.server.node.succ[2]))
        '''
        if data_array[0] == "you_pred_alive":
            if(self.server.node.id != info[0]):
                self.server.node.pred_alive = True
                self.server.node.pred_alive_count += 1

        if data_array[0] == "you_next_alive":
            if (self.server.node.id != info[0]):
                self.server.node.next_alive = True
                self.server.node.next_alive_count += 1

        if data_array[0] == "you_pred_dead":
            self.server.node.pred_alive = False

        if data_array[0] == "you_next_dead":
            self.server.node.next_alive = False
