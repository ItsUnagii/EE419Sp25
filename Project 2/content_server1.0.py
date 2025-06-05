import socket, sys
import ast
import threading, time
import random

BUFSIZE = 1024  # size of receiving buffer
ALIVE_SGN_INTERVAL = 0.5  # interval to send alive signal
TIMEOUT_INTERVAL = 10*ALIVE_SGN_INTERVAL
UPSTREAM_PORT_NUMBER = 1111 # socket number for UL transmission

##
#
# FOR TRANSMITTING PACKET USE THE FOLLOWING CODE
#
#self.ul_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#try:
#   self.ul_socket.connect((host, backend_port))
#   self.ul_socket.send(("STRING TO SEND").encode())
#   self.ul_socket.close()
#except socket.error:
#   pass
#
#
#
#

class Content_server():
    def __init__(self, conf_file_addr):
        
        # each server has these variables
        # uuid: UUID of the server
        # name: Name of the server
        # peer_count: Number of neighbors
        # peers: List of neighbors
        # map: Map of the server
        # dl_socket: socket for receiving packets
        # timeout: timeout for adjacent nodes
        
        # TODO: load and read configuration file
        config = {}
        with open(conf_file_addr, 'r') as file:
            for line in file:
                key, value = line.strip().split('=')
                config[key.strip()] = value.strip()
        print(config)


        # create the receive socket
        self.dl_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.dl_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.dl_socket.bind(("", int(config['backend_port']))) #YOU NEED TO READ THIS FROM CONFIGURATION FILE
        self.dl_socket.listen(100)

        # TODO: Create all the data structures to store various variables
        self.uuid = config['uuid'] # UUID of the server
        self.name = config['name'] # Name of the server
        self.backend_port = int(config['backend_port'])
        self.peer_count = int(config['peer_count']) # Number of neighbors
        self.peers = {}

        self.hostname = socket.gethostname()

        self.sequence_tracker = {}
        self.timeout_tracker = {}

        self.map = {self.name : {}}
        
        for i in range(self.peer_count):
            peer_info = config['peer_' + str(i)].split(",")
            peer_uuid = peer_info[0].strip()
            peer_hostname = peer_info[1].strip()
            peer_backend_port = int(peer_info[2].strip())
            peer_metric = int(peer_info[3].strip())
            self.addneighbor(peer_uuid, peer_hostname, peer_backend_port, peer_metric)
            
        # for i in range(self.peer_count):
        #     peer_info = config['peer_' + str(i)].split(",")
        #     peer_uuid = peer_info[0].strip()
        #     peer_hostname = peer_info[1].strip()
        #     peer_backend_port = int(peer_info[2].strip())
        #     peer_metric = int(peer_info[3].strip())
            
        #     name = "temp"
        #     while name in self.peers.keys():
        #         name = "temp" + str(random.randint(1, 100))

        #     self.peers[name] = {
        #         'uuid': peer_uuid,
        #         'hostname': peer_hostname,
        #         'backend_port': peer_backend_port,
        #         'metric': peer_metric
        #     }

        #     self.timeout_tracker[peer_uuid] = time.time()
        #     self.link_state_adv()
            


        # TODO: Extract neighbor information and populate the initial variables
        
        # TODO: Update the map

        # TODO: Initialize link state advertisement that repeats using a neighbor variable
        self.link_state_adv()
        self.remain_threads = True
        
        

        print(f"Content server {self.name} started.")

        
        self.alive()
        return
    
    def addneighbor(self, uuid, host, backend_port, metric):
        # TODO: Add neighbor code goes here
        backend_port = int(backend_port)
        metric = int(metric)

        new_peer = {
            "uuid": uuid,
            "hostname": host,
            "backend_port": backend_port,
            "metric": metric
        }
        name = "temp"
        while name in self.peers.keys():
            name = "temp" + str(random.randint(1, 100))
        self.peers[name] = new_peer

        self.timeout_tracker[uuid] = time.time()

        self.sequence_tracker[uuid] = 0

        self.map[self.name].update({name : metric}) # update the map with the new peer

        self.link_state_adv()
    
    def link_state_adv(self):
        #while self.remain_threads:
            # TODO: Perform Link State Advertisement to all your neighbors periodically 
            # format: LSA, name, uuid, backend_port, metric, hostname, sequence_number, map of the sender
            peers_copy = dict(self.peers)
            
            for peer_name, peer_info in peers_copy.items():
                self.sequence_tracker[peer_info['uuid']] += 1
                advertisement = "LSA," + str(self.name) + "," + str(self.uuid) + "," + str(self.backend_port) + "," + str(peer_info['metric']) + "," + str(self.hostname) + "," + str(self.sequence_tracker[peer_info['uuid']]) + "," + str(self.map[self.name])
                ad_message = str(advertisement).encode()
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.connect((peer_info['hostname'], peer_info['backend_port']))
                        s.send(ad_message)
                except Exception as e:
                    print(f"Error sending advertisement to {peer_name}: {e}")

            time.sleep(ALIVE_SGN_INTERVAL)

    
    def link_state_flood(self, msg):
        # TODO: If new information then send to all your neighbors, if old information then drop.
        for peer_name, peer_info in self.peers.items():
            advertisement = "LSAFlood," + str(self.name) + "," + str(self.uuid) + "," + str(peer_info['metric']) + "," + str(self.sequence_tracker[peer_info['uuid']]) + "," + str(self.map[self.name])
            ad_message = str(advertisement).encode()
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((peer_info['hostname'], peer_info['backend_port']))
                    s.send(ad_message)
            except Exception as e:
                print(f"Error sending advertisement to {peer_name}: {e}")
    
    def dead_adv(self):
        # TODO: Advertise death to every neighbor before kill
        for peer_name, peer_info in self.peers.items():
            advertisement = "Death," + str(self.name) + "," + str(self.uuid) + "," + str(self.backend_port) + "," + str(self.hostname)
            ad_message = str(advertisement).encode()
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((peer_info['hostname'], peer_info['backend_port']))
                    s.send(ad_message)
            except Exception as e:
                print(f"Error sending advertisement to {peer_name}: {e}")
    
    def dead_flood(self, sequence_num, peername):
        # TODO: Forward the death message information to other peers
        for peer_name, peer_info in self.peers.items():
            advertisement = "Deathflood," + str(peername)
            ad_message = str(advertisement).encode()
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((peer_info['hostname'], peer_info['backend_port']))
                    s.send(ad_message)
            except Exception as e:
                print(f"Error sending advertisement to {peer_name}: {e}")
        

    def keep_alive(self):
        # TODO: Tell that you are alive to all your neighbors, periodically.
        while self.remain_threads:
            peers_copy = dict(self.peers)

            for peer_uuid, peer_info in peers_copy.items():
                advertisement = "Alive," + str(self.name) + "," + str(self.uuid) + "," + str(self.backend_port) + "," + str(peer_info['metric']) + "," + str(self.hostname)
                ad_message = str(advertisement).encode()
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.connect((peer_info['hostname'], peer_info['backend_port']))
                        s.send(ad_message)
                except Exception as e:
                    print(f"Error sending keepalive to {peer_uuid}: {e}")
                    #pass

            time.sleep(ALIVE_SGN_INTERVAL)
        
    
   
   ## THIS IS THE RECEIVE FUNCTION THAT IS RECEIVING THE PACKETS
    def listen(self):
        self.dl_socket.settimeout(0.1)  # for killing the application
        while self.remain_threads:
            try:
                connection_socket, client_address = self.dl_socket.accept()
                msg = connection_socket.recv(BUFSIZE).decode()
                #print("received", connection_socket, client_address, msg_string)
                #print("Received message from ", client_address, ":", msg)
            except socket.timeout:
                msg = ""
                pass

            if msg == "":    # empty message
                pass
            elif msg.startswith("Alive"): # Update the timeout time if known node, otherwise add new neighbor
                # print("Received Alive message: ", msg)
                #print("Timeout tracker: ", self.timeout_tracker)
                #print("Peers: ", self.peers)
                tokens = msg.split(",")
                # format: Alive, name, uuid, backend_port, metric, hostname
                
                self.timeout_tracker[tokens[2]] = time.time()

                temps = [key for key in self.peers.keys() if key.startswith("temp")]
                if len(temps) > 0:
                    for temp in temps:
                        if tokens[2] == self.peers[temp]['uuid']:
                            # Replace temp key with the actual UUID
                            self.peers[tokens[1]] = self.peers.pop(temp)
                            # self.timeout_tracker[tokens[1]] = self.timeout_tracker.pop(temp)
                            self.map[self.name].update({tokens[1] : int(tokens[4])}) # update the map with the new peer
                            self.map[self.name].pop(temp, None)
                            
                

            elif msg.startswith("LSA"):     # Update the map based on new information, drop if old information
                #If new information, also flood to other neighbors
                print("Received LSA message: ", msg)

                tokens = msg.split(",")
                # format: LSA, name, uuid, backend_port, metric, hostname, sequence_number, map of the sender
                name = tokens[1]
                uuid = tokens[2]
                received_map = tokens[7]

                sequence_number = int(tokens[6])

                if sequence_number > self.sequence_tracker.get(uuid, 0):
                    self.sequence_tracker[uuid] = sequence_number
                    if name not in self.peers.keys(): # new peer
                        self.addneighbor(tokens[2], tokens[5], int(tokens[3]), int(tokens[4]))

                    temps = [key for key in self.peers.keys() if key.startswith("temp")]
                    if len(temps) > 0:
                        for temp in temps:
                            if uuid == self.peers[temp]['uuid']:
                                # Replace temp key with the actual name
                                self.peers[name] = self.peers.pop(temp)
                                # self.timeout_tracker[tokens[1]] = self.timeout_tracker.pop(temp)
                    
                    self.timeout_tracker = {}
                    for peer_name, peer_info in self.peers.items():
                        self.timeout_tracker[peer_info['uuid']] = time.time()

                    # update the map with the new information
                    self.map[self.name].update({name : int(tokens[4])})

                    self.link_state_flood(received_map) # Flood the new information to all neighbors
                
            elif msg.startswith("LSAFlood"): 

                # Flood the new information to all neighbors
                print("Received LSAFlood message: ", msg)
                tokens = msg.split(",")
                # format: LSAFlood, name, uuid, metric, sequence_number, map of the sender
                name = tokens[1]
                uuid = tokens[2]
                received_map = tokens[5]
                sequence_number = int(tokens[4])
                if sequence_number > self.sequence_tracker.get(uuid, 0):
                    self.sequence_tracker[uuid] = sequence_number
                    self.map[self.name].update({name : int(tokens[3])}) # update the map with the new information
                    self.link_state_flood(received_map)

            elif msg.startswith("Death"): # Delete the node if it sends the message before executing kill.
                
                print("Received Death message: ", msg)
                
                tokens = msg.split(",")
                peer_name = tokens[1]
                peer_uuid = tokens[2]
                # format: Death, name, uuid, backend_port, hostname
                print("Removing peer: ", peer_name, peer_uuid)
                self.peers.pop(peer_name, None)
                print("Peers after removing: ", self.peers)
                self.timeout_tracker.pop(peer_uuid, None)

                self.map[self.name].pop(peer_name, None)
                final_seq = self.sequence_tracker.pop(peer_uuid, None)
                self.dead_flood(final_seq + 1, peer_name) 
            elif msg.startswith("Deathflood"): # someone in the network died, flood the information to other peers
                print("Received Deathflood message: ", msg)
                tokens = msg.split(",")
                sequence_number = int(tokens[1])
                peer_name = tokens[2]

                if sequence_number > self.sequence_tracker.get(uuid, 0):
                    self.sequence_tracker[uuid] = sequence_number
                    self.map[self.name].pop(peer_name, None)
                    self.dead_flood(sequence_number, peer_name) 
            
            # otherwise the msg is dropped

    def timeout_old(self):
        # drop the neighbors whose information is old
        while self.remain_threads:
            timeout_copy = dict(self.timeout_tracker)
            peers_copy = dict(self.peers)
            current_time = time.time()

            for peer_name, peer_info in peers_copy.items():
                peer_uuid = peer_info['uuid']
                last_time = timeout_copy.get(peer_uuid, None)
                if last_time is not None:
                    if current_time - last_time > TIMEOUT_INTERVAL:
                        print(f"Peer {peer_uuid} timed out")
                        self.peers.pop(peer_name, None)
                        self.timeout_tracker.pop(peer_uuid, None)
                        self.map[self.name].pop(peer_name, None)
                        self.dead_flood(self.sequence_tracker[peer_uuid] + 1, peer_name)
                    
                    
                    # for peer_name, peer_info in peers_copy.items():
                    #     if peer_info['uuid'] == peer_uuid:
                    #         self.peers.pop(peer_name, None)
                    #         self.timeout_tracker.pop(peer_uuid, None)
                    #         break
                    



            # for peer_name, peer_info in timeout_copy.items():
            #     print("type of self.timeout_tracker:", type(self.timeout_tracker))
            #     if time.time() - self.timeout_tracker[peer_info['uuid']] > TIMEOUT_INTERVAL:
            #         # remove the peer from the list
            #         print(f"Peer {peer_name} timed out")
            #         self.peers.pop(peer_name, None)
            #         self.timeout_tracker.pop(peer_info['uuid'], None)
            
            time.sleep(ALIVE_SGN_INTERVAL)
        

    def shortest_path(self):
        # derive the shortest path according to the current link state
        rank = {}
        return rank

    
    def alive(self):
        keep_alive = threading.Thread(target=self.keep_alive) # A thread that keeps sending keep_alive messages
        listen = threading.Thread(target=self.listen) # A thread that keeps listening to incoming packets
        timeout_old = threading.Thread(target=self.timeout_old) # A thread to eliminate old neighbors
        link_state_adv = threading.Thread(target=self.link_state_adv) # A thread that keeps doing link_state_adv
        keep_alive.start()
        listen.start()
        timeout_old.start()
        link_state_adv.start()
        while self.remain_threads:
            time.sleep(ALIVE_SGN_INTERVAL)  # wait for the network to settle
            command_line = input().split(" ")
            command = command_line[0]
            # print("Received command: ", command)
            if command == "kill":
                # Send death message
                # Kill all threads
                print("Killing all threads")
                self.dead_adv()
                self.remain_threads = False
            elif command == "uuid":
                # Print UUID
                print({"uuid" : self.uuid})
            elif command == "neighbors":
                # Print Neighbor information
                print({"neighbors" : self.peers})
            elif command == "addneighbor":
                # Update Neighbor List with new neighbor
                tokens = " ".join(command_line[1:]).strip().split()

                args = {}
                for token in tokens:
                    if '=' in token:
                        key, value = token.split('=', 1)
                        args[key.strip()] = value.strip()
                
                
                uuid = args['uuid']
                host = args['host']
                backend_port = int(args['backend_port'])
                metric = int(args['metric'])
                
                self.addneighbor(uuid, host, backend_port, metric)
            elif command == "map":
                # Print Map
                print(self.map)
            elif command == "rank": 
                # Compute and print the rank
                print("NOT IMPLEMENTED")
            elif command == "timeout":
                print(self.timeout_tracker)
            else:
                print("Unknown command.")
                print("Usage: kill, uuid, neighbors, addneighbor, map, rank")

if __name__ == "__main__":
    content_sever = Content_server(sys.argv[2])
