import socket, sys
import json
import time
import threading

import struct # NEW: for packing and unpacking binary data

import random  # NEW: for simulating packet loss

BUFSIZE = 10202  # size of receiving buffer
PKTSIZE = 10200  # number of bytes in a packet
WINDOW_SIZE = 16
IDX_LENGTH = 2 # 2 bytes of packet index
TIMEOUT = 0.5   # timeout time

class Server():
    def __init__(self, config_file):
        #Read the config file and initialize the port, peer_num, peer_info, content_info from the config file

        with open(config_file, 'r') as f:
            config = json.load(f)

        self.hostname = config['hostname']
        self.port = config['port']
        self.peer_num = config['peers']
        self.content_info = config['content_info']
        self.peer_info = config['peer_info']

        print("Server hostname: ", self.hostname)
        print("Server port: ", self.port)
        print("Peer number: ", self.peer_num)
        print("Content info: ", self.content_info)
        print("Peer info: ", self.peer_info)

        # establish a socket according to the information
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) #NOTE THAT THE SOCK_DGRAM will ensure your socket is UDP
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(("", self.port)) #This is the only port you can use to receive
        
        self.server_socket.settimeout(1)   # timeout value
        print(f"Server started at {self.hostname}:{self.port}")
        
        self.remain_threads = True
        # Lock for thread-safe operations
        self.socket_lock = threading.Lock()
        # Active transmissions tracking
        self.active_transmissions = {}
        # message routing
        self.pending_responses = {}  # addr -> {message_type: queue}
        self.response_lock = threading.Lock()
        
        self.cli()
        return
    
    def find_file(self, file_name):
        #A function to find the peer with the file you want!
        for peer in self.peer_info:
            print(f"Searching peer {peer['hostname']}:{peer['port']}")
            if file_name in peer['content_info']:
                print(f"Found file {file_name} on peer {peer}")
                return peer
        return None

    def wait_for_response(self, expected_addr, message_type="default", timeout=2.0):
        # wait for response from specific address and message type
        start_time = time.time()
        expected_normalized = self.normalize_address(expected_addr)
        
        while time.time() - start_time < timeout:
            with self.response_lock:
                # Check both normalized and original addresses
                for addr_key in self.pending_responses:
                    if (self.normalize_address(addr_key) == expected_normalized and 
                        message_type in self.pending_responses[addr_key] and 
                        self.pending_responses[addr_key][message_type]):
                        return self.pending_responses[addr_key][message_type].pop(0)
            #time.sleep(0.01)
        
        raise socket.timeout(f"No response from {expected_addr} for type {message_type}")
        

    def queue_message(self, data, addr, message_type="default"):
        # Queue message for a specific address and type
        with self.response_lock:
            if addr not in self.pending_responses:
                self.pending_responses[addr] = {}
            if message_type not in self.pending_responses[addr]:
                self.pending_responses[addr][message_type] = []
            self.pending_responses[addr][message_type].append((data, addr))

    def route_message(self, data, addr):
        print(f"Routing message {len(data)} bytes from {addr}")
                     
        if data.startswith(b"REQ:"):
            # File request
            file_name = data[4:].decode().strip()
            print("Received request for file: {} from {}".format(file_name, addr))
            
            # Start transmission in separate thread
            tx_thread = threading.Thread(target=self.transmit, args=(file_name, addr))
            tx_thread.daemon = True
            tx_thread.start()
            
        elif data.startswith(b"SYNACK:"):
            # Queue for handshake
            self.queue_message(data, addr, "handshake")
            
        elif data == b"ACK":
            # Queue for handshake ACK
            self.queue_message(data, addr, "handshake")
            
        elif data.startswith(b"ACK:"):
            # Queue for file transfer ACK
            self.queue_message(data, addr, "file_ack")
            
        elif data.startswith(b"RETX:"):
            # Queue for retransmission request
            self.queue_message(data, addr, "retx")
            
        elif data.startswith(b"ERROR:"):
            # Queue for error messages
            self.queue_message(data, addr, "error")
            
        elif len(data) >= IDX_LENGTH:
            # Potential file packet
            try:
                packet_idx = struct.unpack('!H', data[:IDX_LENGTH])[0]
                # Queue for file reception
                self.queue_message(data, addr, "file_data")
            except:
                # Not a file packet, queue as default
                self.queue_message(data, addr, "default")
        else:
            # Other messages - queue as default
            self.queue_message(data, addr, "default")

    def normalize_address(self, addr):
        # convert all mentions of localhost to actual ip since if you don't do this the handshakes will not go through because the program doesn't know the difference
        # Kind of annoying that this was happening
        host, port = addr
        if host == 'localhost':
            host = '127.0.0.1'
        return (host, port)

    def addresses_match(self, addr1, addr2):
        # check if two addresses go to the same endpoint
        return self.normalize_address(addr1) == self.normalize_address(addr2)

    def load_file(self, file_name):
        # find which server has the file
        peer = self.find_file(file_name)
        if not peer:
            print(f"File {file_name} not found on any peer")
            return
        
        peer_addr = (peer['hostname'], peer['port'])
        # normalize the peer address for consistent matching
        normalized_peer_addr = self.normalize_address(peer_addr)
        print(f"Requesting file {file_name} from {peer_addr} (normalized: {normalized_peer_addr})")
        
        # establish a client socket for downloading file

        # use a connect flag to determine if the file name is sent correctly
        #Initiate three-way handshake and use a connect flag
        connect_flag = False
        handshake_attempts = 0
        max_attempts = 3
        
        while not connect_flag and handshake_attempts < max_attempts:
            try:
                # Send file request (SYN)
                print(f"Sending REQ:{file_name} to {peer_addr}")
                req_message = ("REQ:" + file_name).encode()
                
                with self.socket_lock:
                    bytes_sent = self.server_socket.sendto(req_message, peer_addr)
                    print(f"Sent {bytes_sent} REQ bytes to {peer_addr[0]}:{peer_addr[1]}")
                
                # Wait for SYN-ACK using message routing with correct type
                response, addr = self.wait_for_response(normalized_peer_addr, "handshake", timeout=3.0)
                
                print(f"Received handshake response: {response} from {addr}")
                
                if self.addresses_match(addr, peer_addr):
                    if response.startswith(b"SYNACK:"):
                        packet_num = int(response.decode().split(":")[1])
                        print(f"Received packet count: {packet_num}")
                        
                        # Send ACK
                        print(f"Sending ACK to {normalized_peer_addr}")
                        with self.socket_lock:
                            self.server_socket.sendto(b"ACK", normalized_peer_addr)
                        
                        connect_flag = True
                    elif response.startswith(b"ERROR:"):
                        error_msg = response.decode().split(":", 1)[1]
                        print(f"Server error: {error_msg}")
                        return
                    else:
                        handshake_attempts += 1
                        print(f"Unexpected response: {response}")
                else:
                    print(f"Response from wrong address: {addr} (expected {normalized_peer_addr})")
                    handshake_attempts += 1
                    
            except socket.timeout:
                handshake_attempts += 1
                print(f"Handshake attempt {handshake_attempts} failed (timeout)")
            except Exception as e:
                handshake_attempts += 1
                print(f"Handshake attempt {handshake_attempts} failed: {e}")
        
        if not connect_flag:
            print("Failed to establish connection")
            return
        
        # the receiver keeps a record for which part has been acked
        received_packets = {}
        expected_packets = packet_num
        received_count = 0
        
        # start receiving file
        print("Starting file reception...")
        
        while received_count < expected_packets:
            try:
                # Wait for file data packets
                data, addr = self.wait_for_response(normalized_peer_addr, "file_data", timeout=1.0)
                
                if self.addresses_match(addr, peer_addr) and len(data) >= IDX_LENGTH:
                    # Extract packet index
                    packet_idx = struct.unpack('!H', data[:IDX_LENGTH])[0]
                    packet_data = data[IDX_LENGTH:]
                    
                    if packet_idx not in received_packets:
                        received_packets[packet_idx] = packet_data
                        received_count += 1
                        print(f"Received packet {received_count}/{expected_packets}")
                    
                    # Send ACK for this packet
                    ack_msg = struct.pack('!H', packet_idx)
                    with self.socket_lock:
                        self.server_socket.sendto(b"ACK:" + ack_msg, normalized_peer_addr)
                        
            except socket.timeout:
                # request missing packets
                missing = []
                for i in range(expected_packets):
                    if i not in received_packets:
                        missing.append(i)
                
                if missing and len(missing) <= 10:  # don't spam if too many missing
                    for miss_idx in missing[:5]:  # request first 5 missing
                        retransmit_req = struct.pack('!H', miss_idx)
                        with self.socket_lock:
                            self.server_socket.sendto(b"RETX:" + retransmit_req, normalized_peer_addr)
        
        # transmission complete, close socket

        # Reconstruct file
        file_data = b""
        for i in range(expected_packets):
            if i in received_packets:
                file_data += received_packets[i]
        
        # write the file
        with open(file_name, 'wb') as f:
            f.write(file_data)
        print(f"\nFile {file_name} downloaded successfully")

    def read_file(self, file_name):
        #You can write a function that takes the file to be transmitted and converts into chunks of packet_size
        with open(file_name, 'rb') as f:
            file_data = f.read()
           
        transmit_file = []
        for i in range(0, len(file_data), PKTSIZE):
            chunk = file_data[i:i+PKTSIZE]
            transmit_file.append(chunk)
            
        return transmit_file

    def transmit(self, file_name, addr):
        # create a udp socket for transmission
        
        print(f"Starting transmission of {file_name} to {addr}")
        
        # divide the file into several parts
        transmit_file = self.read_file(file_name)
        
        # use socket to send packet number to the receiver
        packet_num = len(transmit_file)
        print(f"File has {packet_num} packets, sending SYNACK to {addr}")
        
        with self.socket_lock:
            self.server_socket.sendto(("SYNACK:" + str(packet_num)).encode(), addr)
        
        # wait for ACK with correct message type
        ack_received = False
        client_normalized_addr = self.normalize_address(addr)
        
        for attempt in range(3):
            try:
                response, client_addr = self.wait_for_response(client_normalized_addr, "handshake", timeout=2.0)
                print(f"Received handshake response: {response} from {client_addr}")
                
                if response == b"ACK" and self.addresses_match(client_addr, addr):
                    ack_received = True
                    print("ACK received from client")
                    break
                    
            except socket.timeout:
                print(f"Waiting for ACK, attempt {attempt + 1}")
                # Resend SYNACK
                with self.socket_lock:
                    self.server_socket.sendto(("SYNACK:" + str(packet_num)).encode(), addr)
        
        if not ack_received:
            print("Client ACK timeout after retries")
            return
        
        # use a time-out array to record which file is time-out and need to be transmitted again
        # -1 indicates received, 0 indicates not transmitted, positive numbers means the time of transmission
        acked = [False] * packet_num
        last_sent = [-1] * packet_num  # Timestamp of last transmission
        
        # Create transmission state
        tx_state = {
            'active': True,
            'window_start': 0,
            'acked': acked,
            'last_sent': last_sent,
            'transmit_file': transmit_file,
            'addr': addr,
            'packet_num': packet_num
        }
        
        self.active_transmissions[addr] = tx_state
        
        def transmit_thread():
            #Takes the transmit window and transmits every packet that is allowed
            while tx_state['active'] and tx_state['window_start'] < packet_num:
                current_time = time.time()
                
                # Send packets in window
                for i in range(tx_state['window_start'], 
                             min(tx_state['window_start'] + WINDOW_SIZE, packet_num)):
                    
                    if (not tx_state['acked'][i] and 
                        (tx_state['last_sent'][i] == -1 or 
                         current_time - tx_state['last_sent'][i] > TIMEOUT)):
                        
                        # Prepare packet with index
                        packet_data = struct.pack('!H', i) + tx_state['transmit_file'][i]
                        
                        with self.socket_lock:
                            self.server_socket.sendto(packet_data, addr)
                        
                        tx_state['last_sent'][i] = current_time
                        print(f"Sent packet {i} to {addr}")
                
                # time.sleep(0.01)  # Small delay to prevent overwhelming
            
            print(f"Transmission to {addr} completed")
        
        def ack_thread():
            #Receives acknowledgement and updates the transmit window with sendable packets
            while tx_state['active'] and tx_state['window_start'] < packet_num:
                try:
                    # Use message routing for ACKs with correct types
                    try:
                        data, client_addr = self.wait_for_response(client_normalized_addr, "file_ack", timeout=0.05)
                    except socket.timeout:
                        # Also check for retransmission requests
                        try:
                            data, client_addr = self.wait_for_response(client_normalized_addr, "retx", timeout=0.05)
                        except socket.timeout:
                            continue
                    
                    if self.addresses_match(client_addr, addr):
                        if data.startswith(b"ACK:") and len(data) >= 4:
                            # Regular ACK
                            ack_idx = struct.unpack('!H', data[4:6])[0]
                            if 0 <= ack_idx < packet_num:
                                tx_state['acked'][ack_idx] = True
                                
                                # Advance window
                                while (tx_state['window_start'] < packet_num and 
                                       tx_state['acked'][tx_state['window_start']]):
                                    tx_state['window_start'] += 1
                        
                        elif data.startswith(b"RETX:") and len(data) >= 6:
                            # Retransmission request
                            retx_idx = struct.unpack('!H', data[5:7])[0]
                            if 0 <= retx_idx < packet_num:
                                tx_state['last_sent'][retx_idx] = -1  # Force retransmission
                                
                except Exception as e:
                    print("ACK thread error: {}".format(e))
            
            tx_state['active'] = False
        
        #Create TX and RX threads and start doing it
        tx_thread = threading.Thread(target=transmit_thread)
        ack_thread_obj = threading.Thread(target=ack_thread)
        
        tx_thread.start()
        ack_thread_obj.start()
        
        #When done transmitting, close the threads.
        tx_thread.join()
        ack_thread_obj.join()
        
        if addr in self.active_transmissions:
            del self.active_transmissions[addr]

    def listener(self): # listen to the socket to see if there's any transmission request
        print("Listener thread started on {}:{}".format(self.hostname, self.port))
        print("Listening for incoming packets...")
        
        while self.remain_threads:
            try:
                data, addr = self.server_socket.recvfrom(BUFSIZE)
                if random.random() < 0.1: # 10% probability to drop the packet as simulation
                    print(f"DROPPED: Packet dropped from {addr}!")
                    pass
                else:
                    print(f"Listener received {len(data)} bytes from {addr}")
                    
                    # route messages and determine type of message
                    self.route_message(data, addr)
                
            except socket.timeout:
                # No data received, keep listening
                pass
            except Exception as e:
                if self.remain_threads:
                    print("Listener error: {}".format(e))
        
        print("Listener thread stopping")
        return
    
    def cli(self): 
        listen_thread = threading.Thread(target=self.listener)
        listen_thread.daemon = True
        listen_thread.start()

        print()
        
        while self.remain_threads:
            try:
                command_line = input()
                if command_line == "kill":  # for debugging purpose
                    #print("Killing all threads")
                    self.remain_threads = False
                    break
                else:
                    file_name = command_line.strip()
                    if file_name == "":
                        continue
                    # find the file and transmit it
                    self.load_file(file_name)
            except KeyboardInterrupt:
                print("\nShutting down...")
                self.remain_threads = False
                break
        
        # Cleanup
        self.server_socket.close()
        return


if __name__ == "__main__":
    server = Server(sys.argv[1])