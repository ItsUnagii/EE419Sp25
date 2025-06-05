import socket, sys
import datetime
import os

import urllib.parse
import mimetypes

BUFSIZE = 1024
LARGEST_CONTENT_SIZE = 5242880

class Vod_Server():
    def __init__(self, port_id):
        # create an HTTP port to listen to
        self.http_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.http_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.http_socket.bind(("", port_id))
        self.http_socket.listen(10000)
        self.remain_threads = True
        self.content_root = os.path.abspath("content")

        # load all contents in the buffer
        self.content_list = self.load_contents("content")
        
        print(f"Server started on port {port_id}")
        print(f"Content root: {self.content_root}")
        print(f"Loaded {len(self.content_list)} files")

        # listen to the http socket
        self.listen()
        # pass

    def load_contents(self, dir):
        #Create a list of files and stuff that you have
        content_list = {}

        if not os.path.exists(dir):
            os.makedirs(dir)
            print(f"Directory '{dir}' created.")
            return content_list

        for root, dirs, files in os.walk(dir):
            for file in files:
                file_path = os.path.join(root, file)
                # relative path from content root
                relative_path = os.path.relpath(file_path, dir)
                # convert url
                url_path = "/" + relative_path.replace(os.path.sep, "/")

                try:
                    file_stat = os.stat(file_path)
                    content_list[url_path] = {
                        "path": file_path,
                        "size": file_stat.st_size,
                        "modified": datetime.datetime.fromtimestamp(file_stat.st_mtime),
                        'type': mimetypes.guess_type(file_path)[0] or 'application/octet-stream'
                    }
                except OSError as e:
                    print(f"Error accessing file '{file_path}': {e}")
                    continue
        return content_list

    def listen(self):
        while self.remain_threads:
            try:
                connection_socket, client_address = self.http_socket.accept()
                print(f"Connection from {client_address}")

                msg_data = b''
                while True:
                    chunk = connection_socket.recv(BUFSIZE)
                    msg_data += chunk
                    if len(chunk) < BUFSIZE or b"\r\n\r\n" in msg_data:
                        break

                msg_string = msg_data.decode()
                if msg_string.strip():
                    self.response(msg_string, connection_socket)

                # chunk = connection_socket.recv(BUFSIZE).decode()
                connection_socket.close()

            except KeyboardInterrupt:
                print("Server shutting down...")
                self.remain_threads = False
                connection_socket.close()
                break
            except Exception as e:
                print(f"Error in listen: {e}")
                try:
                    connection_socket.close()
                except:
                    pass
            
            #Do stuff here
        self.http_socket.close()
            
    def response(self, msg_string, connection_socket):
        """Process HTTP request and generate appropriate response"""
        http_version = 'HTTP/1.1'  # Default version
        try:
            lines = msg_string.split('\r\n')
            if not lines or not lines[0].strip():
                print("Empty or invalid request")
                return
                
            # Parse request line
            request_line = lines[0].strip().split()
            if len(request_line) < 2:
                print(f"Invalid request line: {lines[0]}")
                self.generate_response_404(http_version, connection_socket)
                return
            
            method = request_line[0]
            uri = request_line[1]
            # HTTP version is optional in some cases
            if len(request_line) >= 3:
                http_version = request_line[2]
            
            print(f"Request: {method} {uri} {http_version}")
            
            # Only support GET method
            if method.upper() != 'GET':
                self.generate_response_404(http_version, connection_socket)
                return
            
            # Parse URI and remove query parameters
            parsed_uri = urllib.parse.urlparse(uri)
            path = parsed_uri.path
            
            # Handle root path
            if path == '/' or path == '':
                path = '/index.html'
            
            # Parse headers for Range requests
            headers = self.eval_commands(lines)
            
            # Check if file exists
            if path not in self.content_list:
                print(f"File not found: {path}")
                print(f"Available files: {list(self.content_list.keys())}")
                print(f"{self.content_list}")
                self.generate_response_404(http_version, connection_socket)
                return
            
            if '/confidential/' in path or path.startswith('.confidential/'):
                print(f"Access denied to confidential file: {path}")
                self.generate_response_403(http_version, connection_socket)
                return

            file_info = self.content_list[path]
            
            # Check file size limit
            if file_info['size'] > LARGEST_CONTENT_SIZE:
                print(f"File too large: {file_info['size']} bytes")
                self.generate_response_403(http_version, connection_socket)
                return
            
            # Handle Range requests (partial content)
            if 'Range' in headers:
                self.generate_response_206(http_version, path, file_info['type'], headers, connection_socket)
            else:
                self.generate_response_200(http_version, path, file_info['type'], connection_socket)
                
        except IndexError as e:
            print(f"Index error parsing request: {e}")
            print(f"Request string: {repr(msg_string)}")
            self.generate_response_404(http_version, connection_socket)
        except Exception as e:
            print(f"Error processing request: {e}")
            print(f"Request string: {repr(msg_string)}")
            self.generate_response_404(http_version, connection_socket)

    
    def generate_response_404(self, http_version, connection_socket):
        #Generate Response and Send
        
        response_body = """<!DOCTYPE html>
<html>
<head><title>404 Not Found</title></head>
<body><h1>404 Not Found</h1><p>The requested resource was not found on this server.</p></body>
</html>"""
        
        response_headers = f"""{http_version} 404 Not Found\r
Date: {datetime.datetime.now(datetime.UTC).strftime('%a, %d %b %Y %H:%M:%S GMT')}\r
Server: Simple-File-Server/1.0\r
Content-Type: text/html\r
Content-Length: {len(response_body)}\r
Connection: close\r
\r
"""

        response = response_headers + response_body
        connection_socket.send(response.encode())
        
        # return response

    def generate_response_403(self, http_version, connection_socket):
        #Generate Response and Send

        response_body = """<!DOCTYPE html>
<html>
<head><title>403 Forbidden</title></head>
<body><h1>403 Forbidden</h1><p>Access to this resource is forbidden.</p></body>
</html>"""
        
        response_headers = f"""{http_version} 403 Forbidden\r
Date: {datetime.datetime.now(datetime.UTC).strftime('%a, %d %b %Y %H:%M:%S GMT')}\r
Server: Simple-File-Server/1.0\r
Content-Type: text/html\r
Content-Length: {len(response_body)}\r
Connection: close\r
\r
"""
        
        response = response_headers + response_body
        connection_socket.send(response.encode())
        
        # return response
    
    def generate_response_200(self, http_version, file_idx, file_type, connection_socket):
        #Generate Response and Send
        
        try:
            file_info = self.content_list[file_idx]
            
            with open(file_info['path'], 'rb') as f:
                file_content = f.read()
            
            response_headers = f"""{http_version} 200 OK\r
Date: {datetime.datetime.now(datetime.UTC).strftime('%a, %d %b %Y %H:%M:%S GMT')}\r
Server: Simple-File-Server/1.0\r
Content-Type: {file_type}\r
Content-Length: {len(file_content)}\r
Last-Modified: {file_info['modified'].strftime('%a, %d %b %Y %H:%M:%S GMT')}\r
Accept-Ranges: bytes\r
Connection: close\r
\r
"""
            
            # Send headers
            connection_socket.send(response_headers.encode())
            # Send file content
            connection_socket.send(file_content)
            
        except Exception as e:
            print(f"Error serving file: {e}")
            self.generate_response_404(http_version, connection_socket)

        # return response

    def generate_response_206(self, http_version, file_idx, file_type, command_parameters, connection_socket):
        #Generate Response and Send
        
        try:
            file_info = self.content_list[file_idx]
            file_size = file_info['size']
            
            # Parse Range header
            range_header = command_parameters.get('Range', '')
            if not range_header.startswith('bytes='):
                self.generate_response_200(http_version, file_idx, file_type, connection_socket)
                return
            
            # Parse range (simple implementation for single range)
            range_spec = range_header[6:]  # Remove 'bytes='
            if '-' not in range_spec:
                self.generate_response_200(http_version, file_idx, file_type, connection_socket)
                return
            
            start_str, end_str = range_spec.split('-', 1)
            
            start = int(start_str) if start_str else 0
            end = int(end_str) if end_str else file_size - 1
            
            # Validate range
            if start >= file_size or end >= file_size or start > end:
                # Send 416 Range Not Satisfiable
                response_headers = f"""{http_version} 416 Range Not Satisfiable\r
Date: {datetime.datetime.now(datetime.UTC).strftime('%a, %d %b %Y %H:%M:%S GMT')}\r
Server: Simple-File-Server/1.0\r
Content-Range: bytes */{file_size}\r
Connection: close\r
\r
"""
                connection_socket.send(response_headers.encode())
                return
            
            # Read partial content
            with open(file_info['path'], 'rb') as f:
                f.seek(start)
                content_length = end - start + 1
                file_content = f.read(content_length)
            
            response_headers = f"""{http_version} 206 Partial Content\r
Date: {datetime.datetime.now(datetime.UTC).strftime('%a, %d %b %Y %H:%M:%S GMT')}\r
Server: Simple-File-Server/1.0\r
Content-Type: {file_type}\r
Content-Length: {len(file_content)}\r
Content-Range: bytes {start}-{end}/{file_size}\r
Last-Modified: {file_info['modified'].strftime('%a, %d %b %Y %H:%M:%S GMT')}\r
Accept-Ranges: bytes\r
Connection: close\r
\r
"""
            
            # Send headers and content
            connection_socket.send(response_headers.encode())
            connection_socket.send(file_content)
            
        except Exception as e:
            print(f"Error serving partial content: {e}")
            self.generate_response_404(http_version, connection_socket)

        #return response

    def generate_content_type(self, file_type):
        #Generate Headers
        return f"Content-Type: {file_type}\r\n"

    def eval_commands(self, commands):
        command_dict = {}
        for item in commands[1:]:
            item = item.strip()
            if ':' in item and item:
                try:
                    key, value = item.split(":", 1)
                    command_dict[key.strip()] = value.strip()
                except ValueError:
                    print(f"Invalid command format: {item}")
                    continue
            # splitted_item = item.split(":")
            # command_dict[splitted_item[0]] = splitted_item[1].strip()
        return command_dict

if __name__ == "__main__":
    Vod_Server(int(sys.argv[1]))
