# host
# innitate communication with arbiter and long flow
# Echo server program
# client program
import socket

HOST = 'localhost'    # The remote host
PORT = 50007              # The same port as used by the server
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((HOST, PORT))
s.sendall('Hello, world')
data = s.recv(1024)

# once data is receveived from arbiter
# ping to other host
# send data

print 'Received', repr(data)

# send message h1, h2, 100 bytes
s.close()
