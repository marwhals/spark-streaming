# For writing to a socket

choco install nmap
ncat.exe -lk 123123
echo hello world | ncat.exe 127.0.0.1 123123 # In another shell to test setup
