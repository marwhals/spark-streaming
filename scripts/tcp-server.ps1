# Save as tcp-server.ps1 --- for sending data to spark job

$port = 12345

# Start TCP Listener
$listener = [System.Net.Sockets.TcpListener]::new($port)
$listener.Start()
Write-Host "Listening on port $port. Waiting for client connection..."

$client = $listener.AcceptTcpClient()
Write-Host "Client connected."

$stream = $client.GetStream()
$writer = New-Object System.IO.StreamWriter($stream)
$writer.AutoFlush = $true

while ($true) {
    $message = Read-Host "Enter message (type 'exit' to quit)"
    if ($message -eq "exit") {
        break
    }
    $writer.WriteLine($message)
}

Write-Host "Closing connection."
$writer.Close()
$client.Close()
$listener.Stop()
