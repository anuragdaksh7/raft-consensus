go run main.go 5001
go run main.go 5002
go run main.go 5003
go run main.go 5004
go run main.go 5005

# Run each command in a separate PowerShell terminal.
# The node writes election and heartbeat activity to that terminal.

# Or start all nodes from one PowerShell window and save output:
$ports = 5001, 5002, 5003, 5004, 5005
foreach ($port in $ports) {
    Start-Process powershell -ArgumentList "-NoExit", "-Command", `
        "go run . $port 2>&1 | Tee-Object node-$port.log"
}

# Stop a node by focusing its terminal and pressing Ctrl+C.
# Persistent state is stored in raft-state-<port>.json.
# Delete those files before a clean election test:
Remove-Item .\raft-state-500*.json