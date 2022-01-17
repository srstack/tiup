<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
        <key>Label</key>
        <string>com.pingcap.{{.ServiceName}}.{{.Port}}</string>
        <key>Program</key>
        <string>{{.DeployDir}}/scripts/run_{{.ServiceName}}.sh</string>
        <key>StandardErrorPath</key>
        <string>{{.DeployDir}}/log/plist.log</string>
        <key>StandardOutPath</key>
        <string>{{.DeployDir}}/log/plist.log</string>
</dict>
</plist>