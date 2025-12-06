#!/bin/bash

IPS=(
    "172.22.95.98"
    "172.22.154.169"
    "172.22.158.169"
    "172.22.95.99"
    "172.22.154.170"
    "172.22.158.170"
    "172.22.95.100"
    "172.22.154.171"
    "172.22.158.171"
    "172.22.95.101"
)

for i in $(seq 1 10); do
    idx=$((i - 1))
    SELF_IP=${IPS[$idx]}
    num=$(printf "%02d" $i)
    HOST="fa25-cs425-51${num}.cs.illinois.edu"

    # First machine runs StormRuler; others run RainStorm
    if [ $i -eq 1 ]; then
        CMD="./StormRuler"
    else
        CMD="./RainStorm"
    fi

    echo "Opening Terminal for $HOST  (run: $CMD)"

    osascript <<EOF
tell application "Terminal"
    do script "ssh -t njacobs3@${HOST} 'cd g51mp4 && exec ${CMD}'"
end tell
EOF

done
