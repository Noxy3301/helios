#!/bin/bash

# Get PID of build/server/helios-storage and kill it
ps aux | grep "build/server/helios-storage" | grep -v grep | awk '{print $2}' | xargs -r kill -9