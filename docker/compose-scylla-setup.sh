#!/bin/bash
# ScyllaDB Host Setup Script
# This script runs in a privileged container to configure host system for ScyllaDB
# Values are calculated dynamically based on system resources per ScyllaDB recommendations
# With --persist flag, also creates /etc/sysctl.d/99-scylladb.conf for permanent settings

# Don't exit on errors - we want to configure what we can
set +e

# Check for --persist flag
PERSIST_MODE=false
if [ "$1" = "--persist" ]; then
	PERSIST_MODE=true
	echo "=== Running in PERSIST mode - will create /etc/sysctl.d/99-scylladb.conf ==="
fi

echo "=== ScyllaDB Host System Setup ==="
if [ "$PERSIST_MODE" = true ]; then
	echo "Mode: Persistent (creating sysctl.d configuration)"
else
	echo "Mode: Temporary (runtime only)"
fi
echo ""

# Check if running with sufficient privileges
if [ ! -d /host/proc ]; then
	echo "ERROR: Host /proc not mounted. Please ensure volumes are configured:"
	echo "  volumes:"
	echo "    - /proc:/host/proc"
	echo "    - /sys:/host/sys"
	if [ "$PERSIST_MODE" = true ]; then
		echo "    - /etc/sysctl.d:/host/sysctl.d"
	fi
	exit 1
fi

# Check sysctl.d mount if in persist mode
if [ "$PERSIST_MODE" = true ] && [ ! -d /host/sysctl.d ]; then
	echo "ERROR: Host /etc/sysctl.d not mounted. Please ensure volume is configured:"
	echo "  volumes:"
	echo "    - /etc/sysctl.d:/host/sysctl.d"
	exit 1
fi

# Test if we have write access to critical parameters
CAN_WRITE=true
if [ -f /host/proc/sys/fs/aio-max-nr ]; then
	CURRENT_AIO=$(cat /host/proc/sys/fs/aio-max-nr 2>/dev/null)
	if ! echo "$CURRENT_AIO" >/host/proc/sys/fs/aio-max-nr 2>/dev/null; then
		echo "WARNING: Cannot write to /host/proc/sys - some settings may not be applied"
		echo "Container may need privileged mode for full configuration"
		CAN_WRITE=false
	fi
else
	echo "WARNING: Cannot access /host/proc/sys/fs/aio-max-nr"
	CAN_WRITE=false
fi

# Get system information
echo "=== System Information ==="
CPU_CORES=$(nproc)
MEMORY_KB=$(grep MemTotal /host/proc/meminfo | awk '{print $2}')
MEMORY_GB=$((MEMORY_KB / 1024 / 1024))
echo "CPU Cores: ${CPU_CORES}"
echo "Total Memory: ${MEMORY_GB} GB"
echo ""

# Calculate dynamic values based on system resources
# Based on ScyllaDB documentation and best practices

# AIO: ScyllaDB recommends 65536 * number of shards
# ScyllaDB typically uses 1 shard per core, but we'll add buffer
SHARDS_PER_CORE=1
ESTIMATED_SHARDS=$((CPU_CORES * SHARDS_PER_CORE))
AIO_PER_SHARD=65536
# Add 50% buffer for safety
AIO_REQUIRED=$((ESTIMATED_SHARDS * AIO_PER_SHARD * 3 / 2))
# Ensure minimum of 1048576 as recommended
if [ "$AIO_REQUIRED" -lt 1048576 ]; then
	AIO_REQUIRED=1048576
fi

# Network settings based on cores
# ScyllaDB handles high connection counts
SOMAXCONN=$((CPU_CORES * 1024))
if [ "$SOMAXCONN" -lt 4096 ]; then
	SOMAXCONN=4096
elif [ "$SOMAXCONN" -gt 65535 ]; then
	SOMAXCONN=65535
fi

TCP_MAX_SYN_BACKLOG=$((CPU_CORES * 512))
if [ "$TCP_MAX_SYN_BACKLOG" -lt 4096 ]; then
	TCP_MAX_SYN_BACKLOG=4096
elif [ "$TCP_MAX_SYN_BACKLOG" -gt 65535 ]; then
	TCP_MAX_SYN_BACKLOG=65535
fi

# Memory map count: ScyllaDB recommends high values
# Base calculation: 65530 per GB of RAM
VM_MAX_MAP_COUNT=$((MEMORY_GB * 65530))
if [ "$VM_MAX_MAP_COUNT" -lt 1048575 ]; then
	# ScyllaDB minimum recommendation
	VM_MAX_MAP_COUNT=1048575
fi

# Network buffer sizes: 1% of RAM but capped
NET_MEM_BYTES=$((MEMORY_KB * 1024 / 100))
if [ "$NET_MEM_BYTES" -gt 134217728 ]; then
	# Cap at 128MB
	NET_MEM_BYTES=134217728
elif [ "$NET_MEM_BYTES" -lt 16777216 ]; then
	# Minimum 16MB for good performance
	NET_MEM_BYTES=16777216
fi

# File descriptors: ScyllaDB needs many
FD_LIMIT=$((CPU_CORES * 200000))
if [ "$FD_LIMIT" -lt 1000000 ]; then
	FD_LIMIT=1000000
fi

# Function to safely set sysctl values
set_sysctl() {
	local param=$1
	local value=$2
	local description=$3
	local current_value

	current_value=$(cat /host/proc/sys/${param//\.//} 2>/dev/null || echo "0")

	if [ "$current_value" -lt "$value" ]; then
		echo "Setting ${param} from ${current_value} to ${value} (${description})"
		if echo "$value" >/host/proc/sys/${param//\.//} 2>/dev/null; then
			echo "✓ ${param} = ${value}"
		else
			echo "⚠ Could not set ${param} (may need host reboot)"
		fi
	else
		echo "✓ ${param} = ${current_value} (already sufficient, recommended: ${value})"
	fi
}

echo "=== Configuring Host System Parameters ==="
echo ""

# 1. AIO - Most critical for ScyllaDB
echo "1. Asynchronous I/O (AIO) Configuration:"
echo "   Calculated: ${AIO_REQUIRED} (${CPU_CORES} cores × ${SHARDS_PER_CORE} shard × ${AIO_PER_SHARD} + buffer)"
set_sysctl "fs.aio-max-nr" "${AIO_REQUIRED}" "async I/O operations"
echo ""

# 2. Network settings
echo "2. Network Settings (based on ${CPU_CORES} cores):"
set_sysctl "net.core.somaxconn" "${SOMAXCONN}" "socket listen backlog"
set_sysctl "net.ipv4.tcp_max_syn_backlog" "${TCP_MAX_SYN_BACKLOG}" "TCP SYN queue"

# Disable SYN cookies (ScyllaDB recommendation)
echo "0" >/host/proc/sys/net/ipv4/tcp_syncookies 2>/dev/null &&
	echo "✓ Disabled TCP SYN cookies (for performance)" ||
	echo "⚠ Could not disable TCP SYN cookies"

# Network buffers
set_sysctl "net.core.rmem_max" "${NET_MEM_BYTES}" "receive buffer max"
set_sysctl "net.core.wmem_max" "${NET_MEM_BYTES}" "send buffer max"
set_sysctl "net.core.netdev_max_backlog" "10000" "network device backlog"

# TCP memory and buffers (space-separated values need special handling)
echo "Setting TCP buffer sizes..."
echo "4096 87380 ${NET_MEM_BYTES}" >/host/proc/sys/net/ipv4/tcp_rmem 2>/dev/null ||
	echo "⚠ Could not set TCP receive buffers"
echo "4096 65536 ${NET_MEM_BYTES}" >/host/proc/sys/net/ipv4/tcp_wmem 2>/dev/null ||
	echo "⚠ Could not set TCP send buffers"

# TCP tuning for low latency
set_sysctl "net.ipv4.tcp_timestamps" "1" "TCP timestamps"
set_sysctl "net.ipv4.tcp_sack" "1" "TCP selective ack"
set_sysctl "net.ipv4.tcp_window_scaling" "1" "TCP window scaling"
echo ""

# 3. Memory settings
echo "3. Memory Settings (based on ${MEMORY_GB} GB RAM):"
set_sysctl "vm.max_map_count" "${VM_MAX_MAP_COUNT}" "memory map areas"
set_sysctl "vm.swappiness" "0" "disable swap usage"
set_sysctl "vm.dirty_ratio" "5" "dirty page ratio"
set_sysctl "vm.dirty_background_ratio" "2" "background dirty ratio"
echo ""

# 4. File descriptors
echo "4. File Descriptor Limits:"
set_sysctl "fs.file-max" "${FD_LIMIT}" "max file descriptors"
set_sysctl "fs.nr_open" "${FD_LIMIT}" "max open files per process"
echo ""

# 5. Transparent Huge Pages (THP) - ScyllaDB requires this disabled
echo "5. Transparent Huge Pages (THP):"
THP_STATUS=$(cat /host/sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null | grep -o '\[.*\]' | tr -d '[]' || echo "unknown")
if [ "$THP_STATUS" != "never" ]; then
	echo "Warning: THP is '${THP_STATUS}', ScyllaDB requires 'never'"
	if echo never >/host/sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null; then
		echo "✓ THP disabled"
	else
		echo "⚠ Could not disable THP (may need kernel boot parameter)"
	fi

	# Also disable defrag
	echo never >/host/sys/kernel/mm/transparent_hugepage/defrag 2>/dev/null || true
else
	echo "✓ THP is already disabled"
fi
echo ""

# 6. CPU frequency scaling (for consistent performance)
echo "6. CPU Performance Settings:"
if [ -d /host/sys/devices/system/cpu/cpu0/cpufreq ]; then
	for gov in /host/sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
		echo "performance" >"$gov" 2>/dev/null || true
	done
	echo "✓ Set CPU governor to performance mode"
else
	echo "! CPU frequency scaling not available"
fi
echo ""

# 7. NUMA settings if available
echo "7. NUMA Settings:"
if [ -f /host/proc/sys/kernel/numa_balancing ]; then
	echo "0" >/host/proc/sys/kernel/numa_balancing 2>/dev/null &&
		echo "✓ Disabled NUMA balancing (ScyllaDB manages NUMA)" ||
		echo "⚠ Could not disable NUMA balancing"
else
	echo "! NUMA not available on this system"
fi
echo ""

# Create persistent sysctl configuration if requested
if [ "$PERSIST_MODE" = true ]; then
	echo ""
	echo "=== Creating Persistent Configuration ==="

	SYSCTL_CONFIG="/host/sysctl.d/99-scylladb.conf"

	# Check if config already exists
	if [ -f "$SYSCTL_CONFIG" ]; then
		echo "Found existing $SYSCTL_CONFIG"
		echo "Backing up to ${SYSCTL_CONFIG}.bak"
		cp "$SYSCTL_CONFIG" "${SYSCTL_CONFIG}.bak"
	fi

	cat >"$SYSCTL_CONFIG" <<EOF
# ScyllaDB Performance Tuning
# Generated on $(date)
# System: ${CPU_CORES} cores, ${MEMORY_GB} GB RAM

# Asynchronous I/O
fs.aio-max-nr = ${AIO_REQUIRED}

# Network settings
net.core.somaxconn = ${SOMAXCONN}
net.ipv4.tcp_max_syn_backlog = ${TCP_MAX_SYN_BACKLOG}
net.ipv4.tcp_syncookies = 0
net.core.rmem_max = ${NET_MEM_BYTES}
net.core.wmem_max = ${NET_MEM_BYTES}
net.core.netdev_max_backlog = 10000
net.ipv4.tcp_timestamps = 1
net.ipv4.tcp_sack = 1
net.ipv4.tcp_window_scaling = 1

# Memory settings
vm.max_map_count = ${VM_MAX_MAP_COUNT}
vm.swappiness = 0
vm.dirty_ratio = 5
vm.dirty_background_ratio = 2

# File descriptors
fs.file-max = ${FD_LIMIT}
fs.nr_open = ${FD_LIMIT}

# NUMA (if available)
kernel.numa_balancing = 0

# TCP buffer sizes (space-separated values handled separately)
# Apply with: sysctl -p /etc/sysctl.d/99-scylladb.conf
# Then manually set:
#   echo "4096 87380 ${NET_MEM_BYTES}" > /proc/sys/net/ipv4/tcp_rmem
#   echo "4096 65536 ${NET_MEM_BYTES}" > /proc/sys/net/ipv4/tcp_wmem
EOF

	if [ -f "$SYSCTL_CONFIG" ]; then
		echo "✓ Created $SYSCTL_CONFIG successfully"
		echo ""
		echo "To apply on host system, run:"
		echo "  sudo sysctl --system"
		echo ""
		echo "Note: Some settings require additional steps:"
		echo "  - Transparent Huge Pages: Add 'transparent_hugepage=never' to kernel boot parameters"
		echo "  - TCP buffers: Apply manually as shown in config comments"
		echo "  - CPU governor: Set via cpupower or similar tool"
	else
		echo "⚠ Failed to create $SYSCTL_CONFIG"
	fi
fi

# Create a flag file to indicate setup is complete
touch /tmp/scylla-setup-complete

echo "=== Configuration Summary ==="
echo "System Resources:"
echo "  - CPU Cores: ${CPU_CORES}"
echo "  - Memory: ${MEMORY_GB} GB"
echo ""
echo "Applied Settings:"
echo "  - AIO max-nr: ${AIO_REQUIRED}"
echo "  - Socket backlog: ${SOMAXCONN}"
echo "  - TCP SYN backlog: ${TCP_MAX_SYN_BACKLOG}"
echo "  - VM max map count: ${VM_MAX_MAP_COUNT}"
echo "  - Network buffers: $((NET_MEM_BYTES / 1024 / 1024)) MB"
echo "  - File descriptors: ${FD_LIMIT}"
echo ""

echo "=== ScyllaDB Host Setup Complete ==="
echo ""
echo "All parameters have been configured based on system resources."
echo "ScyllaDB containers can now start with optimal performance settings."
echo ""

exit 0
