#!/bin/sh

sudo apt update && sudo apt install mdadm --no-install-recommends
find /dev/ | grep google-local-nvme-ssd
sudo mdadm --create /dev/md0 --level=0 --raid-devices=$(find /dev/ -name 'google-local-nvme-ssd*' | wc -l) $(find /dev/ -name 'google-local-nvme-ssd*')
sudo mdadm --detail --prefer=by-id /dev/md0
sudo mkfs.ext4 -F /dev/md0
sudo mkdir -p /mnt/disks/local-ssd
sudo mount /dev/md0 /mnt/disks/local-ssd
sudo chmod a+w /mnt/disks/local-ssd

while true; do
  ./linera storage check_existence --storage "dualrocksdbscylladb:/mnt/disks/local-ssd/linera.db:spawn_blocking:tcp:scylla-client.scylla.svc.cluster.local:9042"
  status=$?

  if [ $status -eq 0 ]; then
    echo "Database already exists, no need to initialize."
    exit 0
  elif [ $status -eq 1 ]; then
    echo "Database does not exist, attempting to initialize..."
    if ./linera-server initialize \
      --storage dualrocksdbscylladb:/mnt/disks/local-ssd/linera.db:spawn_blocking:tcp:scylla-client.scylla.svc.cluster.local:9042 \
      --genesis /config/genesis.json; then
      echo "Initialization successful."
      exit 0
    else
      echo "Initialization failed, retrying in 5 seconds..."
      sleep 5
    fi
  else
    echo "An unexpected error occurred (status: $status), retrying in 5 seconds..."
    sleep 5
  fi
done
