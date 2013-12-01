# BVol

BVol is a tool to replace some of the
functionality I've been missing when switching from ZFS to Btrfs,
this includes:

* Recursive snapshots
* Automatic snapshots
* Replication streams

## How?

The general concept is to mount all Btrfs Volumes in one
common directory. Normally this is '/bvol'; I did not choose /dev/bvol because
actual data is available in the mounted volumes.

Then simple scripts are used to determine the vols
mounted below
