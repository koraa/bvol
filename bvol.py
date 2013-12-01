#/usr/bin/env python3

# TODO: Use argparse namespaces
# TODO: Check in list
# TODO: Harsh checking
# TODO: Assertations for is_pool...
# TODO: recurse in named args is bad style

import argparse
import time
import sys
import os
import os.path as path
import subprocess as sub
import itertools as itr
reject = itr.filterfalse
flat1 = itr.chain.from_iterable

#######################################################
# UTILS

def default(dic, name, norm=None):
    """Extract value from dictionary. Returns norm if the
    value does not exist."""
    return dic.get(name, norm)

def flag(dic, name, norm=False):
    """like norm(...) but uses False as the default return
    value."""
    return default(dic,name, norm)

def empty(iterbl):
    """Check if a string is empty"""
    print("empty?: ", iterbl)
    return len(iterbl) == 0

def join(*argv, **A):
    """Easy join: Take multiple arguments instead of a list,
    supports the flag delm, which specifies the delimiter.
    delm is by default ' '."""
    delm = flag(A, "delm", " ")
    segs = map(str, argv)
    return selm.join(segs)

def fill(l, no, default=None):
    """Append so many $defaults to l that it has at least no
    values."""
    return l + ([default] * (no - len(l)))

def epoch():
    """Return the unix time: Seconds since the 1.1.1970"""
    return int(time.time())

def ttystr(b):
    """Convert bytes from console to a string"""
    # TODO: This should care about the locale
    if isinstance(b, bytes):
        return str(path, encoding='utf-8')
    else:
        return b

#######################################################
# BTRFS

class __BtrfsListEntry:
    """Holds info about a btrfs Root. Properties:
    id, generation, level, path"""

    def __init__(self, line):
        [_, id, _,  generation, _, _, level,
                _, path] = line.split()
        self.path = ttystr(path)
        self.id = int(id)
        self.generation = int(generation)

def btrfs_list(mnt):
    """Returns a dict {$subvol-path: $__BtrfsListEntry} for
    a given pool"""
    proc = sub.Popen(['btrfs', 'sub', 'list', mnt],
            stdout = sub.PIPE,
            stderr=sys.stderr, stdin=sys.stdin)

    x= iter(proc.stdout.readline, b'')
    x= map(__BtrfsListEntry, x)
    x= ( (e.path, e) for e in x)
    return dict(x)


class BVol:
    # TODO: Subvol: with snapname <=> without
    # TODO: Use [path]
    @staticmethod
    def fromSubvol(root, pool, subvol):
        pathar = subvol.split('/')
        atsegs = pathar[-1].split("@")
        if len(atsegs) > 1:
            snap = atsegs.pop()
            name = join(atsegs, '')
            pathar[-1] = name
            return BVol(root, pool, join(pathar, '/'), snap)
        else:
            return BVol(root, pool, subvol)
    

    def __init__(self, root, pool='', subvol='', snapname=''):
        """
        Instanciate a BVol object from it'S components:
            root - The root for all BVols, by default /bvol.
                   Must be absolute
            pool - The *name* of the pool (under  root).
                   Must be '' if this is a root.
            subvol - The path of subvol, by default.
                     Must be '' if this is a pool or root.
            snapname - The name of the snapshot if this is a
                   snapshot. Mus
        """
        self.root = root
        self.vol = vol
        self.subvol = subvol

    def __str__(self):
        return self.pathin_fs()

    def __eq__(self, other):
        return self is other  or  self.pathin_fs() == other.pathin_fs()

    def __cwd(self):
        """Directory this normally operates in"""
        return self.get_root().pathin_fs()

    def pathin_fs(self):
        """Returns the absolute path of this volume in the
        root fs."""
        return path.join(root, pool, subvol + self.__snapsuf())

    def pathin_pool(self):
        """Returns the path of this subvol relative to the
        pool. Will return .' if this is a subvol or root."""
        return subvol + self.__snapsuf()

    def pathin_root(self):
        """Returns the path of this subvolume relative to
        the pool. Returns ''  if this is a root."""
        return pool + self.pathin_pool()


    def snapname(self):
        """Returns the name of the snapshot or '' if this is
        not a snapshot."""
        return snaname

    def volname(self):
        """Returns the name of this subvolume, not including
        the snapshot name."""
        if self.is_snap():
            return self.get_orig().fullname()
        else:
            return path.basename(self.pathin_fs())

    def __snapsuf(self):
        """Little cheat: returns '@snapname' if this is a
        snapshot, otherwise ''.
        This eases implementation of some functions here but
        it is not such a good interface for outside..."""
        if self.is_snap():
            return '@' + self.snapname()
        else:
            return ''

    def fullname(self):
        """Get the name with basic name and snapshot name
        (if this is a snapshot)."""
        return self.volname() + self.__snapsuf()

    def is_pool(self):
        return not empty(self.pool) and     empty(self.subvo)

    def is_subvol(self):
        return not empty(self.pool) and not empty(self.subvol)

    def is_root(self):
        return     empty(self.pool) and     empty(self.subvol)

    def is_snap(self):
        return empty(self.snapname)

    def get_container(self):
        """Returns the volume holding this volume or '' if
        this is a root."""
        return BVol.from_path(path.dirname(self.pathin_fs()))

    def get_pool(self):
        """Get the pool of this volume.
        Will return self if this is a pool, or None if this
        is a root."""
        if self.is_root():
            return None
        elif self.is_pool():
            return self
        else:
            return BVol(self.root, self.pool, '')

    def get_root(self):
        """Get the root volume of this volume. Returns self
        if this is a root."""
        if self.is_root():
            return self
        else:
            return BVol(root)

    def get_orig(self):
        """Returns the snapshot destination if this is a
        snapshot or self if this is not a snapshot."""
        if self.is_snap():
            return BVol(root, pool, subvol)
        else:
            return self

    def snapshots(self):
        """List all the snapshots of this volume.
        Always returns [] if this is a snapshot."""
        if self.is_snap():
            return []
        else:
            cand= self.get_container().childs()
            cand= [ v for v in cand if v == self ]
            return cand

    def childs(self, **A):
        """Returns all the childs of this subvolume as an
        array. If the flag 'recursive' is set the list will
        also contain children of children (of children...)."""
        # We use the pool path here to get the benefits of
        # caching in btrfs_list
        l= btrfs_list(self.get_pool().pathin_fs())
        l= l.keys() # => list of subvol paths

        l= (v for v in l if
                v.startswith(self.pathin_pool() + "/" ))

        if not flag(A, "recursive", False):
            # Filter deeper levels by checking for '/' in
            # the remaining portion of the string
            prefl = len(self.pathin_pool()) + 1
            l= (v for v in l if ( "/" not in v[prefl:]) )

        return (BVol.fromSubvol(self.root, self.pool, v) for v in l)

    def __subtree_ALL(self, **A):
        """Helper for subtree: the entire subtree."""
        return [self] + self.childs(**A)

    def __subtree_only_VOLS(self, **A):
        if self.is_pool():
            return childs(**A)
        else:
            return __subtree_ALL(**A)

    def subtree(self, **A):
        """Like childs, but also includes SELF.
        Supports the only='vols' argument to exclude
        pools and roots."""
        only = default(A, 'only')

        if only == 'vols':
            B=A.copy()
            B.pop("only", None)
            return __subtree_only_VOLS(**B)
        else:
            return __subtree_ALL(**A)

    def __do_clone_REC(self, targ, **A):
        """Helper function for recursive cloning.
        Use do_clone(recurisiv=true, ...) instead!"""

        B=A.copy().update({'recursive': False})
        # First clone this.
        do_clone(targ, **B)

        C=A.copy().update({
            'recursive': False,
            'relative_to': targ})

        # Now all the childs
        prefl = len(self.pathin_pool())
        childs = self.childs(recursive=True):

        cloneschild.do_clone(child.pathin_pool()[prefl:], **C)

    def do_clone(self, targ, **A):
        """Create a clone of self at this subvol at the
        pathin_pool targ. Returns a BVol object for the
        created clone. If the 'recursive' flag is set each
        subvolume will be cloned too. In this case all
        created clones will be returned as an array. Does
        not work on roots; pools can not have snapshots but
        might be used with recursion. If the 'readonly'
        flags is set the snapshot will be created...read
        only. The flag 'relative_to'=$path can be used to
        define the targ relative to a certain location.
        WARNING: ALL PATHS ARE INTERPRETED RELATIVELY"""
        # TODO: This needs tor return something
        # TODO: Relativity is shit
        if flag(A, "recursive"):
            return __do_clone_REC(targ, **A)

        params = []
        if flag(A, 'readonly'):
            params.append('-r')
        
        cwd=flag(A, 
                'relative_to', 
                self.cwd())

        proc = sub.Popen(
                ['btrfs', 'subvolume', 'snapshot', 
                    self.pathin_fs(), 
                    './' + targ] + params,
                stdout = STDOUT, stderr = STDERR,
                stdin = STDIN,
                cwd=cwd)

    def __do_snap_REC(self, name, **A):
        """Helper for do_snap for recursive opertaion."""
        B=A.copy().update({'recursive': False})

        vols = self.subtree(only='vols')
        return [v.do_snap(name, **B) for v in vols]

    def do_snap(self, name, **A):
        """Creates a snapshot of this with the specified
        title and returns the BVol for it.
        If the 'recursive' flag is given snapshots will be
        created for every child and their BVols will be
        returned in an array. Does not work on roots; pools
        can not have snapshots but might be used with
        recursion. The return value will be the BVol object
        of the snapshot created or a list of BVols for the
        created snapshots in recursive operation."""

        if flag(A, 'recursive'):
            return __do_snap_REC(name, **A)

        self.do_clone(
                join(subvol, '@', name, delm=""),
                readonly=True)
        
    def do_destroy(self, name, **A):
        """Destroys this volume. Does not work on roots and
        pools. If the volume has childs the 'recursive' flag
        must be used. Returns the bvols of the volumes
        destroyed. """
        # This must destroy caches.
        todel= [self]
        if flag(A, "recursive"):
            todel= self.subtree()

        delargs = (v.pathin_pool() for v in todel)
        proc = sub.Popen(
                ['btrfs', 'subvolume', 'destroy'] + delargs,
                stdout = STDOUT, stderr = STDERR,
                stdin = STDIN,
                cwd=self.cwd())
        return todel

    def __do_autosnap_DROP(self, typ, limit, **A):

    def do_autosnap(self, typ, limit, **A):
        """For automatic creation of snapshots: Creates
        recursively snapshots whith the naming scheme
        '@auto_$unixtime_$typ' and deletes the old snapshots
        so that a maximum of $limit snapshots is kept.
        Returns an array  containing the BVols for
        [$created, $deleted].
        Does not work on roots; pools
        can not have snapshots but might be used with
        recursion.
        Returns a tuple consisting of iterables for (1) the
        newly created snapshots and (2) the snapshots
        destroyed."""
        new = self.do_snap(
                join("auto", epoch(), typ, delim="_"),
                **A)

        dropped= (v.get_orig() for v in new)
        dropped= (v.__do_autosnap_DROP(typ, limit, **A) for v in dropped)
        dropped= flat1(dropped)

        return map(list, (new, dropped))


######################################################
# SETUP

cfg={
    'root': default(os.environ, "BVOL_ROOT", default="/bvol")
}

#####################################################
# COMMANDS

def cmd_attach(__argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("dev", 
        help="The block device containing the btrfs partition to attach")
    ap.add_argument("name",
        help="The name of the volume.")
    ap.add_argment("-o", "--mount-options",
        help="You can also parse options like with mount")
    argv = ap.parse_args(args=__argv)

    os.call([
        "mount", 
        "-t",     "btrfs",
        "-o",     argv.mount-options, 
        argv.dev,
        argv.name
    ])


def cmd_detach(__argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("name", 
        help="The name of the volume to unmount")
    argv = ap.parse_args(args=__argv)

    os.call([
        "umount", 
        resolve(argv.name, abs=True)
    ])

def cmd_list(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("vol", 
        help="The name of the volume to unmount")
    argv = ap.parse_args(args=__argv)


def cmd_snap(argv):
    pass

def cmd_delete(argv):
    pass

def cmd_autosnap(argv):
    pass
