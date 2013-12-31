#! /usr/bin/env python3

# TODO: Use argparse namespaces
# TODO: Check in list
# TODO: Harsh checking
# TODO: Assertations for is_pool...
# TODO: recurse in named args is bad style
# TODO: Some verbose output
# TODO: Checks should do actual checks in FS
# TODO: Mount/Unmount
# TODO: Attach/Detach

import argparse as ap
import tarfile
import time
import sys
import io
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
    assert no > 0, "no > 0"
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

def wrap_list(e):
    """Force the element to be an iterable.  Returns just
    the element if it's already an iterable, otherwise
    returns a tuple with `e` as the single element."""
    if hasattr(e, '__iter__'):
        return e
    else:
        return (e,)

def unwrap_list(l__):
    """Reverse of wrap_list.
    If l__ contains a single element l__[0] is returned.
    If it is empty or contains more a,
    list with all the elements is returned.
    Note that this always evaluates iterables."""
    l = list(l)
    if len(l) == 1:
        return l[1]
    else:
        return l

def compatl(l):
    """Builds an iterator from the given list that skips
    None values."""
    return (v for v in l if v != None)

def tail(l):
    """Tail of a list.
    All elements except the first."""
    return l[1:]

def XOR(*arg):
    """Boolean XOR. Inverse of XNOR.
    Takes any number of arguments. Will return true, if the
    number of truthy arguments is odd. For two arguments
    this is equal to `bool(a) != bool(b)`."""
    # This works because True<=>1, False<=>0
    return bool( sum(map(bool, arg)) % 2 )

def XNOR(*arg):
    """Boolean XNOR. Inverse of XOR.
    Takes any number of arguments and returns, wether the
    number of truthy arguments is even. For two arguments
    this is equivalent to `bool(a) == bool(b)`."""
    return not XOR(*arg)

#######################################################
# BTRFS

class __BtrfsListEntry:
    """Holds info about a btrfs Prefix. Properties:
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

class BVolAssertError(IOError):
    pass

class BVol:
    def fromPathInPrefix(prefix, pa):
        pool, subvol = fill(pa.split("/", 1), 2, '')
        self.fromPathInPool(rppt, pool, subvol)

    # TODO: Use [path]
    @staticmethod
    def fromPathInPool(prefix, pool, subvol):
        pathar = subvol.split('/')
        atsegs = pathar[-1].split("@")
        if len(atsegs) > 1:
            snap = atsegs.pop()
            name = join(atsegs, '')
            pathar[-1] = name
            return BVol(prefix, pool, join(pathar, '/'), snap)
        else:
            return BVol(prefix, pool, subvol)
    

    def __init__(self, prefix, pool='', subvol='', snapname=''):
        """
        Instanciate a BVol object from it'S components:
            prefix - The prefix for all BVols, by default /bvol.
                   Must be absolute
            pool - The *name* of the pool (under  prefix).
                   Must be '' if this is a prefix.
            subvol - The path of subvol, by default.
                     Must be '' if this is a pool or prefix.
            snapname - The name of the snapshot if this is a
                   snapshot. Mus
        """
        self.prefix = prefix
        self.vol = vol
        self.subvol = subvol

    def __str__(self):
        return self.pathin_fs()

    def __eq__(self, other):
        return self is other \
            or  self.pathin_fs() == other.pathin_fs()

    def __cwd(self):
        """Directory this normally operates in"""
        return self.get_prefix().pathin_fs()

    def pathin_fs(self):
        """Returns the absolute path of this volume in the
        prefix fs."""
        return path.join(prefix, pool, subvol + self.__snapsuf())

    def pathin_pool(self):
        """Returns the path of this subvol relative to the
        pool. Will return .' if this is a subvol or prefix."""
        return subvol + self.__snapsuf()

    def pathin_prefix(self):
        """Returns the path of this subvolume relative to
        the pool. Returns ''  if this is a prefix."""
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

    def exists(self):
        return path.exists(self.pathin_fs)

    def is_pool(self):
        return not empty(self.pool) and     empty(self.subvo)

    def is_subvol(self):
        return not empty(self.pool) and not empty(self.subvol)

    def is_prefix(self):
        return     empty(self.pool) and     empty(self.subvol)

    def is_snap(self):
        return empty(self.snapname)

    def _ver_(self, boll, msg,
            action="--", expect=True, skip=False):
        """Raises an BVolAssertError with the given message,
        and the given action if `boll is not expect`. Both
        boll and expect are interpreted as booleans.  msg
        shall contain an info of what has gone wrong, action
        shall contain an info, during which action something
        has gone wrong.
        If skip is set, the checking is skipped """
        if (not skip) and XOR(expect, boll):
            stat = ""
            if not expect:
                stat = "[NOT] "

            raise BVolAssertError(join(
                "BVol assertation Error.",
                "\nREASON: ", stat, msg,
                "\nDURING: ", action,
                "\nBVol: ", self.pathin_fs,
                "\n| PREFIX: ", self.prefix,
                "\n| POOL: ", self.pool,
                "\n| SUBV: ", self.subvol,
                "\n| SNAP: ", self.snapname,
                "\n"))

    def ver_exists(action="--", expect=False, skip=False):
        """Raise an error if this BVol exists in the FS or
        if it does not, depending on expect.
        BE CAREFUL: DEFAULT VALUE FOR EXPECT=FALSE."""
        _ver_(self.exists(), 
                "BVol does exist", expect, skip)

    def ver_pool(action="--", expect=True, skip=False):
        """Raise an error if this bvol is a pool or if it is
        not, depending on expect."""
        _ver_(self.is_pool(),
                "BVol is a pool", expect, skip)

    def ver_prefix(action="--", expect=True, skip=False):
        """Raise an error if this bvol is a prefix or if it is
        not, depending on expect."""
        _ver_(self.is_prefix(),
                "BVol is a prefix", expect, skip)

    def ver_pool(action="--", expect=True, skip=False):
        """Raise an error if this bvol is a pool or if it is
        not, depending on expect."""
        _ver_(self.is_pool(),
                "BVol is a pool", expect, skip)
 
    def ver_subvol(action="--", expect=True, skip=False):
        """Raise an error if this bvol is a subvol or if it
        is not, depending on expect."""
        _ver_(self.is_subvol(),
                "BVol is a subvol", expect, skip)

    def ver_pool(action="--", expect=True, skip=False):
        """Raise an error if this bvol is a pool or if it is
        not, depending on expect."""
        _ver_(self.is_pool(),
                "Subvol is a pool", expect, skip)

    def get_container(self):
        """Returns the volume holding this volume or '' if
        this is a prefix."""
        return BVol.from_path(path.dirname(self.pathin_fs()))

    def get_pool(self):
        """Get the pool of this volume.
        Will return self if this is a pool, or None if this
        is a prefix."""
        if self.is_prefix():
            return None
        elif self.is_pool():
            return self
        else:
            return BVol(self.prefix, self.pool, '')

    def get_prefix(self):
        """Get the prefix volume of this volume. Returns self
        if this is a prefix."""
        if self.is_prefix():
            return self
        else:
            return BVol(prefix)

    def get_orig(self):
        """Returns the snapshot destination if this is a
        snapshot or self if this is not a snapshot."""
        if self.is_snap():
            return BVol(prefix, pool, subvol)
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

        return (BVol.fromSubvol(self.prefix, self.pool, v) for v in l)

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
        pools and prefixs."""
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
        childs = self.childs(recursive=True)

        cloneschild.do_clone(child.pathin_pool()[prefl:], **C)

    def do_clone(self, targ, **A):
        """Create a clone of self at this subvol at the
        pathin_pool targ. Returns a BVol object for the
        created clone. If the 'recursive' flag is set each
        subvolume will be cloned too. In this case all
        created clones will be returned as an array. Does
        not work on prefixs; pools can not have snapshots but
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
                stdout = sys.stdout, stderr = sys.stderr,
                stdin = sys.stdin,
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
        returned in an array. Does not work on prefixs; pools
        can not have snapshots but might be used with
        recursion. The return value will be the BVol object
        of the snapshot created or a list of BVols for the
        created snapshots in recursive operation."""

        if flag(A, 'recursive'):
            return __do_snap_REC(name, **A)

        self.do_clone(
                join(subvol, '@', name, delm=""),
                readonly=True)
        
    def do_destroy(self, **A):
        """Destroys this volume. Does not work on prefixs and
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
                stdout = sys.stdout, stderr = sys.stderr,
                stdin = sys.stdin,
                cwd=self.cwd())
        return unwrap_list(todel)

    def __do_autosnap_DROP_parse(self, typ, limit, pref, splitc, **A):
        """Parse info for a snapshot and make sure it is valid
        and fits the given arguments.  Returns either None
        (if it is invalid) or the unixtime at the creation
        of this snapshot."""
        try:
            n = s.snapname()
            n_seg = split(splitc)
            (p, time__, suf) = nseg
            time = int(time__)
        except:
            return None

        if (p != prefix) or (typ != suf) or len(n_set) != 3:
            return None

        return time

    def __do_autosnap_DROP(self, typ, limit, pref, splitc, **A):
        """Implements the dropping functionality for old
        snapshots with do_autosnap."""
        cands= self.snapshots()
        cands= (v.__do_autosnap_DROP_parse(typ, limit, pref, splitc, **A) for v in cands)
        cands= sorted(cands)
        cands= cands[limit:]

        deld= ( v.do_destroy() for v in cands)
        deld= deld

        return list(deld)

    def do_autosnap(self, typ, limit, **A):
        """For automatic creation of snapshots: Creates
        recursively snapshots whith the naming scheme
        '@auto_$unixtime_$typ' and deletes the old snapshots
        so that a maximum of $limit snapshots is kept.
        Returns an array  containing the BVols for
        [$created, $deleted].
        Does not work on prefixs; pools
        can not have snapshots but might be used with
        recursion.
        Returns a tuple consisting of iterables for (1) the
        newly created snapshots and (2) the snapshots
        destroyed."""
        new= self.do_snap(
                join("auto", epoch(), typ, delim="_"),
                **A)
        new= cast_iter(new)
        new= list(new)

        dropped= (v.get_orig() for v in new)
        dropped= (v.__do_autosnap_DROP(typ, limit, "auto", "_", **A) for v in dropped)
        dropped= flat1(dropped)
        dropped= list(dropped)

        return (new, dropped)

    def basic_send(iot=sys.stdout, **A):
        """Generates a very basic send stream without the
        capabiliy of sendstreams with multiple subvolumes.
        Same as using 'btrfs send ...'. The stream is
        written to the writable stream specified in the iot
        argument.
        You can specify a 'verbosity'=N parameter to set the
        verbosity level. 'base'=[BVol] or 'base'=Bvol can
        be used to specifiy bases for incremental snapshots
        (equiv. to -i). Use 'parent' to specify…a parent
        ('-p').
        Returns iot."""

        p = []

        verbosity = default(A, 'verbosity', 0)
        p_verb = itr.repeat('-v', verbosity)
        p.push(p_verb)

        paren = default(A, 'parent')
        if paren:
            p_paren = ['-p', paren.pathin_pool()]
            p.push(p_paren)

        basevol = default(A, 'base')
        p_base= wrap_list(base)
        p_base= compact(base)
        p_base= ( ('-p', v.pathin_pool()) for v in base)
        p_base= flat1(p_base)
        p.push(p_base)

        path = self.pathin_pool()
        cmd = itr.chain(('btrfs', 'send', path), itr.chain(p))

        stream = default(A, 'stream', )

        proc = sub.Popen(
                cmd,
                stdout = iot, stderr = sys.stderr,
                stdin = sys.stdin,
                cwd=self.cwd())

        return iot

    def basic_recv(ios=sys.stdin, **A):
        """Recieve a basic  send stream (the ones generated
        by `basic send`). This is equivalent to the
        `btrfs recieve ...` command.
        You can specify a 'verbosity'=N parameter to set the
        verbosity level.
        Returns ios.""" 

        p = []
        verbosity = default(A, 'verbosity', 0)
        p_verb = itr.repeat('-v', verbosity)
        p.push(p_verb)

        path = self.pathin_pool()

        proc = sub.Popen(
                itr.chain(('btrfs', 'recieve'), p, path),
                stdout = sys.stdout, stderr = sys.stderr,
                stdin = ios,
                cwd=self.cwd())

        return ios

    def tar_send(iot=sys.stdout, **A):
        """Creates a complex send stream with the support
        for snapshots and recursive sends.
        If recursive!=true, this may not be a pool or prefix.

        Args:
          iot - File object that specifies where to write
                the tar file generated to.
          recursive=True - Wether to include subvolumes
          incremental=True - Wether to include snapshots.
          base=[BVol] - A list of volumes that are available
                        on the reieving side."""
        rec = flag(A, "recursive", True)
        inc = flag(A, "incremental", True)
        base = default(A, "base", ())

        if rec: # RECURSIVE (, INCREMENTAL)
            exp = self.subtree(only="vols")

            if not (flag(A, "incremental", True)):
                exp = (v for v in exp if not v.is_snap())  
        elif inc: # NOT RECURSIVE, INCREMENTAL
            exp = itr.chain((this), self.snapshots())
        else: # NEITHER RECURSIVE NOR INCREMENTAL
            exp = (this)

        if len(base) > 0:
            exp = (v for v in exp if v not in base)

######################################################
# SETUP

cfg={
    'prefix': default(os.environ, "BVOL_ROOT", "/bvol")
}

#####################################################
# COMMANDS

# bvol [-p PREFIX | --prefix PREFIX]

# snap auto [--noconfirm] PATH ID KEEP
# snap mk PATH NAME
# sub create PATH
# sub destroy [-r | --recursive] [--noconfirm] PATH
# sub clone --ro PATH PATH
# send [-r | --recursive] NAME [NAME...] > FILE
# send [--plain] > FILE
# recv < FILE
# recv --plain < FILE
# list [-s | --snaps | -S | --nosnaps] [-r | --recursive] PATH

#def c_snap_auto(args):
#def c_snap_mk(args):
#def c_snap_destroy(args):
#def c_sub_create(args):
#def c_sub_destroy(args):
#def c_send(args):
#def c_recv(args):
#def c_list(args):

def apa():
    """Generates the argument parse used by main"""
    m = ap.ArgumentParser(add_help = False)

    m.add_argument("--prefix", "-p", dest='prefix',
            default=cfg['prefix'])

    m.add_argument("-v", "--verbose",
            dest='verbosity',
            action="count")
    m.add_argument("--verbosity",
            dest="verbosity",
            action="store",
            type=int)
    m.add_argument("--quiet", "-q",
            dest="verbosity",
            action="store_const",
            const="-1")


    m.add_argument("--noconfirm", "--nc",
            dest="noconfirm",
            action="store_true",
            default=False)
    m.add_argument("--do-confirm",
            dest="noconfirm",
            action="store_false")

    m.add_argument("--simulate", "--dry-run",
            dest="simulate",
            action="store_true",
            default=False)


    m.add_argument("--recursive", "-r",
            dest="recursive",
            action="store_true",
            default=False)
    m.add_argument("--no-snaps", "-n",
            dest="no_snaps",
            action="store_true",
            default=False)
    m.add_argument("--only-snaps", "-s",
            dest="only_snaps",
            action="store_true",
            default=False)

    m_ = m.add_subparsers()

    # help
    m_help = m_.add_parser('help', aliases=['h'])

    m_auto = m_.add_parser('autosnap', aliases=['asnap'])
    m_auto.add_argument("vol")
    m_auto.add_argument("name")
    m_auto.add_argument("keep", type=int)


    m_mksnap = m_.add_parser('make-snap',
            aliases=['mksnap','snap'])
    m_mksnap.add_argument("vol")
    m_mksnap.add_argument("name")


    m_create = m_.add_parser("make-sub",
            aliases=['mksub', 'sub'])
    m_create.add_argument("vol")
    
    
    m_destroy = m_.add_parser("destroy",
            aliases = ['del', 'rm', 'remove', 'delete'])
    m_destroy.add_argument("vol")


    m_clone = m_.add_parser("clone")
    m_clone.add_argument("src")
    m_clone.add_argument("dest")
    m_clone.add_argument("-ro")

    
    m_list = m_.add_parser("list", aliases=['ls'])
    m_list.add_argument("vol")

    return m

def main(argv):
    print(apa().parse_args(argv))

if __name__ == "__main__":
    main(tail(sys.argv))
