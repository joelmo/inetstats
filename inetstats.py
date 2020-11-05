import gzip
import os
import pickle
import tempfile
from collections import defaultdict
from enum import Enum
from ipaddress import ip_address
from os.path import join as join_path
from typing import List, Dict, ValuesView, Generator
from urllib.request import urlretrieve


class Organisation:

    def __init__(self, name):
        self.maintainers: List[Maintainer] = []
        self.type: Organisation.Type
        self.name: str = name

    class Type(Enum):
        IANA = 1  # Internet Assigned Numbers Authority
        RIR = 2  # Regional Internet Registry
        NIR = 3  # National
        LIR = 4  # Local
        OTHER = 5
        WHITEPAGES = 6


class Maintainer:
    _instances: Dict[str, 'Maintainer'] = {}

    def __new__(cls, net_name=None):
        if not net_name:
            return object.__new__(Maintainer)
        net_name = net_name.upper()
        if net_name in Maintainer._instances:
            return Maintainer._instances[net_name]
        else:
            instance = object.__new__(Maintainer)
            instance.net_name = net_name
            instance.maintains = []
            instance.ip4addresses = defaultdict(int)  # e.g. {SE: 1024, DK: 256}
            instance.ip6addresses = defaultdict(int)
            instance.num4routes = 0
            instance.num6routes = 0
            Maintainer._instances[net_name] = instance
            return instance

    def maintains(self) -> List[Organisation]:
        return self.maintains


_pickle_path = join_path(tempfile.gettempdir(), "inetstats.pickle")


def _build_db(update=False):
    Maintainer._instances = {}

    def read_db(dbname):
        filename = "ripe.db.%s.gz" % dbname
        path = join_path(tempfile.gettempdir(), filename)
        if update:
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
        if update or not os.path.isfile(path):
            tmp_path, _ = urlretrieve("ftp://ftp.ripe.net/ripe/dbase/split/" + filename)
            os.rename(tmp_path, path)
        with gzip.open(path, mode="rt", encoding="latin1") as f:
            for ln in f.readlines():
                split = ln.split(':', 1)
                if len(split) != 2:
                    continue
                attr = split[0]
                value = split[1].strip()
                yield attr, value

    def process_organisations(lines):
        org = None
        for attr, value in lines:
            if attr == "org-name":
                org = Organisation(value)
            elif attr == "org-type":
                org.type = Organisation.Type[value.upper()]
            elif attr == "mnt-by":
                mnt = Maintainer(value)
                mnt.maintains.append(org)

    def process_inetnum(lines, version4=True):
        num_addresses = 0
        country = ""
        for attr, value in lines:
            if attr in ["inetnum", "inet6num"]:
                split = value.split(" - ", 1)
                if len(split) != 2:
                    continue
                ipa, ipb = split
                num_addresses = ip_address(ipb)._ip - ip_address(ipa)._ip + 1
            elif attr == "country":
                country = value
            elif attr == "mnt-by":
                mnt = Maintainer(value)
                if version4:
                    mnt.ip4addresses[country] += num_addresses
                else:
                    mnt.ip6addresses[country] += num_addresses

    def process_routes(lines, version4=True):
        num_routes = 0
        for attr, value in lines:
            if attr in ["route", "route6"]:
                _, suffix = value.split('/')
                num_routes = pow(2, (32 if version4 else 128) - int(suffix))
            elif attr == "mnt-by":
                mnt = Maintainer(value)
                if version4:
                    mnt.num4routes += num_routes
                else:
                    mnt.num6routes += num_routes

    process_organisations(read_db("organisation"))
    process_inetnum(read_db("inetnum"))
    process_inetnum(read_db("inet6num"), version4=False)
    process_routes(read_db("route"))
    process_routes(read_db("route6"), version4=False)

    if update:
        os.remove(_pickle_path)
    with open(_pickle_path, "wb") as f:
        pickle.dump(Maintainer._instances, f)


def _load_db():
    if not os.path.isfile(_pickle_path):
        _build_db()
    with open(_pickle_path, "rb") as f:
        try:
            Maintainer._instances = pickle.load(f)
        except AttributeError:
            f.close()
            update_db()


def rebuild_db():
    os.remove(_pickle_path)
    _load_db()


def update_db():
    _build_db(update=True)
    _load_db()


def maintainers() -> ValuesView[Maintainer]:
    return Maintainer._instances.values()


def organisations() -> Generator[Organisation, None, None]:
    for m in maintainers():
        for o in m.maintains:
            yield o


_load_db()
