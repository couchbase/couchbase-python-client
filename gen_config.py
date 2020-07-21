import abc
import logging
import warnings
import pathlib
import os
from abc import abstractmethod
from enum import IntEnum
import json
import sys
import ssl
import platform
import posixpath
from enum import Enum
import argparse
import urllib.request
import datetime
import time
import traceback


curdir = pathlib.Path(__file__).parent

lcb_min_version_baseline = (3, 0, 1)


def win_cmake_path(orig_path):
    import posixpath
    return posixpath.normpath(orig_path).replace('\\','/')


def get_lcb_min_version():
    result = lcb_min_version_baseline
    try:
        # check the version listed in README.rst isn't greater than lcb_min_version
        # bump it up to the specified version if it is
        import docutils.parsers.rst
        import docutils.utils
        import docutils.frontend

        parser = docutils.parsers.rst.Parser()

        with open(str(curdir.joinpath("README.rst"))) as README:
            settings = docutils.frontend.OptionParser().get_default_values()
            settings.update(
                dict(tab_width=4, report_level=1, pep_references=False, rfc_references=False, syntax_highlight=False),
                docutils.frontend.OptionParser())
            document = docutils.utils.new_document(README.name, settings=settings)

            parser.parse(README.read(), document)
            readme_min_version = tuple(
                map(int, document.substitution_defs.get("libcouchbase_version").astext().split('.')))
            result = max(result, readme_min_version)
            logging.info("min version is {}".format(result))
    except Exception as e:
        warnings.warn("problem: {}".format(traceback.format_exc()))
    return result


lcb_min_version = get_lcb_min_version()


class SSL_MinVer(IntEnum):
    dev = 0
    beta_1 = 0x1
    beta_2 = 0x2
    beta_3 = 0x3
    beta_4 = 0x4
    beta_5 = 0x5
    beta_6 = 0x6
    beta_7 = 0x7
    beta_8 = 0x8
    beta_9 = 0x9
    beta_10 = 0xa
    beta_11 = 0xb
    beta_12 = 0xc
    beta_13 = 0xd
    beta_14 = 0xe
    release = 0xf


ssl_letter = bytes.decode(bytes((str.encode('a', 'utf-8')[0] + ssl.OPENSSL_VERSION_INFO[-2] - 1,)), 'utf-8')
ssl_major = "{}{}".format(".".join(map(str, ssl.OPENSSL_VERSION_INFO[:-2])), ssl_letter)


class DownloadableRepo(object):
    def __init__(self,
                 repository_name,  # type: str
                 gh_client=None,  # type: github.Github
                 timeout=None  # type: datetime.timedelta
                 ):

        import github
        self._deadline = datetime.datetime.now().__add__(timeout or datetime.timedelta(minutes=1))
        self._last_op = None  # type: datetime.datetime
        self.__rate_limit = None
        self._gh_client = gh_client or github.Github(login_or_token=os.getenv("PYCBC_GH_TOKEN_ENCRYPTED"))
        self._ghrepo = self.throttle_command(self._gh_client.get_repo, repository_name)

    @property
    def _rate_limit(self):
        if not self.__rate_limit or self.__rate_limit.core.reset>datetime.datetime.now():
            self._last_op = None
            self.__rate_limit=self._gh_client.get_rate_limit()
        return self.__rate_limit

    @property
    def min_wait(self):
        return datetime.timedelta(seconds=60*60/self._rate_limit.core.limit)

    @property
    def op_wait_time(self):
        if not self._last_op or self._last_op+self.min_wait>datetime.datetime.now():
            return datetime.timedelta(seconds=0)
        return self._last_op+self.min_wait-datetime.datetime.now()

    def throttle_command(self, cmd, *args, **kwargs):
        from github.GithubException import RateLimitExceededException
        while True:
            if not self._rate_limit.core.remaining:
                remainder=self._rate_limit.core.reset-datetime.datetime.now()
                if self._rate_limit.core.reset>self._deadline:
                    raise TimeoutError("Can't download all files in time, reset is {} away, but deadline is {} away".format(remainder,self._deadline-datetime.datetime.now()))
            else:
                remainder = self.op_wait_time
            logging.info("remainder = {}".format(remainder))
            if remainder:
                logging.warning("Rate limit exceeded, waiting {}".format(remainder))
                time.sleep(remainder.seconds)
            self._last_op = datetime.datetime.now()
            assert(self._last_op)
            try:
                return cmd(*args, **kwargs)
            except RateLimitExceededException as e:
                logging.warning(traceback.format_exc())

    def get_sha_for_tag(self,  # type: github.Repository
                        tag  # type: str
                        ):
        """
        Returns a commit PyGithub object for the specified repository and tag.
        """
        branches = self._ghrepo.get_branches()
        matched_branches = [match for match in branches if match.name == tag]
        if matched_branches:
            return matched_branches[0].commit.sha

        y = next(iter({x for x in self._ghrepo.get_tags() if x.name == tag}), None)
        return y.commit.sha if y else None

    def download_directory(self, sha, server_path, dest):
        """
        Download all contents at server_path with commit tag sha in
        the repository.
        """
        contents = self.throttle_command(self._ghrepo.get_dir_contents, server_path, ref=sha)
        if os.path.exists(dest):
            return
        os.makedirs(dest,exist_ok=True)
        for content in contents:
            print("Processing %s" % content.path)
            if content.type == 'dir':
                self.download_directory(sha, content.path, os.path.join(dest, content.path))
            else:
                dl_url=content.download_url
                dest_path=os.path.join(dest, content.name)
                print("Donwloading {} to {} from {}".format(content.path, dest_path, dl_url))
                urllib.request.urlretrieve(dl_url, dest_path)


class AbstractOpenSSL(abc.ABC):
    def get_headers(self, dest=os.path.abspath(os.path.curdir)):
        self.get_arch_content(dest, ('include',))

    def get_all(self, dest=os.path.abspath(os.path.curdir)):
        self.get_arch_content(dest, tuple())

    @abstractmethod
    def get_arch_content(self, dest, rel_path):
        pass


class Windows(object):
    class Machine(Enum):
        x86_64 = 'amd64'
        x86_32 = 'win32'
        aarch_be = 'arm64'
        aarch = 'arm64'
        armv8b = 'arm64'
        armv8l = 'arm64'
        AMD64 = 'amd64'
        WIN32 = 'win32'

    class OpenSSL(AbstractOpenSSL):
        def __init__(self,
                     arch  # type: Windows.Machine
                     ):
            self.arch = arch
            self.repo = DownloadableRepo('python/cpython-bin-deps')
            self.sha = self.repo.get_sha_for_tag("openssl-bin-{}".format(ssl_major))

        def get_arch_content(self, dest, rel_path):
            if self.sha:
                self.repo.download_directory(self.sha, posixpath.join(self.arch.value, *rel_path), dest)

    @classmethod
    def get_arch(cls):
        return cls.Machine[platform.machine()]

    @classmethod
    def get_openssl(cls):
        return cls.OpenSSL(cls.get_arch())


def get_system():
    if platform.system().lower().startswith('win'):
        return Windows
    return None


def get_openssl():
    system = get_system()
    try:
        return system().get_openssl() if system else None
    except Exception as e:
        logging.warning("Couldn't initialise OpenSSL repository {}".format(traceback.format_exc()))
    return None


def gen_config(temp_build_dir=None, ssl_relative_path=None, couchbase_core='couchbase_core'):
    build_dir = curdir.joinpath('build')

    if not os.path.exists(str(build_dir)):
        os.mkdir(str(build_dir))
    with open(str(build_dir.joinpath("lcb_min_version.h")), "w+") as LCB_MIN_VERSION:
        LCB_MIN_VERSION.write('\n'.join(
            ["#define LCB_MIN_VERSION 0x{}".format(''.join(map(lambda x: "{0:02d}".format(x), lcb_min_version))),
             '#define LCB_MIN_VERSION_TEXT "{}"'.format('.'.join(map(str, lcb_min_version))),
             '#define PYCBC_PACKAGE_NAME "{}"'.format(couchbase_core)]))

    if temp_build_dir:
        posix_temp_build_dir=os.path.normpath(temp_build_dir)
        ssl_abs_path=os.path.join(os.path.abspath(posix_temp_build_dir), ssl_relative_path or 'openssl')

        print("From: temp_build_dir {} and ssl_relative_path {} Got ssl_abs_path {}".format(temp_build_dir, ssl_relative_path, ssl_abs_path))
        #ssl_root_dir_pattern = os.getenv("OPENSSL_ROOT_DIR", ssl_abs_path)
        ssl_root_dir = win_cmake_path(ssl_abs_path.format(ssl_major))

        ssl_info = dict(major=ssl_major,
                        minor=SSL_MinVer(ssl.OPENSSL_VERSION_INFO[-1]).name.replace('_', ' '),
                        original=ssl.OPENSSL_VERSION,
                        ssl_root_dir=ssl_root_dir,
                        python_version=sys.version_info,
                        raw_version_info=".".join(map(str,ssl.OPENSSL_VERSION_INFO[:-2])))
        with open("openssl_version.json", "w+") as OUTPUT:
            json.dump(ssl_info, OUTPUT)

        if ssl_relative_path is not None:
            openssl = get_openssl()
            if openssl:
                try:
                    openssl.get_all(ssl_abs_path)
                except Exception as e:
                    logging.warning("Couldn't get OpenSSL headers: {}".format(traceback.format_exc()))

        return ssl_info
    return None


if __name__ == "__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument('--temp_build_dir', type=str,default=None)
    parser.add_argument('--ssl_relative_path', type=str,default=None)
    parser.parse_args()
    gen_config(**(parser.parse_args().__dict__))
