#!/usr/bin/env python
import os
import os.path
import sys
import argparse
import urllib2
import zipfile
import shutil
import glob
import subprocess
import json
import re
import datetime
from ConfigParser import ConfigParser
from tempfile import tempdir

ap = argparse.ArgumentParser()

ap.add_argument('-p', '--prefix',
                help="Prefix for libcouchbase files",
                required=True,
                type=str)

ap.add_argument('-A', '--arch',
                help="Architecture (x86 or x64)",
                required=True)

ap.add_argument('-V', '--version',
                help="Python Version (i.e. XY)",
                required=True)

ap.add_argument('-l', '--lcb-version',
                help="Version for libcouchbase",
                default='2.4.7')

ap.add_argument('-i', '--inplace', action='store_true')

ap.add_argument('-U', '--self-update',
                action='store_true',
                help="Update this script to the latest version, in place")

ap.add_argument('-T', '--test',
                help="Run unit tests when complete",
                action='store_true')

ap.add_argument('-m', '--mock',
                help="URL from which to download the mock",
                default=None)

ap.add_argument('-j', '--jenkins',
                help="Use Jenkins libcouchbase build from job named", default="",
                type=str, required=False)

opts = ap.parse_args()

MOCK_URL = 'http://packages.couchbase.com/clients/c/mock/CouchbaseMock-LATEST.jar'


PYPATH = os.path.join('C:\\', 'Python{0}_{1}'.format(opts.version, opts.arch))
if not os.path.exists(PYPATH):
    raise Exception("Couldn't find path %s" % (PYPATH,))
PY_EXE = os.path.join(PYPATH, "python.exe")
PIP_EXE = os.path.join(PYPATH, "Scripts", "pip.exe")
NOSE_EXE = os.path.join(PYPATH, "Lib", "site-packages", "nose", "__main__.py")


VC_VERSION = None
vmaj, vmin = [ int(x) for x in opts.version ]
if vmaj == 2:
    VC_VERSION = '9'
else:
    # Apparently only x86 uses VC10, x64 still uses VC10?
    if vmin >= 3:
        VC_VERSION = '10'
    else:
        VC_VERSION = '9'


ARCH_STR = 'x86' if opts.arch == 'x86' else 'amd64'
SDK_VERS = '6.1' if VC_VERSION == '9' else '7.1'


# Find libcouchbase version

VERSION_DICT=dict(ver = opts.lcb_version,
    arch = ARCH_STR,
    vcvers = VC_VERSION)

LCB_VSTR = "libcouchbase-{ver}_{arch}_vc{vcvers}".format(**VERSION_DICT)

if opts.jenkins and len(opts.jenkins)>0:
    LCB_REPO = "http://sdkbuilds.sc.couchbase.com/job/"+opts.jenkins+"/ARCH={arch},MSVCC_VER={vcvers},label=windows-builder/ws/BUILD/_CPack_Packages/{platarch}/ZIP/".format(platarch='win'+re.sub(r'86',r'32',re.sub(r'^[^0-9]*(.*)$',r'\1',ARCH_STR)),**VERSION_DICT)
else:
    LCB_REPO = "http://packages.couchbase.com/clients/c/"
zipurl = LCB_REPO + LCB_VSTR + ".zip"

lcb_deproot = os.path.join(opts.prefix, LCB_VSTR)
print("URL: ", zipurl)
if not os.path.exists(lcb_deproot):
    if not os.path.exists(lcb_deproot + ".zip"):
        dl = urllib2.urlopen(zipurl)
        if dl.getcode() != 200:
            raise Exception("Got unexpected HTTP", dl)
        zf = open(lcb_deproot + ".zip", "wb")
        zf.write(dl.read())
        zf.close()

    z = zipfile.ZipFile(lcb_deproot + ".zip", "r")
    for f in z.namelist():
        if f.endswith('/'):
            os.makedirs(os.path.join(opts.prefix, f.replace('/', '\\')))
        else:
            z.extract(f, opts.prefix)

bname = 'BUILD-{0}-{1}.bat'.format(LCB_VSTR, opts.version)
batchfile = open(bname, "w")

def format_bootstrap_line(path, args=""):
    return '"{0}" {1}'.format(path, args)


vcvars_base = "C:\\Program Files (x86)\\Microsoft Visual Studio {vc_version}.0\\VC"
vcvars_base = vcvars_base.format(vc_version = VC_VERSION)
if VC_VERSION == '9':
    vcvars_script = 'vcvars32.bat' if ARCH_STR == 'x86' else 'vcvars64.bat'
    vcvars_line = vcvars_base + '\\bin\\' + vcvars_script
    vcvars_line = format_bootstrap_line(vcvars_line)

else:
    #Visual Studio 2010 Express doesn't have the 64 bit compiler, but
    #it's included in the Windows SDK v7.1
    basepath = (
        "C:\\Program Files\\Microsoft SDKs\\Windows\\v7.1\\Bin\\SetEnv.cmd")

    arch_arg = 'x86' if ARCH_STR == 'x86' else 'x64'
    vcvars_line = format_bootstrap_line(basepath,
                                        "/release /{0}".format(arch_arg))

batchfile.write('''
setlocal enabledelayedexpansion
{pyexe} -c "import sys; print(sys.version)"
call {vcvars_line}
{pyexe} setup.py build_ext --include-dirs {lcb}\include --library-dirs {lcb}\lib --inplace
{pyexe} setup.py build_ext --include-dirs {lcb}\include --library-dirs {lcb}\lib
{pyexe} -c "import couchbase_core; print(couchbase_core.__version__)"
'''.format(vcvars_line = vcvars_line,
           pyexe = PY_EXE,
           lcb=lcb_deproot))

batchfile.close()

shutil.copyfile(os.path.join(lcb_deproot, "bin", "libcouchbase.dll"),
                os.path.join("couchbase", "libcouchbase.dll"))

rv = os.system(bname)
assert rv == 0
result=os.system(PIP_EXE + "install wheel")
print("got wheel install result {}".format(result))
os.system(PIP_EXE + "install --upgrade pip")
os.system(PIP_EXE + "--version")
os.system(PY_EXE + " setup.py bdist_wininst")
os.system(PY_EXE + " setup.py bdist")
os.system(PY_EXE + " setup.py bdist_wheel")

def download_and_bootstrap(src, name, prereq=None):
    """
    Download and install something if 'prerequisite' fails
    """
    if prereq:
        prereq_cmd = '{0} -c "{1}"'.format(PY_EXE, prereq)
        rv = os.system(prereq_cmd)
        if rv == 0:
            return

    ulp = urllib2.urlopen(src)
    fp = open(name, "wb")
    fp.write(ulp.read())
    fp.close()
    cmdline = "{0} {1}".format(PY_EXE, name)
    rv = os.system(cmdline)
    assert rv == 0

def maybe_install(pkgname, impname=None):
    if not impname:
        impname = pkgname

    prereq_cmd = '{0} -c "import {1}"'.format(PY_EXE, impname)
    rv = os.system(prereq_cmd)
    if rv == 0:
        return

    cmd = "{0} --exists-action=w install --build=PIPBUILD {1}".format(PIP_EXE, pkgname)
    rv = os.system(cmd)
    assert rv == 0


def try_java(path='java'):
    try:
        po = subprocess.Popen([path, '-version'])
        po.communicate()
        if po.returncode == 0:
            return True
        else:
            print("Java failed to return 0")
            return False
    except:
        return False


def ensure_java():
    if try_java('java'):
        print('Java in path')
        return

    # Try most recent first:
    for _ in ('8', '7', '6'):
        pth = 'C:\\Program Files (x86)\\Java\\jre{0}\\bin'.format(x)
        if os.path.exists(os.path.join(pth, "java.exe")):
            if try_java(os.path.join(pth, "java.exe")):
                print("Adding", pth, "to PATH")
                os.environ['PATH'] += ';' + pth
                return

    raise Exception("Couldn't get a working Java version!")

print("sys.version_info: "+str(sys.version_info))
if vmaj==3 and vmin<3:
    print("Using legacy PIP for 3.2")
    PIP_URL = "https://bootstrap.pypa.io/3.2/get-pip.py"
    EZ_SETUP = "https://bitbucket.org/pypa/setuptools/raw/0.7.4/ez_setup.py"
else:
    print("Using defacto PIP")
    PIP_URL = "https://bootstrap.pypa.io/get-pip.py"
    EZ_SETUP = "https://bootstrap.pypa.io/ez_setup.py"
if opts.test:
    #download_and_bootstrap(EZ_SETUP, "ez_setup", "import setuptools")
    download_and_bootstrap(PIP_URL, "get_pip", "import pip")
    maybe_install("testresources")
    maybe_install("nose")
    maybe_install("jsonschema") 
    maybe_install("configparser") 
    #maybe_install("configparser2") 
    maybe_install("testfixtures") 
    maybe_install("basictracer") 
    #maybe_install("jaeger_client") 
    #cmd = "{0} --exists-action=w install --build=PIPBUILD -r requirements.txt".format(PIP_EXE)
    #rv = os.system(cmd)
    #print("calling "+str(cmd)+" gave "+str(rv))
    cmd = "{0} --exists-action=w install --build=PIPBUILD -r dev_requirements.txt".format(PIP_EXE)
    rv = os.system(cmd)
    print("calling "+str(cmd)+" gave "+str(rv))
    # Write the test configuration..
    fp = open("tests.ini.sample", "r")
    template = ConfigParser()
    template.readfp(fp)
    template.set("realserver", "enabled", "False")
    template.set("mock", "path", "CouchbaseMock.jar")
    template.set("mock", "url", opts.mock or MOCK_URL)

    if os.path.exists("tests.ini"):
        raise Exception("tests.ini already exists")
    
    print("template is:[")
    template.write(sys.stdout)
    print(repr(template)+"]")
    tests_ini_path = os.path.join(os.getcwd(),"tests.ini")
    print("writing template to "+tests_ini_path)
    with open(tests_ini_path, "w+") as fp:
        template.write(fp)


    os.environ['LIBCOUCHBASE_EVENT_PLUGIN_NAME'] = 'select'
    ensure_java()
    cmd = "{} {} --with-xunit -v".format(PY_EXE,NOSE_EXE)
    print("About to launch {}".format(cmd))
    rv = os.system(cmd)
    os.unlink("tests.ini")
    assert rv == 0
