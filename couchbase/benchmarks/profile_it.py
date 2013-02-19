#!/usr/bin/env python
# Copyright (c) 2007 RADLogic
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
"""Profile a given script, without having to touch it.

usage: python profile_it.py [options] target [target args and target options]

target can be:
(1) the filename of a python script
(2) a module name (which will profile 'import module') (use -m)
(3) a module.function() style function call (use -m)
    (which will profile 'import module; module.function()')
    (enclosing the target in single quotes might be helpful)

The target args and target options will get passed to the target
through a modified sys.argv list. 

options:
  -m, --module-call
     target is a module name or module.function() function call
     (i.e. not a script filename)
  -r, --report-file PROFILE_REPORT_FILE
     send profile output data to PROFILE_REPORT_FILE
     this file can be read in by the pstats.Stats class
     and processed output can be generated through the Stats object
  --hotshot
     use hotshot profiler module (less overhead) (Requires at least Python 2.2)
  --help
     display this help and exit
  --version
     output version information and exit

examples:
   python profile_it.py -s report.txt hello.py --hello-opt helloarg
      Profile 'python hello.py --hello-opt helloarg'
      with profile output printed to stdout at the end.
   python profile_it.py -s -r report.txt hello.py helloarg > hello.output
      Profile hello.py keeping the profile report separate to the target
      script's output. (Use profile_stats.py to view the report.)

"""
__author__ = 'Tim Wegener <twegener@radlogic.com.au>'
__date__ = '$Date: 2007/03/27 03:20:41 $'
__version__ = '$Revision: 0.4 $'

import sys
import profile

def profile_func():
    """Call module/script given in global target.

    Call as script if global is_script is true.

    Called script is given (the possibly modified) sys.argv

    Otherwise, if target is a module then import it.
    If target is a module.function() call then import the module
    and call module.function()

    The following exceptions will occur for a bad target:
    IOError -- bad script filename
    ImportError -- bad module name
    AttributeError -- bad function name

    """
    if is_script:
        # Run the file and trick it into thinking it is the main script.
        execfile(target, {'__name__': '__main__'})
    else:
        module_function = target.split('.')
        module = '.'.join(module_function[:-1])
        if not module:
            module = module_function[0]
            cmd = 'import %s' % module 
        elif len(module_function) > 1:
            cmd = 'global %s; import %s; %s' % (module, module, target) 
        exec cmd in {}

def hotshot_profile(log_filename, target, is_script,
                    lineevents=0, linetimings=1):
    """Profile a given target using hotshot profiler."""

    import hotshot
    hot_profiler = hotshot.Profile(log_filename, lineevents, linetimings)
    if is_script:
        cmd = "execfile('%s', {'__name__': '__main__'})" % target
        namespace = {'__builtins__': __builtins__}
        hot_profiler.runctx(cmd, namespace, {})
    else:
        module_function = target.split('.')
        module = '.'.join(module_function[:-1])
        if not module:
            module = module_function[0]
            cmd = 'import %s' % module 
        elif len(module_function) > 1:
            cmd = 'global %s; import %s; %s' % (module, module, target) 
        hot_profiler.runctx(cmd, {}, {})
        hot_profiler.close()
        
    
def main():
    """Command-line front-end."""

    # target and is_script must be global since profile_func needs to be global
    # and requires this data
    global target
    global is_script
    usage = __doc__
    version = 'profile_it ' + __version__.split()[1] + '\n' \
              'Copyright (C) 2004 RADLogic Pty. Ltd.\n' + \
              'All rights reserved.\n'
    # Custom command-line option parser to keep profile_it options separate
    # from target options.
    argv = sys.argv[:]
    # Remove profile_it command.
    del argv[0]
    opts = {}
    while 1:
        try:
            arg = argv.pop(0)
        except IndexError:
            sys.stderr.write('Error: profile_it: Must specify target.'
                             ' Use --help for usage.\n')
            sys.exit(2)
        if arg[:2] == '--':
            # Long options for profile_it
            if '--version'.startswith(arg):
                opts['version'] = 1
                break
            elif '--help'.startswith(arg):
                opts['help'] = 1
                break
            elif '--module-call'.startswith(arg):
                opts['module-call'] = 1
            elif '--hotshot'.startswith(arg):
                opts['hotshot'] = 1
            elif '--lineevents'.startswith(arg):
                # only applicable with --hotshot
                opts['lineevents'] = 1
            elif '--no-linetimings'.startswith(arg):
                # only applicable with --hotshot
                opts['no-linetimings'] = 1
            elif '--report-file'.startswith(arg):
                try:
                    opts['report-file'] = argv.pop(0)
                except IndexError:
                    sys.stderr.write('Error: profile_it:'
                                     ' Must specify report filename.'
                                     ' Use --help for usage.\n')
                    sys.exit(2)
            else:
                sys.stderr.write('Error: profile_it: Invalid option: '
                                 +repr(arg)+'\n')
                sys.exit(2)
        elif arg[:1] == '-':
            # Short options for profile_it
            if arg == '-m':
                opts['module-call'] = 1
            elif arg == '-r':
                try:
                    opts['report-file'] = argv.pop(0)
                except IndexError:
                    sys.stderr.write('Error: profile_it:'
                                     ' Must specify report filename.'
                                     ' Use --help for usage.\n')
                    sys.exit(2)
            else:
                sys.stderr.write('Error: profile_it: Invalid option: '
                                 +repr(arg)+'\n')
                sys.exit(2)
        else:
            break
    if opts.has_key('version'):
        sys.stdout.write(version)
        sys.exit()
    if opts.has_key('help'):
        # Print help/usage information and exit.
        sys.stdout.write(usage)
        sys.exit()

    target = arg

    is_script = not opts.has_key('module-call')

    # Modify sys.argv so that target sees what it would normally see.
    # (i.e. profile_it options are removed.)
    sys.argv = argv
    if is_script:
        sys.argv.insert(0, target)
    # Run the python profiler on the target.
    if not opts.has_key('hotshot'):
        try:
            profile.run('profile_func()', filename=opts.get('report-file'))
        except (IOError, ImportError, AttributeError), msg:
            sys.stderr.write('Error: profile_it: %s\n' % msg)
            sys.exit(1)
    else:
        if not opts.has_key('report-file'):
            sys.stderr.write('Error: profile_it:'
                             ' The --report-file options is required'
                             ' when using --hotshot option'
                             ' Use --help for usage.\n')
            sys.exit(2)
        hotshot_profile(log_filename=opts['report-file'],
                        target=target,
                        is_script=is_script,
                        lineevents=opts.has_key('lineevents'),
                        linetimings=not opts.has_key('no-linetimings')
                        )


if __name__ == '__main__':
    main()
