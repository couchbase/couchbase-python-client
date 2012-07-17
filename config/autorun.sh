#!/bin/sh

AUTOMAKE_FLAGS="--add-missing --copy --force --foreign --warning=portability"
ACLOCAL_FLAGS="-I m4"
AUTOCONF_CLAGS="--warnings=error --force"

ARGV0=$0
ARGS="$@"

die() { echo "$@"; exit 1; }

run() {
  echo "$ARGV0: running \`$@' $ARGS"
  $@ $ARGS
}

# Try to locate a program by using which, and verify that the file is an
# executable
locate_binary() {
  for f in $@
  do
    file=`which $f 2>/dev/null | grep -v '^no '`
    if test -n "$file" -a -x "$file"; then
      echo $file
      return 0
    fi
  done

  echo ""
  return 1
}

if test -f config/pre_hook.sh
then
  . config/pre_hook.sh
fi

if [ -d .git ]
then
  mkdir m4 > /dev/zero 2>&1
  perl config/version.pl || die "Failed to run config/version.pl"
fi

# Try to detect the supported binaries if the user didn't
# override that by pushing the environment variable
if test x$ACLOCAL = x; then
  ACLOCAL=`locate_binary aclocal-1.11 aclocal-1.10 aclocal`
  if test x$ACLOCAL = x; then
    die "Did not find a supported aclocal"
  fi
fi

if test x$AUTOMAKE = x; then
  AUTOMAKE=`locate_binary automake-1.11 automake-1.10 automake`
  if test x$AUTOMAKE = x; then
    die "Did not find a supported automake"
  fi
fi

if test x$AUTOCONF = x; then
  AUTOCONF=`locate_binary autoconf`
  if test x$AUTOCONF = x; then
    die "Did not find a supported autoconf"
  fi
fi

run $ACLOCAL $ACLOCAL_FLAGS || die "Can't execute aclocal"
run $AUTOMAKE $AUTOMAKE_FLAGS  || die "Can't execute automake"
run $AUTOCONF $AUTOCONF_FLAGS || die "Can't execute autoconf"

if test -f config/post_hook.sh
then
  . config/post_hook.sh
fi

echo "---"
echo "Configured with the following tools:"
echo "  * `$ACLOCAL --version | head -1`"
echo "  * `$AUTOMAKE --version | head -1`"
echo "  * `$AUTOCONF --version | head -1`"
echo "---"
