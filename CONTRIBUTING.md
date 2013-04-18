We've decided to use "Gerrit" for our code review system, making it
easier for all of us to contribute with code and comments.

  1. Visit http://review.couchbase.org and "Register" for an account
  2. Review http://review.couchbase.org/static/individual_agreement.html
  3. Agree to agreement by visiting http://review.couchbase.org/#/settings/agreements
  4. If you do not receive an email, please contact us
  5. Check out the Python SDK area http://review.couchbase.org/#/q/status:open+project:couchbase-python-client,n,z
  6. Join us on IRC at #libcouchbase on Freenode :-)

We normally don't go looking for stuff in gerrit, so you should add at
least me (volker.mische@gmail.com) as a reviewer for your patch (and
I'll know who else to add and add them for you).


## Contributing Using Repo Tool

If you haven't done so already you should
download the repo from http://code.google.com/p/git-repo/downloads/list
and put it in your path.

All you should need to set up your development environment should be:

    ~$ mkdir sdk
    ~$ cd sdk
    ~/sdk$ repo init -u git://github.com/vmx/manifest.git -m sdks/python.xml
    ~/sdk$ repo sync
    ~/sdk$ repo start my-branch-name --all
    ~/sdk$ cd python
    ~/sdk/python$ python setup.py build_ext --inplace

You can work in the branch just as in any other git branch. Once you
are happy with your changes commit them as usual. Every commit will
show up as separate change within Gerrit, so you might want to squash
your commits into a single one before you upload them to gerrit with
the following command:

    ~/sdk/python$ repo upload

You might experience a problem trying to upload the patches if you've
selected a different login name at review.couchbase.org than your login
name. Don't worry, all you need to do is to add the following to your
~/.gitconfig file:

    [review "review.couchbase.org"]
            username = your-gerrit-username


## Contributing Using Plain Git

If you not so familiar with repo tool and its workflow there is an
alternative way to do the same job. Just complete the GGerrit
registration steps above and clone the source repository
(remember the repository on github.com is just a mirror):

    ~/sdk$ git clone ssh://YOURNAME@review.couchbase.org:29418/couchbase-python-client.git

Install [`commit-msg` hook][1]:

    ~/sdk$ cd couchbase-python-client
    ~/sdk/couchbase-python-client$ scp -p -P 29418 YOURNAME@review.couchbase.org:hooks/commit-msg .git/hooks/

Make your changes and upload them for review:

    ~/sdk/couchbase-python-client$ git commit
    ~/sdk/couchbase-python-client$ git push origin HEAD:refs/for/master

If you need to fix or add something to your patch, do it and re-upload
the changes (all you need is to keep `Change-Id:` line the same to
allow gerrit to track the patch.

    ~/couchbase-python-client % git commit --amend
    ~/couchbase-python-client % git push origin HEAD:refs/for/master

Happy hacking!


[1]: http://review.couchbase.org/Documentation/user-changeid.html
