
def gen_role(real_role, real_name,  new_prefix):
    def myrole(rtype, rawtext, text, lineno, inliner, options={}, content={}):

        # new_prefix=couchbase_core.bucket
        # text=upsert
        # rtype=cb_bmeth

        # e.g couchbase_core.bucket.upsert
        new_text = '{0}.{1}'.format(new_prefix, text)

        # e.g. :meth:`couchbase_core.bucket.upsert
        new_rawtext = ':{0}:`{1}`'.format(real_name, new_text)

        # e.g. py:meth
        new_type = 'py:' + real_name

        return real_role(new_type,
                         new_rawtext,
                         new_text,
                         lineno, inliner, options, content)
    return myrole


def on_inited(app):
    from sphinx.domains.python import PythonDomain as p
    fns = [
        ('cb_bmeth', '~couchbase_core.bucket.Bucket', 'meth'),
        ('cb_sdmeth', '~couchbase_core.subdocument', 'func'),
        ('cb_exc', 'couchbase_core.exceptions', 'exc')
    ]

    for newname, target, pyrole_name in fns:
        pyrole = p.roles[pyrole_name]
        app.add_role(newname, gen_role(pyrole, pyrole_name, target))


def setup(app):
    app.connect('builder-inited', on_inited)
