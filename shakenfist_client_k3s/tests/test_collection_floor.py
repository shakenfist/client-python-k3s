"""The collection's floor on this package covers what its module calls.

collection/requirements.txt names a static minimum shakenfist_client_k3s
for the sf_k3s_cluster module, and its comment says to raise that floor
in the commit which first makes the module call something newer. That is
a rule a person has to remember, and the failure when they do not is
silent here and loud somewhere else: every test in this repository runs
the module against the working tree, so nothing fails until a user with
the oldest permitted release installed runs a play and gets an
AttributeError out of a module which claimed to support it.

So the rule is checked rather than remembered. The module's source is
parsed -- with ast, so a comment or a string which mentions a name is
not mistaken for a use of it -- and every attribute it reaches through
one of its shakenfist_client_k3s imports is looked up in FIRST_SHIPPED
below, the release which first carried it. The floor has to be at least
the newest of those.

What this does not see: methods called on a Cluster instance, and the
keyword arguments passed to anything. Both reach the library through a
value rather than through an import alias, and following them needs type
inference this test does not attempt. Today every one of them is older
than every symbol in the table, so the gap is theoretical; a module
change which starts calling a new Cluster method, or passing a new
keyword, still has to raise the floor by hand.
"""
import ast
import os
import re

import testtools


_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))
MODULE = os.path.join(_REPO_ROOT, 'collection', 'plugins', 'modules',
                      'sf_k3s_cluster.py')
REQUIREMENTS = os.path.join(_REPO_ROOT, 'collection', 'requirements.txt')
PACKAGE = 'shakenfist_client_k3s'

# Every attribute the module reaches through a shakenfist_client_k3s
# import, keyed as '<submodule>.<name>', against the first released
# version of the package which defined it. Established by reading the
# tagged source -- `git show v0.2.0:shakenfist_client_k3s/cluster.py` and
# so on -- not by assumption: a symbol present at neither v0.1.0 nor
# v0.2.0 is listed against the release it will first ship in.
FIRST_SHIPPED = {
    'client.make_client': (0, 1, 0),
    'cluster.Cluster': (0, 1, 0),
    'cluster.validate_create_arguments': (0, 3, 0),
    'cluster.validate_create_counts': (0, 3, 0),
    'exceptions.ClusterNameError': (0, 3, 0),
    'exceptions.K3sClusterException': (0, 1, 0),
    'exceptions.ShapeError': (0, 3, 0),
    'progress.CollectingReporter': (0, 1, 0),
}


def _version(text):
    return tuple(int(part) for part in text.split('.'))


def _dotted(version):
    return '.'.join(str(part) for part in version)


def library_references(source):
    """Return the set of '<submodule>.<name>' the module source uses.

    Only the import form the module uses is understood --
    ``from shakenfist_client_k3s import <submodule> as <alias>`` -- and
    any other way of importing the package is an error rather than
    something to skip, because a reference this cannot see is a reference
    the floor is not checked against. The same goes for an alias used
    other than as ``alias.name``: passed to getattr() or handed to another
    function, it could reach anything.
    """
    tree = ast.parse(source)
    aliases = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for name in node.names:
                if name.name.split('.')[0] == PACKAGE:
                    raise AssertionError(
                        'The module imports %s as "import %s". Use "from %s '
                        'import <submodule> as <alias>", which this test '
                        'knows how to follow.' % (PACKAGE, name.name, PACKAGE))
        elif isinstance(node, ast.ImportFrom) and node.module and \
                node.module.split('.')[0] == PACKAGE:
            if node.module != PACKAGE:
                raise AssertionError(
                    'The module imports names from %s directly. Use "from '
                    '%s import <submodule> as <alias>", which this test '
                    'knows how to follow.' % (node.module, PACKAGE))
            for name in node.names:
                aliases[name.asname or name.name] = name.name

    referenced = set()
    attribute_values = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and \
                isinstance(node.value, ast.Name) and \
                node.value.id in aliases:
            referenced.add('%s.%s' % (aliases[node.value.id], node.attr))
            attribute_values.add(id(node.value))
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id in aliases and \
                id(node) not in attribute_values:
            raise AssertionError(
                'Line %d uses %s other than as %s.<name>, so this test '
                'cannot tell what it reaches.' % (node.lineno, node.id,
                                                  node.id))
    return referenced


def requirements_floor(text):
    """Return the version floor requirements.txt names for this package."""
    found = re.findall(r'^%s>=([0-9]+(?:\.[0-9]+)*)\s*$' % PACKAGE, text,
                       re.MULTILINE)
    if len(found) != 1:
        raise AssertionError(
            'Expected exactly one "%s>=X.Y.Z" line in %s, found %d'
            % (PACKAGE, REQUIREMENTS, len(found)))
    return _version(found[0])


class CollectionFloorTestCase(testtools.TestCase):

    def setUp(self):
        super().setUp()
        if not os.path.exists(MODULE):
            self.skipTest(
                'collection/ is not part of an installed package, so the '
                'module cannot be located')
        with open(MODULE, encoding='utf-8') as f:
            self.source = f.read()
        with open(REQUIREMENTS, encoding='utf-8') as f:
            self.requirements = f.read()

    def test_every_referenced_symbol_has_a_known_version(self):
        missing = sorted(library_references(self.source) -
                         set(FIRST_SHIPPED))
        self.assertEqual(
            [], missing,
            'sf_k3s_cluster.py uses %s, which FIRST_SHIPPED in %s does not '
            'list. Add each with the first release that ships it (check '
            'with `git show vX.Y.Z:%s/<file>.py`; if no tag has it, the '
            'next release), and raise the floor in '
            'collection/requirements.txt if that is newer than it.'
            % (', '.join(missing), os.path.basename(__file__), PACKAGE))

    def test_the_floor_covers_every_referenced_symbol(self):
        floor = requirements_floor(self.requirements)
        too_new = sorted(
            '%s (%s)' % (symbol, _dotted(FIRST_SHIPPED[symbol]))
            for symbol in library_references(self.source)
            if symbol in FIRST_SHIPPED and FIRST_SHIPPED[symbol] > floor)
        self.assertEqual(
            [], too_new,
            'collection/requirements.txt says %s>=%s, but sf_k3s_cluster.py '
            'uses %s. Raise the floor to the newest of those, and update '
            'the comment above it and docs/collection.md to match.'
            % (PACKAGE, _dotted(floor), ', '.join(too_new)))

    def test_the_table_has_no_stale_entries(self):
        # A symbol the module stopped using leaves an entry behind which
        # makes the table look like a record of the module when it is not.
        stale = sorted(set(FIRST_SHIPPED) - library_references(self.source))
        self.assertEqual(
            [], stale,
            'FIRST_SHIPPED lists %s, which sf_k3s_cluster.py no longer '
            'uses. Remove them.' % ', '.join(stale))

    def test_the_parser_sees_through_the_aliases(self):
        # The checks above pass vacuously if the parse finds nothing, so
        # this pins that it finds a reference made through each import.
        refs = library_references(
            'from shakenfist_client_k3s import cluster as c\n'
            'from shakenfist_client_k3s import progress\n'
            'c.Cluster()\n'
            'progress.CollectingReporter\n'
            '# c.NotACall\n'
            "x = 'c.NotAnAttribute'\n")
        self.assertEqual({'cluster.Cluster', 'progress.CollectingReporter'},
                         refs)

    def test_an_import_it_cannot_follow_is_refused(self):
        for source in (
                'from shakenfist_client_k3s.cluster import Cluster\n',
                'import shakenfist_client_k3s.cluster\n',
                'from shakenfist_client_k3s import cluster as c\n'
                'getattr(c, "Cluster")\n'):
            self.assertRaises(AssertionError, library_references, source)
