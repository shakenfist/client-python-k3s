import re
import shutil
import sys
import time


# How often, in seconds, line mode reprints an unchanged status so that
# logs still show liveness during long waits.
HEARTBEAT_INTERVAL = 60


def format_elapsed(seconds):
    """Format a duration in seconds as a short human readable string."""
    seconds = int(seconds)
    if seconds < 60:
        return '%ds' % seconds
    if seconds < 3600:
        return '%dm%02ds' % (seconds // 60, seconds % 60)
    return '%dh%02dm' % (seconds // 3600, (seconds % 3600) // 60)


def count_str(count, noun):
    """Return a count with a naively pluralised noun, such as '2 instances'."""
    if count == 1:
        return '1 %s' % noun
    if noun.endswith('s') or noun.endswith('x'):
        return '%d %ses' % (count, noun)
    return '%d %ss' % (count, noun)


# The names of environment variable assignments whose values are secrets
# when they appear on a command line this package builds. Agent commands
# run through a shell on the guest, so a credential the command needs has
# to be in the command line -- and the API echoes that command line back,
# which is how it reaches an exception message. Adding a name here is the
# whole of what is needed to cover a new credential; nothing else has to
# be found and changed.
SECRET_ENVIRONMENT_NAMES = ('K3S_TOKEN',)

REDACTED = '<redacted>'

# A value, as it appears on a command line: a run of non-whitespace,
# except that a quoted section may contain whitespace. That covers both
# forms shlex.quote() produces -- a bare word when the value needs no
# quoting, which is what a k3s token normally is, and a single quoted
# string when it does, including the '"'"' idiom shlex uses for an
# embedded quote, where the three adjacent quoted sections match in turn.
# Written as two literals so that neither quote character needs escaping.
_VALUE_RE = "(?:'[^']*'" '|"[^"]*"' r"|[^\s])*"

# One assignment, matched from the name to the end of its value. The
# lookbehind is so that SOME_OTHER_K3S_TOKEN is left alone rather than
# rewritten as SOME_OTHER_<redacted>.
_SECRET_ASSIGNMENT_RE = re.compile(
    r'(?<![A-Za-z0-9_])(%s)=%s'
    % ('|'.join(SECRET_ENVIRONMENT_NAMES), _VALUE_RE))


def redact_command_line(text):
    """Replace the value of every secret environment assignment in text.

    Called from AgentOperationError and CommandFailedError's constructors
    and from describe_agent_op(), rather than from the places which raise
    or print, so that the dangerous text is never stored in the first
    place and a rendering site added later cannot forget to ask. That is
    the same reasoning the two shell rules at the top of cluster.py are
    stated under: a rule, because the next reader cannot be expected to
    re-derive which values are attacker reachable.

    Non-string input is returned unchanged, because the fields this is
    applied to are whatever the API put in them.
    """
    if not isinstance(text, str):
        return text
    return _SECRET_ASSIGNMENT_RE.sub(r'\1=' + REDACTED, text)


def redact_structure(value):
    """Apply redact_command_line() through a nested dict or list of strings.

    Agent operation results are a dict of per command stdout and stderr,
    so a command which echoes its own invocation -- a shell started with
    set -x, an installer printing what it is about to run -- puts the
    credential in there too. Walking the structure costs a line and
    removes the need to reason about what any particular installer prints.
    """
    if isinstance(value, dict):
        return dict((k, redact_structure(v)) for k, v in value.items())
    if isinstance(value, list):
        return [redact_structure(v) for v in value]
    return redact_command_line(value)


def describe_agent_op(aop, max_len=60):
    """Return a short human readable description of the command an agent operation is up to."""
    commands = aop.get('commands', [])
    results = aop.get('results', {}) or {}

    # Results are recorded per command index as they complete, so the number
    # of results is the index of the currently executing command.
    idx = min(len(results), len(commands) - 1)
    if idx < 0:
        return None

    c = commands[idx]
    desc = c.get('commandline')
    if not desc:
        desc = c.get('command', 'unknown')
        if c.get('path'):
            desc += ' %s' % c['path']

    # Before the first line and length reductions below, so that what they
    # shorten is already safe to show. await_idle() passes the default
    # max_len, which happens to truncate before the token in the one
    # command line that carries one -- a margin nothing states or tests,
    # and _agent_op_error() disables it with max_len=None anyway.
    desc = redact_command_line(desc)

    # Multi-line commands (for example heredocs) would break the one line
    # per item status display, so describe them by their first line.
    if '\n' in desc:
        desc = desc.split('\n', 1)[0] + ' ...'

    if max_len and len(desc) > max_len:
        desc = desc[:max_len - 3] + '...'
    return desc


class Reporter:
    """Where library output goes, and whether debug output is emitted.

    The command line wants output on stdout as it happens, and a library
    caller (an Ansible module, say) wants it collected and handed back, so
    the destination is an object the caller chooses rather than a bare
    print(). Reporters are file like -- write(), flush() and isatty() --
    because Progress already writes to a stream, so a reporter can be
    passed as that stream and the library keeps a single output channel
    rather than two.

    This default implementation is backed by the real sys.stdout. It looks
    the stream up on each call rather than caching it at construction so
    that code which replaces sys.stdout after a reporter is built (the
    Click test runner, contextlib.redirect_stdout) still sees the output.
    """

    def __init__(self, verbose=False):
        self.verbose = verbose

    def write(self, text):
        return sys.stdout.write(text)

    def flush(self):
        sys.stdout.flush()

    def isatty(self):
        """Report whether the underlying stream is a terminal.

        Progress chooses between in place ANSI updates and line mode from
        this, so answering without asking the real stream would silently
        change the format of every long running command's output.
        """
        return sys.stdout.isatty()

    def debug(self, msg):
        """Emit a debug line, but only when verbose output was requested."""
        if self.verbose:
            self.write('%s\n' % msg)
            self.flush()


class CollectingReporter(Reporter):
    """A reporter which accumulates output for its caller rather than printing it.

    Written text is kept as arrived and split into lines only when asked
    for, because a line can reach a stream in several writes: print()
    writes its text and its newline separately, and Progress._println()
    appends the newline itself. Splitting is on '\\n' alone, rather than
    with splitlines(), so that a form feed or an exotic unicode separator
    inside captured command output cannot invent a line boundary -- one
    element of lines is exactly one line of what would have been printed.
    A trailing incomplete line is returned rather than dropped, so nothing
    written is ever lost from the list.

    isatty() is False: a collector has no terminal to move a cursor
    around, and Progress must therefore use its line mode.
    """

    def __init__(self, verbose=False):
        super(CollectingReporter, self).__init__(verbose=verbose)
        self._chunks = []

    def write(self, text):
        self._chunks.append(text)
        return len(text)

    def flush(self):
        pass

    def isatty(self):
        return False

    def getvalue(self):
        """Return everything written, as a single string."""
        return ''.join(self._chunks)

    @property
    def lines(self):
        """Return everything written, as a list of lines with no line endings."""
        lines = self.getvalue().split('\n')
        if lines and lines[-1] == '':
            lines.pop()
        return lines


class Progress:
    """Phase and wait-loop progress reporting for long running commands.

    Output has two modes. When stdout is a TTY (and we are not in verbose
    mode, whose debug lines would interleave badly), wait loop statuses are
    rendered as one line per item, updated in place with ANSI cursor
    movement. Otherwise statuses are printed only when they change, with a
    periodic heartbeat so logs written from CI or a pipe still show
    liveness.
    """

    def __init__(self, total_phases=None, verbose=False, stream=None):
        self.stream = stream if stream is not None else sys.stdout
        self.total_phases = total_phases
        self.interactive = not verbose and self.stream.isatty()

        # time.monotonic() throughout this module rather than time.time().
        # Every reading taken here is subtracted from another one to produce
        # an elapsed time, and none of them is ever recorded or compared
        # across processes, so a wall clock buys nothing and can step: an
        # ntp correction during a long install would otherwise make a phase
        # appear to take a negative amount of time, or an hour.
        self.started = time.monotonic()
        self.phase_index = 0

        # Per wait block state: item key -> (status, time the status first
        # appeared, time last printed)
        self._statuses = {}
        self._rendered_lines = 0

    def _println(self, msg):
        self.stream.write(msg + '\n')
        self.stream.flush()

    def phase(self, name):
        """Start a new named phase, printing a numbered header once."""
        self.wait_done()
        self.phase_index += 1
        if self.total_phases:
            self._println('[%d/%d] %s' % (self.phase_index, self.total_phases, name))
        else:
            self._println('[%d] %s' % (self.phase_index, name))

    def note(self, msg):
        """Print a one-off informational line within the current phase.

        Notes can arrive in the middle of a wait block (for example the
        stall warning), so the wait state must be preserved: discarding
        it would reset the per-item elapsed timers, destroying the very
        signal a stall note exists to highlight.
        """
        if self.interactive and self._rendered_lines:
            # Print the note where the status block currently starts, then
            # redraw the block below it.
            self.stream.write('\x1b[%dF' % self._rendered_lines)
            self.stream.write('\x1b[K  %s\n' % msg)
            self.stream.flush()
            self._rendered_lines = 0
            self._render_block()
            return
        self._println('  %s' % msg)

    def update(self, key, status):
        """Report the current status of one item within a wait loop.

        The elapsed time shown against each item is how long the item has
        had its current status, so a stalled command is visible as a
        growing elapsed time.
        """
        now = time.monotonic()
        prev = self._statuses.get(key)
        since = prev[1] if prev and prev[0] == status else now

        if self.interactive:
            self._statuses[key] = (status, since, now)
            self._render_block()
            return

        if prev and prev[0] == status and now - prev[2] < HEARTBEAT_INTERVAL:
            return
        self._statuses[key] = (status, since, now)
        self._println('  %s: %s (%s)' % (key, status, format_elapsed(now - since)))

    def _render_block(self):
        columns = shutil.get_terminal_size().columns
        now = time.monotonic()
        lines = []
        for key, (status, since, _) in self._statuses.items():
            line = '  %s: %s (%s)' % (key, status, format_elapsed(now - since))
            lines.append(line[:columns - 1])

        out = ''
        if self._rendered_lines:
            out += '\x1b[%dF' % self._rendered_lines
        for line in lines:
            out += '\x1b[K%s\n' % line
        self.stream.write(out)
        self.stream.flush()
        self._rendered_lines = len(lines)

    def wait_done(self):
        """End the current wait block.

        In interactive mode the final statuses remain on screen as history;
        the next wait block renders below them.
        """
        self._statuses = {}
        self._rendered_lines = 0

    def finish(self, msg):
        """Print a completion line with the total elapsed time."""
        self.wait_done()
        self._println('%s (%s total)' % (msg, format_elapsed(time.monotonic() - self.started)))
