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
        self.started = time.time()
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
        now = time.time()
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
        now = time.time()
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
        self._println('%s (%s total)' % (msg, format_elapsed(time.time() - self.started)))


def get_progress(ctx):
    """Fetch the Progress reporter from the click context, creating a default one if required."""
    p = ctx.obj.get('PROGRESS')
    if not p:
        p = Progress(verbose=ctx.obj.get('VERBOSE', False))
        ctx.obj['PROGRESS'] = p
    return p
