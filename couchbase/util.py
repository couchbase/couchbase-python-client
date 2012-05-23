
import sys


class ProgressBar:
    """from http://code.activestate.com/recipes/168639-progress-bar-class/
    License: http://www.opensource.org/licenses/PythonSoftFoundation.php
    """
    def __init__(self, minValue=0, maxValue=10, totalWidth=12):
        self.progBar = "[]"   # This holds the progress bar string
        self.min = minValue
        self.max = maxValue
        self.span = maxValue - minValue
        self.width = totalWidth
        self.amount = 0       # When amount == max, we are 100% done
        self.updateAmount(0)  # Build progress bar string

    def updateAmount(self, newAmount=0):
        if newAmount < self.min:
            newAmount = self.min
        if newAmount > self.max:
            newAmount = self.max
        self.amount = newAmount

        # Figure out the new percent done, round to an integer
        diffFromMin = float(self.amount - self.min)
        percentDone = (diffFromMin / float(self.span)) * 100.0
        percentDone = round(percentDone)
        percentDone = int(percentDone)

        # Figure out how many hash bars the percentage should be
        allFull = self.width - 2
        numHashes = (percentDone / 100.0) * allFull
        numHashes = int(round(numHashes))

        # build a progress bar with hashes and spaces
        self.progBar = ("[" + '#' * numHashes + ' ' * (allFull - numHashes)
                        + "]")

        # figure out where to put the percentage, roughly centered
        percentPlace = (len(self.progBar) / 2) - len(str(percentDone))
        percentString = str(percentDone) + "%"

        # slice the percentage into the bar
        self.progBar = (self.progBar[0:percentPlace] + percentString
                        + self.progBar[percentPlace + len(percentString):])

    def __str__(self):
        return str(self.progBar)

    def draw(self):
        """from
        http://code.activestate.com/recipes/168639-progress-bar-class/#c6
        Draw progress bar - but only if it has changed
        """
        if self.pbar_str != self._old_pbar:
            self._old_pbar = self.pbar_str
            sys.stdout.write(self.pbar_str + '\r')
            sys.stdout.flush()      # force updating of screen


class StringUtil(object):

    @staticmethod
    def create_value(pattern, size):
        return ((pattern * (size / len(pattern)))
                + pattern[0:(size % len(pattern))])
