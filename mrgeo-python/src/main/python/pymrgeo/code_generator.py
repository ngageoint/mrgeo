#
# Python Code Generator class
#
# Inspired by: http://effbot.org/zone/python-code-generator.htm
#
from __future__ import print_function

import string
import sys


class CodeGenerator(object):
    def __len__(self):
        return len(self._code)

    def __init__(self, tab="  "):
        self._tab = tab
        self._code = []
        self._level = 0

    def begin(self):
        self._code = []
        self._level = 0

    def generate(self):
        return string.join(self._code, "\n")

    def write(self, code, indent=False, unindent=False, post_indent=False, post_unindent=False):
        if indent:
            self.indent()
        if unindent:
            self.unindent()
        self._code.append(self._tab * self._level + code)
        if post_indent:
            self.indent()
        if post_unindent:
            self.unindent()

    def compile(self, method_name):

        code = self.generate()
        try:
            compiled = {}
            exec compile(code, method_name + ".py", "exec") in compiled
            # exec self.generate() in compiled
            return compiled
        except SyntaxError as se:
            ex_cls, ex, tb = sys.exc_info()

            # stack = traceback.extract_tb(tb)
            line = se.lineno

            print(ex_cls.__name__ + ' (' + str(se) + ')', file=sys.stderr)
            code = code.split("\n")
            cnt = 1
            for c in code:
                if cnt == line:
                    print('==> ' + c + ' <==', file=sys.stderr)
                else:
                    print('    ' + c, file=sys.stderr)
                cnt += 1

    def get_level(self):
        return self._level

    def force_level(self, level):
        self._level = level

    def append(self, other):
        for line in other._code:
            self.write(line)

    def indent(self, levels=1):
        self._level += levels

    def unindent(self, levels=1):
        if self._level >= levels:
            self._level -= levels
