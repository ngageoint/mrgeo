#
# Python Code Generator class
#
# Inspired by: http://effbot.org/zone/python-code-generator.htm
#
import string


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
        compiled = {}
        exec compile(self.generate(), method_name + ".py", "exec") in compiled
        #exec self.generate() in compiled
        return compiled

    def append(self, other):
        for line in other._code:
            self.write(line)

    def indent(self, levels=1):
        self._level += levels

    def unindent(self, levels=1):
        if self._level > levels:
            self._level -= levels