from os import getcwd, path, chdir, devnull, system
from distutils.dir_util import remove_tree, mkpath, copy_tree
from subprocess import call

def clean_path(dir_name):
    if path.exists(dir_name):
        remove_tree(dir_name)

def check_path(dir_name):
    if not path.exists(dir_name):
        return False
    else:
        return True

def create_path(dir_name):
    if not path.exists(dir_name):
        mkpath(dir_name)

def call_cmd(line, verbose=False):
    if verbose:
        print line
        return call(line)
    else:
        return call(line, stdout=dnull)

def cp(root_src, root_dest, rel_path):
    print "Copying %s from %s to %s" % (rel_path, root_src, root_dest)
    if not path.isdir(path.join(root_src, rel_path)):
        print "Warning: %s doesn't exist!" % path.join(root_src, rel_path)
    else:
        copy_tree(path.join(root_src, rel_path), path.join(root_dest, rel_path))

class DataParser(object):
    def __init__(self):
        self.parsed_str = ""
        self._callback = {
            "parsed": None
        }

    def set_callback(self, name, cb):
        self._callback[name] = cb


class SerialParser(DataParser):
    def __init__(self):
        super(SerialParser, self).__init__()

    def parse_data(self, data):
        for ch in data:
            if ch == '\n':
                if self._callback["parsed"] is not None:
                    self._callback["parsed"](self.parsed_str)
                    self.parsed_str = ""
            else:
                self.parsed_str += ch


class JsonParser(DataParser):
    def __init__(self):
        self._parse_level = 0
        self._in_str = False
        self._esc_char = False

    def parse_data(self, data):
        done = False
        for ch in data:
            if ch == '\"':
                if not self._in_str:
                    self._in_str = True
                else:
                    if not self._esc_char:
                        self._in_str = False
                    else:
                        self._esc_char = False

            if self._in_str:
                if ch == "\\":
                    self._esc_char = True
            else:
                if ch == '{':
                    if self._parse_level == 0:
                        self.parsed_str = ""
                    self._parse_level += 1
                elif ch == '}':
                    self._parse_level -= 1
                    if self._parse_level == 0:
                        done = True

            self.parsed_str += ch

            if done and self._callback["parsed"] is not None:
                self._callback["parsed"](self.parsed_str)