#!/usr/bin/env python3

"""Sync files between a computer and an Android device"""

from __future__ import annotations

from typing import List, Tuple, Union, Optional
from dataclasses import dataclass
import argparse
from pathlib import Path
import logging
import os
import stat
import fnmatch
import sys
import re
import shlex
import datetime
import subprocess


__version__ = "1.4.0"



class ColoredFormatter(logging.Formatter):
    """Logging Formatter to add colors"""

    fg_bright_blue    = "\x1b[94m"
    fg_yellow        = "\x1b[33m"
    fg_red           = "\x1b[31m"
    fg_bright_red_bold = "\x1b[91;1m"
    reset            = "\x1b[0m"

    def __init__(self, fmt, datefmt):
        super().__init__()
        self.messagefmt = fmt
        self.datefmt = datefmt

        self.formats = {
            logging.DEBUG:    "{}{}{}".format(self.fg_bright_blue, self.messagefmt, self.reset),
            logging.INFO:       "{}".format(self.messagefmt),
            logging.WARNING:  "{}{}{}".format(self.fg_yellow, self.messagefmt, self.reset),
            logging.ERROR:    "{}{}{}".format(self.fg_red, self.messagefmt, self.reset),
            logging.CRITICAL: "{}{}{}".format(self.fg_bright_red_bold, self.messagefmt, self.reset)
        }

        self.formatters = {
            logging.DEBUG:    logging.Formatter(self.formats[logging.DEBUG],    datefmt = self.datefmt),
            logging.INFO:     logging.Formatter(self.formats[logging.INFO],     datefmt = self.datefmt),
            logging.WARNING:  logging.Formatter(self.formats[logging.WARNING],  datefmt = self.datefmt),
            logging.ERROR:    logging.Formatter(self.formats[logging.ERROR],    datefmt = self.datefmt),
            logging.CRITICAL: logging.Formatter(self.formats[logging.CRITICAL], datefmt = self.datefmt)
        }

    def format(self, record):
        formatter = self.formatters[record.levelno]
        return formatter.format(record)

def setup_root_logger(
        no_color: bool = False,
        verbosity_level: int = 0,
        quietness_level: int = 0,
        messagefmt: str = "[%(asctime)s][%(levelname)s] %(message)s (%(filename)s:%(lineno)d)",
        messagefmt_verbose: str = "[%(asctime)s][%(levelname)s] %(message)s (%(filename)s:%(lineno)d)",
        datefmt: str = "%Y-%m-%d %H:%M:%S"
    ):
    messagefmt_to_use = messagefmt_verbose if verbosity_level else messagefmt
    logging_level = 10 * (2 + quietness_level - verbosity_level)
    if not no_color and sys.platform == "linux":
        formatter_class = ColoredFormatter
    else:
        formatter_class = logging.Formatter

    root_logger = logging.getLogger()
    root_logger.setLevel(logging_level)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter_class(fmt = messagefmt_to_use, datefmt = datefmt))
    root_logger.addHandler(console_handler)

def logging_fatal(message, log_stack_info: bool = True, exit_code: int = 1):
    logging.critical(message)
    logging.debug("Stack Trace", stack_info = log_stack_info)
    logging.critical("Exiting")
    raise SystemExit(exit_code)

def log_tree(title, tree, finals = None, log_leaves_types = True, logging_level = logging.INFO):
    """Log tree nicely if it is a dictionary.
    log_leaves_types can be False to log no leaves, True to log all leaves, or a tuple of types for which to log."""
    if finals is None:
        finals = []
    if not isinstance(tree, dict):
        logging.log(msg = "{}{}{}".format(
            "".join([" " if final else "│" for final in finals[:-1]] + ["└" if final else "├" for final in finals[-1:]]),
            title,
            ": {}".format(tree) if log_leaves_types is not False and (log_leaves_types is True or isinstance(tree, log_leaves_types)) else ""
        ), level = logging_level)
    else:
        logging.log(msg = "{}{}".format(
            "".join([" " if final else "│" for final in finals[:-1]] + ["└" if final else "├" for final in finals[-1:]]),
            title
        ), level = logging_level)
        tree_items = list(tree.items())
        for key, value in tree_items[:-1]:
            log_tree(key, value, finals = finals + [False], log_leaves_types = log_leaves_types, logging_level = logging_level)
        for key, value in tree_items[-1:]:
            log_tree(key, value, finals = finals + [True], log_leaves_types = log_leaves_types, logging_level = logging_level)

# like logging.CRITICAl, logging.DEBUG etc
FATAL = 60

def perror(s: Union[str, Any], e: Exception, logging_level: int = logging.ERROR):
    strerror = e.strerror if (isinstance(e, OSError) and e.strerror is not None) else e.__class__.__name__
    msg = f"{s}{': ' if s else ''}{strerror}"
    if logging_level == FATAL:
        logging_fatal(msg)
    else:
        logging.log(logging_level, msg)



class FileSystem():
    def __init__(self, adb_arguments: List[str]) -> None:
        self.adb_arguments = adb_arguments

    def _get_files_tree(self, tree_path: str, tree_path_stat: os.stat_result, follow_links: bool = False):
        # the reason to have two functions instead of one purely recursive one is to use self.lstat_in_dir ie ls
        # which is much faster than individually stat-ing each file. Hence we have get_files_tree's special first lstat
        if stat.S_ISLNK(tree_path_stat.st_mode):
            if not follow_links:
                logging.warning(f"Ignoring symlink {tree_path}")
                return None
            logging.debug(f"Following symlink {tree_path}")
            try:
                tree_path_realpath = self.realpath(tree_path)
                tree_path_stat_realpath = self.lstat(tree_path_realpath)
            except (FileNotFoundError, NotADirectoryError, PermissionError) as e:
                perror(f"Skipping symlink {tree_path}", e)
                return None
            return self._get_files_tree(tree_path_realpath, tree_path_stat_realpath, follow_links = follow_links)
        elif stat.S_ISDIR(tree_path_stat.st_mode):
            tree = {".": (60 * (int(tree_path_stat.st_atime) // 60), 60 * (int(tree_path_stat.st_mtime) // 60))}
            for filename, stat_object_child, in self.lstat_in_dir(tree_path):
                if filename in [".", ".."]:
                    continue
                tree[filename] = self._get_files_tree(
                    self.join(tree_path, filename),
                    stat_object_child,
                    follow_links = follow_links)
            return tree
        elif stat.S_ISREG(tree_path_stat.st_mode):
            return (60 * (int(tree_path_stat.st_atime) // 60), 60 * (int(tree_path_stat.st_mtime) // 60)) # minute resolution
        else:
            raise NotImplementedError

    def get_files_tree(self, tree_path: str, follow_links: bool = False):
        statObject = self.lstat(tree_path)
        return self._get_files_tree(tree_path, statObject, follow_links = follow_links)

    def remove_tree(self, tree_path: str, tree: Union[Tuple[int, int], dict], dry_run: bool = True) -> None:
        if isinstance(tree, tuple):
            logging.info(f"Removing {tree_path}")
            if not dry_run:
                self.unlink(tree_path)
        elif isinstance(tree, dict):
            remove_folder = tree.pop(".", False)
            for key, value in tree.items():
                self.remove_tree(self.normpath(self.join(tree_path, key)), value, dry_run = dry_run)
            if remove_folder:
                logging.info(f"Removing folder {tree_path}")
                if not dry_run:
                    self.rmdir(tree_path)
        else:
            raise NotImplementedError

    def push_tree_here(self,
        tree_path: str,
        relative_tree_path: str, # for logging paths of files / folders copied relative to the source root / destination root
                                 # nicely instead of repeating the root every time; rsync does this nice logging
        tree: Union[Tuple[int, int], dict],
        destination_root: str,
        fs_source: FileSystem,
        dry_run: bool = True,
        show_progress: bool = False
        ) -> None:
        if isinstance(tree, tuple):
            if dry_run:
                logging.info(f"{relative_tree_path}")
            else:
                if not show_progress:
                    # log this instead of letting adb display output
                    logging.info(f"{relative_tree_path}")
                self.push_file_here(tree_path, destination_root, show_progress = show_progress)
                self.utime(destination_root, tree)
        elif isinstance(tree, dict):
            try:
                tree.pop(".") # directory needs making
                logging.info(f"{relative_tree_path}{self.sep}")
                if not dry_run:
                    self.makedirs(destination_root)
            except KeyError:
                pass
            for key, value in tree.items():
                self.push_tree_here(
                    fs_source.normpath(fs_source.join(tree_path, key)),
                    fs_source.join(relative_tree_path, key),
                    value,
                    self.normpath(self.join(destination_root, key)),
                    fs_source,
                    dry_run = dry_run,
                    show_progress = show_progress
                )
        else:
            raise NotImplementedError

    # Abstract methods below implemented in Local.py and Android.py

    @property
    def sep(self) -> str:
        raise NotImplementedError

    def unlink(self, path: str) -> None:
        raise NotImplementedError

    def rmdir(self, path: str) -> None:
        raise NotImplementedError

    def makedirs(self, path: str) -> None:
        raise NotImplementedError

    def realpath(self, path: str) -> str:
        raise NotImplementedError

    def lstat(self, path: str) -> os.stat_result:
        raise NotImplementedError

    def lstat_in_dir(self, path: str) -> Iterable[Tuple[str, os.stat_result]]:
        raise NotImplementedError

    def utime(self, path: str, times: Tuple[int, int]) -> None:
        raise NotImplementedError

    def join(self, base: str, leaf: str) -> str:
        raise NotImplementedError

    def split(self, path: str) -> Tuple[str, str]:
        raise NotImplementedError

    def normpath(self, path: str) -> str:
        raise NotImplementedError

    def push_file_here(self, source: str, destination: str, show_progress: bool = False) -> None:
        raise NotImplementedError



class LocalFileSystem(FileSystem):
    @property
    def sep(self) -> str:
        return os.path.sep

    def unlink(self, path: str) -> None:
        os.unlink(path)

    def rmdir(self, path: str) -> None:
        os.rmdir(path)

    def makedirs(self, path: str) -> None:
        os.makedirs(path, exist_ok = True)

    def realpath(self, path: str) -> str:
        return os.path.realpath(path)

    def lstat(self, path: str) -> os.stat_result:
        return os.lstat(path)

    def lstat_in_dir(self, path: str) -> Iterable[Tuple[str, os.stat_result]]:
        for filename in os.listdir(path):
            yield filename, self.lstat(self.join(path, filename))

    def utime(self, path: str, times: Tuple[int, int]) -> None:
        os.utime(path, times)

    def join(self, base: str, leaf: str) -> str:
        return os.path.join(base, leaf)

    def split(self, path: str) -> Tuple[str, str]:
        return os.path.split(path)

    def normpath(self, path: str) -> str:
        return os.path.normpath(path)

    def push_file_here(self, source: str, destination: str, show_progress: bool = False) -> None:
        if show_progress:
            kwargs_call = {}
        else:
            kwargs_call = {
                "stdout": subprocess.DEVNULL,
                "stderr": subprocess.DEVNULL
            }
        if subprocess.call(self.adb_arguments + ["pull", source, destination], **kwargs_call):
            logging_fatal("Non-zero exit code from adb pull")



class AndroidFileSystem(FileSystem):
    RE_TESTCONNECTION_NO_DEVICE = re.compile("^adb\\: no devices/emulators found$")
    RE_TESTCONNECTION_DAEMON_NOT_RUNNING = re.compile("^\\* daemon not running; starting now at tcp:\\d+$")
    RE_TESTCONNECTION_DAEMON_STARTED = re.compile("^\\* daemon started successfully$")

    RE_LS_TO_STAT = re.compile(
        r"""^
        (?:
        (?P<S_IFREG> -) |
        (?P<S_IFBLK> b) |
        (?P<S_IFCHR> c) |
        (?P<S_IFDIR> d) |
        (?P<S_IFLNK> l) |
        (?P<S_IFIFO> p) |
        (?P<S_IFSOCK> s))
        [-r][-w][-xsS]
        [-r][-w][-xsS]
        [-r][-w][-xtT] # Mode string
        [ ]+
        (?:
        [0-9]+ # Number of hard links
        [ ]+
        )?
        [^ ]+ # User name/ID
        [ ]+
        [^ ]+ # Group name/ID
        [ ]+
        (?(S_IFBLK) [^ ]+[ ]+[^ ]+[ ]+) # Device numbers
        (?(S_IFCHR) [^ ]+[ ]+[^ ]+[ ]+) # Device numbers
        (?(S_IFDIR) (?P<dirsize>[0-9]+ [ ]+))? # Directory size
        (?(S_IFREG) (?P<st_size> [0-9]+) [ ]+) # Size
        (?(S_IFLNK) ([0-9]+) [ ]+) # Link length
        (?P<st_mtime>
        [0-9]{4}-[0-9]{2}-[0-9]{2} # Date
        [ ]
        [0-9]{2}:[0-9]{2}) # Time
        [ ]
        # Don't capture filename for symlinks (ambiguous).
        (?(S_IFLNK) .* | (?P<filename> .*))
        $""", re.DOTALL | re.VERBOSE)

    RE_NO_SUCH_FILE = re.compile("^.*: No such file or directory$")
    RE_LS_NOT_A_DIRECTORY = re.compile("ls: .*: Not a directory$")
    RE_TOTAL = re.compile("^total \\d+$")

    RE_REALPATH_NO_SUCH_FILE = re.compile("^realpath: .*: No such file or directory$")
    RE_REALPATH_NOT_A_DIRECTORY = re.compile("^realpath: .*: Not a directory$")

    ADBSYNC_END_OF_COMMAND = "ADBSYNC END OF COMMAND"

    def __init__(self, adb_arguments: List[str], adb_encoding: str) -> None:
        super().__init__(adb_arguments)
        self.adb_encoding = adb_encoding
        self.proc_adb_shell = subprocess.Popen(
            self.adb_arguments + ["shell"],
            stdin = subprocess.PIPE,
            stdout = subprocess.PIPE,
            stderr = subprocess.STDOUT
        )

    def __del__(self):
        self.proc_adb_shell.stdin.close()
        self.proc_adb_shell.wait()

    def adb_shell(self, commands: List[str]) -> Iterator[str]:
        self.proc_adb_shell.stdin.write(shlex.join(commands).encode(self.adb_encoding))
        self.proc_adb_shell.stdin.write(" </dev/null\n".encode(self.adb_encoding))
        self.proc_adb_shell.stdin.write(shlex.join(["echo", self.ADBSYNC_END_OF_COMMAND]).encode(self.adb_encoding))
        self.proc_adb_shell.stdin.write(" </dev/null\n".encode(self.adb_encoding))
        self.proc_adb_shell.stdin.flush()

        lines_to_yield: List[str] = []
        while adb_line := self.proc_adb_shell.stdout.readline():
            adb_line = adb_line.decode(self.adb_encoding).rstrip("\r\n")
            if adb_line == self.ADBSYNC_END_OF_COMMAND:
                break
            else:
                lines_to_yield.append(adb_line)
        for line in lines_to_yield:
            yield line

    def line_not_captured(self, line: str) -> NoReturn:
        logging.critical("ADB line not captured")
        logging_fatal(line)

    def test_connection(self):
        for line in self.adb_shell([":"]):
            print(line)

            if self.RE_TESTCONNECTION_DAEMON_NOT_RUNNING.fullmatch(line) or self.RE_TESTCONNECTION_DAEMON_STARTED.fullmatch(line):
                continue

            raise BrokenPipeError

    def ls_to_stat(self, line: str) -> Tuple[str, os.stat_result]:
        if self.RE_NO_SUCH_FILE.fullmatch(line):
            raise FileNotFoundError
        elif self.RE_LS_NOT_A_DIRECTORY.fullmatch(line):
            raise NotADirectoryError
        elif match := self.RE_LS_TO_STAT.fullmatch(line):
            match_groupdict = match.groupdict()
            st_mode = stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH # 755
            if match_groupdict['S_IFREG']:
                st_mode |= stat.S_IFREG
            if match_groupdict['S_IFBLK']:
                st_mode |= stat.S_IFBLK
            if match_groupdict['S_IFCHR']:
                st_mode |= stat.S_IFCHR
            if match_groupdict['S_IFDIR']:
                st_mode |= stat.S_IFDIR
            if match_groupdict['S_IFIFO']:
                st_mode |= stat.S_IFIFO
            if match_groupdict['S_IFLNK']:
                st_mode |= stat.S_IFLNK
            if match_groupdict['S_IFSOCK']:
                st_mode |= stat.S_IFSOCK
            st_size = None if match_groupdict["st_size"] is None else int(match_groupdict["st_size"])
            st_mtime = int(datetime.datetime.strptime(match_groupdict["st_mtime"], "%Y-%m-%d %H:%M").timestamp())

            # Fill the rest with dummy values.
            st_ino = 1
            st_rdev = 0
            st_nlink = 1
            st_uid = -2  # Nobody.
            st_gid = -2  # Nobody.
            st_atime = st_ctime = st_mtime

            return match_groupdict["filename"], os.stat_result((st_mode, st_ino, st_rdev, st_nlink, st_uid, st_gid, st_size, st_atime, st_mtime, st_ctime))
        else:
            self.line_not_captured(line)

    @property
    def sep(self) -> str:
        return "/"

    def unlink(self, path: str) -> None:
        for line in self.adb_shell(["rm", path]):
            self.line_not_captured(line)

    def rmdir(self, path: str) -> None:
        for line in self.adb_shell(["rm", "-r", path]):
            self.line_not_captured(line)

    def makedirs(self, path: str) -> None:
        for line in self.adb_shell(["mkdir", "-p", path]):
            self.line_not_captured(line)

    def realpath(self, path: str) -> str:
        for line in self.adb_shell(["realpath", path]):
            if self.RE_REALPATH_NO_SUCH_FILE.fullmatch(line):
                raise FileNotFoundError
            elif self.RE_REALPATH_NOT_A_DIRECTORY.fullmatch(line):
                raise NotADirectoryError
            else:
                return line
            # permission error possible?

    def lstat(self, path: str) -> os.stat_result:
        for line in self.adb_shell(["ls", "-lad", path]):
            return self.ls_to_stat(line)[1]

    def lstat_in_dir(self, path: str) -> Iterable[Tuple[str, os.stat_result]]:
        for line in self.adb_shell(["ls", "-la", path]):
            if self.RE_TOTAL.fullmatch(line):
                continue
            else:
                yield self.ls_to_stat(line)

    def utime(self, path: str, times: Tuple[int, int]) -> None:
        atime = datetime.datetime.fromtimestamp(times[0]).strftime("%Y%m%d%H%M")
        mtime = datetime.datetime.fromtimestamp(times[1]).strftime("%Y%m%d%H%M")
        for line in self.adb_shell(["touch", "-at", atime, "-mt", mtime, path]):
            self.line_not_captured(line)

    def join(self, base: str, leaf: str) -> str:
        return os.path.join(base, leaf).replace("\\", "/") # for Windows

    def split(self, path: str) -> Tuple[str, str]:
        head, tail = os.path.split(path)
        return head.replace("\\", "/"), tail # for Windows

    def normpath(self, path: str) -> str:
        return os.path.normpath(path).replace("\\", "/")

    def push_file_here(self, source: str, destination: str, show_progress: bool = False) -> None:
        if show_progress:
            kwargs_call = {}
        else:
            kwargs_call = {
                "stdout": subprocess.DEVNULL,
                "stderr": subprocess.DEVNULL
            }
        if subprocess.call(self.adb_arguments + ["push", source, destination], **kwargs_call):
            logging_fatal("Non-zero exit code from adb push")


@dataclass
class Args():
    logging_no_color: bool
    logging_verbosity_verbose: int
    logging_verbosity_quiet: int

    dry_run: bool
    copy_links: bool
    exclude: List[str]
    exclude_from: List[Path]
    delete: bool
    delete_excluded: bool
    force: bool
    show_progress: bool
    adb_encoding: str

    adb_bin: str
    adb_flags: List[str]
    adb_options: List[List[str]]

    direction: str

    direction_push_local: Optional[str]
    direction_push_android: Optional[str]

    direction_pull_android: Optional[str]
    direction_pull_local: Optional[str]

def get_cli_args(docstring: str, version: str) -> Args:
    parser = argparse.ArgumentParser(description = docstring)
    parser.add_argument("--version",
        action = "version",
        version = version
    )

    parser_logging = parser.add_argument_group(title = "logging")
    parser_logging.add_argument("--no-color",
        help = "Disable colored logging (Linux only)",
        action = "store_true",
        dest = "logging_no_color"
    )
    parser_logging_verbosity = parser_logging.add_mutually_exclusive_group(required = False)
    parser_logging_verbosity.add_argument("-v", "--verbose",
        help = "Increase logging verbosity: -v for debug",
        action = "count",
        dest = "logging_verbosity_verbose",
        default = 0
    )
    parser_logging_verbosity.add_argument("-q", "--quiet",
        help = "Decrease logging verbosity: -q for warning, -qq for error, -qqq for critical, -qqqq for no logging messages",
        action = "count",
        dest = "logging_verbosity_quiet",
        default = 0
    )

    parser.add_argument("-n", "--dry-run",
        help = "Perform a dry run; do not actually copy and delete etc",
        action = "store_true",
        dest = "dry_run"
    )
    parser.add_argument("-L", "--copy-links",
        help = "Follow symlinks and copy their referent file / directory",
        action = "store_true",
        dest = "copy_links"
    )
    parser.add_argument("--exclude",
        help = "fnmatch pattern to ignore relative to source (reusable)",
        action = "append",
        dest = "exclude",
        default = []
    )
    parser.add_argument("--exclude-from",
        help = "Filename of file containing fnmatch patterns to ignore relative to source (reusable)",
        metavar = "EXCLUDE_FROM",
        type = Path,
        action = "append",
        dest = "exclude_from",
        default = []
    )
    parser.add_argument("--del",
        help = "Delete files at the destination that are not in the source",
        action = "store_true",
        dest = "delete"
    )
    parser.add_argument("--delete-excluded",
        help = "Delete files at the destination that are excluded",
        action = "store_true",
        dest = "delete_excluded"
    )
    parser.add_argument("--force",
        help = "Allows files to overwrite folders and folders to overwrite files. This is false by default to prevent large scale accidents",
        action = "store_true",
        dest = "force"
    )
    parser.add_argument("--show-progress",
        help = "Show progress from 'adb push' and 'adb pull' commands",
        action = "store_true",
        dest = "show_progress"
    )
    parser.add_argument("--adb-encoding",
        help = "Which encoding to use when talking to adb. Defaults to UTF-8. Relevant to GitHub issue #22",
        dest = "adb_encoding",
        default = "UTF-8"
    )

    parser_adb = parser.add_argument_group(title = "ADB arguments",
        description = "By default ADB works for me without touching any of these, but if you have any specific demands then go ahead. See 'adb --help' for a full list of adb flags and options"
    )
    parser_adb.add_argument("--adb-bin",
        help = "Use the given adb binary. Defaults to 'adb' ie whatever is on path",
        dest = "adb_bin",
        default = "adb")
    parser_adb.add_argument("--adb-flag",
        help = "Add a flag to call adb with, eg '--adb-flag d' for adb -d, that is return an error if more than one device is connected",
        metavar = "ADB_FLAG",
        action = "append",
        dest = "adb_flags",
        default = []
    )
    parser_adb.add_argument("--adb-option",
        help = "Add an option to call adb with, eg '--adb-option P 5037' for adb -P 5037, that is use port 5037 for the adb server",
        metavar = ("OPTION", "VALUE"),
        nargs = 2,
        action = "append",
        dest = "adb_options",
        default = []
    )

    parser_direction = parser.add_subparsers(title = "direction",
        dest = "direction",
        required = True
    )

    parser_direction_push = parser_direction.add_parser("push",
        help = "Push from computer to phone"
    )
    parser_direction_push.add_argument("direction_push_local",
        metavar = "LOCAL",
        help = "Local path"
    )
    parser_direction_push.add_argument("direction_push_android",
        metavar = "ANDROID",
        help = "Android path"
    )

    parser_direction_pull = parser_direction.add_parser("pull",
        help = "Pull from phone to computer"
    )
    parser_direction_pull.add_argument("direction_pull_android",
        metavar = "ANDROID",
        help = "Android path"
    )
    parser_direction_pull.add_argument("direction_pull_local",
        metavar = "LOCAL",
        help = "Local path"
    )

    args = parser.parse_args()

    if args.direction == "push":
        args_direction_ = (
            args.direction_push_local,
            args.direction_push_android,
            None,
            None
        )
    else:
        args_direction_ = (
            None,
            None,
            args.direction_pull_android,
            args.direction_pull_local
        )

    args = Args(
        args.logging_no_color,
        args.logging_verbosity_verbose,
        args.logging_verbosity_quiet,

        args.dry_run,
        args.copy_links,
        args.exclude,
        args.exclude_from,
        args.delete,
        args.delete_excluded,
        args.force,
        args.show_progress,
        args.adb_encoding,

        args.adb_bin,
        args.adb_flags,
        args.adb_options,

        args.direction,
        *args_direction_
    )

    return args



class FileSyncer():
    @classmethod
    def diff_trees(cls,
        source: Union[dict, Tuple[int, int], None],
        destination: Union[dict, Tuple[int, int], None],
        path_source: str,
        path_destination: str,
        destination_exclude_patterns: List[str],
        path_join_function_source,
        path_join_function_destination,
        folder_file_overwrite_error: bool = True,
        ) -> Tuple[
            Union[dict, Tuple[int, int], None], # delete
            Union[dict, Tuple[int, int], None], # copy
            Union[dict, Tuple[int, int], None], # excluded_source
            Union[dict, Tuple[int, int], None], # unaccounted_destination
            Union[dict, Tuple[int, int], None]  # excluded_destination
        ]:

        exclude = False
        for destination_exclude_pattern in destination_exclude_patterns:
            if fnmatch.fnmatch(path_destination, destination_exclude_pattern):
                exclude = True
                break

        if source is None:
            if destination is None:
                delete = None
                copy = None
                excluded_source = None
                unaccounted_destination = None
                excluded_destination = None
            elif isinstance(destination, tuple):
                if exclude:
                    delete = None
                    copy = None
                    excluded_source = None
                    unaccounted_destination = None
                    excluded_destination = destination
                else:
                    delete = None
                    copy = None
                    excluded_source = None
                    unaccounted_destination = destination
                    excluded_destination = None
            elif isinstance(destination, dict):
                if exclude:
                    delete = {".": None}
                    copy = None
                    excluded_source = None
                    unaccounted_destination = {".": None}
                    excluded_destination = destination
                else:
                    delete = {".": None}
                    copy = None
                    excluded_source = None
                    unaccounted_destination = {".": destination["."]}
                    excluded_destination = {".": None}
                    destination.pop(".")
                    for key, value in destination.items():
                        delete[key], _, _, unaccounted_destination[key], excluded_destination[key] = cls.diff_trees(
                            None,
                            value,
                            path_join_function_source(path_source, key),
                            path_join_function_destination(path_destination, key),
                            destination_exclude_patterns,
                            path_join_function_source,
                            path_join_function_destination,
                            folder_file_overwrite_error = folder_file_overwrite_error
                        )
            else:
                raise NotImplementedError

        elif isinstance(source, tuple):
            if destination is None:
                if exclude:
                    delete = None
                    copy = None
                    excluded_source = source
                    unaccounted_destination = None
                    excluded_destination = None
                else:
                    delete = None
                    copy = source
                    excluded_source = None
                    unaccounted_destination = None
                    excluded_destination = None
            elif isinstance(destination, tuple):
                if exclude:
                    delete = None
                    copy = None
                    excluded_source = source
                    unaccounted_destination = None
                    excluded_destination = destination
                else:
                    if source[1] > destination[1]:
                        delete = destination
                        copy = source
                        excluded_source = None
                        unaccounted_destination = None
                        excluded_destination = None
                    else:
                        delete = None
                        copy = None
                        excluded_source = None
                        unaccounted_destination = None
                        excluded_destination = None
            elif isinstance(destination, dict):
                if exclude:
                    delete = {".": None}
                    copy = None
                    excluded_source = source
                    unaccounted_destination = {".": None}
                    excluded_destination = destination
                else:
                    delete = destination
                    copy = source
                    excluded_source = None
                    unaccounted_destination = {".": None}
                    excluded_destination = {".": None}
                    if folder_file_overwrite_error:
                        logging.critical(f"Refusing to overwrite directory {path_destination} with file {path_source}")
                        logging_fatal("Use --force if you are sure!")
                    else:
                        logging.warning(f"Overwriting directory {path_destination} with file {path_source}")
            else:
                raise NotImplementedError

        elif isinstance(source, dict):
            if destination is None:
                if exclude:
                    delete = None
                    copy = {".": None}
                    excluded_source = source
                    unaccounted_destination = None
                    excluded_destination = None
                else:
                    delete = None
                    copy = {".": source["."]}
                    excluded_source = {".": None}
                    unaccounted_destination = None
                    excluded_destination = None
                    source.pop(".")
                    for key, value in source.items():
                        _, copy[key], excluded_source[key], _, _ = cls.diff_trees(
                            value,
                            None,
                            path_join_function_source(path_source, key),
                            path_join_function_destination(path_destination, key),
                            destination_exclude_patterns,
                            path_join_function_source,
                            path_join_function_destination,
                            folder_file_overwrite_error = folder_file_overwrite_error
                        )
            elif isinstance(destination, tuple):
                if exclude:
                    delete = None
                    copy = {".": None}
                    excluded_source = source
                    unaccounted_destination = None
                    excluded_destination = destination
                else:
                    delete = destination
                    copy = {".": source["."]}
                    excluded_source = {".": None}
                    unaccounted_destination = None
                    excluded_destination = None
                    source.pop(".")
                    for key, value in source.items():
                        _, copy[key], excluded_source[key], _, _ = cls.diff_trees(
                            value,
                            None,
                            path_join_function_source(path_source, key),
                            path_join_function_destination(path_destination, key),
                            destination_exclude_patterns,
                            path_join_function_source,
                            path_join_function_destination,
                            folder_file_overwrite_error = folder_file_overwrite_error
                        )
                    if folder_file_overwrite_error:
                        logging.critical(f"Refusing to overwrite file {path_destination} with directory {path_source}")
                        logging_fatal("Use --force if you are sure!")
                    else:
                        logging.warning(f"Overwriting file {path_destination} with directory {path_source}")
                excluded_destination = None
            elif isinstance(destination, dict):
                if exclude:
                    delete = {".": None}
                    copy = {".": None}
                    excluded_source = source
                    unaccounted_destination = {".": None}
                    excluded_destination = destination
                else:
                    delete = {".": None}
                    copy = {".": None}
                    excluded_source = {".": None}
                    unaccounted_destination = {".": None}
                    excluded_destination = {".": None}
                    source.pop(".")
                    for key, value in source.items():
                        delete[key], copy[key], excluded_source[key], unaccounted_destination[key], excluded_destination[key] = cls.diff_trees(
                            value,
                            destination.pop(key, None),
                            path_join_function_source(path_source, key),
                            path_join_function_destination(path_destination, key),
                            destination_exclude_patterns,
                            path_join_function_source,
                            path_join_function_destination,
                            folder_file_overwrite_error = folder_file_overwrite_error
                        )
                    destination.pop(".")
                    for key, value in destination.items():
                        delete[key], _, _, unaccounted_destination[key], excluded_destination[key] = cls.diff_trees(
                            None,
                            value,
                            path_join_function_source(path_source, key),
                            path_join_function_destination(path_destination, key),
                            destination_exclude_patterns,
                            path_join_function_source,
                            path_join_function_destination,
                            folder_file_overwrite_error = folder_file_overwrite_error
                        )
            else:
                raise NotImplementedError

        else:
            raise NotImplementedError

        return delete, copy, excluded_source, unaccounted_destination, excluded_destination

    @classmethod
    def remove_excluded_folders_from_unaccounted_tree(cls, unaccounted: Union[dict, Tuple[int, int]], excluded: Union[dict, None]) -> dict:
        # For when we have --del but not --delete-excluded selected; we do not want to delete unaccounted folders that are the
        # parent of excluded items. At the point in the program that this function is called at either
        # 1) unaccounted is a tuple (file) and excluded is None
        # 2) unaccounted is a dict and excluded is a dict or None
        # trees passed to this function are already pruned; empty dictionary (sub)trees don't exist
        if excluded is None:
            return unaccounted
        else:
            unaccounted_non_excluded = {}
            for unaccounted_key, unaccounted_value in unaccounted.items():
                if unaccounted_key == ".":
                    continue
                unaccounted_non_excluded[unaccounted_key] = cls.remove_excluded_folders_from_unaccounted_tree(
                    unaccounted_value,
                    excluded.get(unaccounted_key, None)
                )
            return unaccounted_non_excluded

    @classmethod
    def prune_tree(cls, tree):
        """Remove all Nones from a tree. May return None if tree is None however."""
        if not isinstance(tree, dict):
            return tree
        else:
            return_dict = {}
            for key, value in tree.items():
                value_pruned = cls.prune_tree(value)
                if value_pruned is not None:
                    return_dict[key] = value_pruned
            return return_dict or None

    @classmethod
    def sort_tree(cls, tree):
        if not isinstance(tree, dict):
            return tree
        return {
            k: cls.sort_tree(v)
            for k, v in sorted(tree.items())
        }

    @classmethod
    def paths_to_fixed_destination_paths(cls,
        path_source: str,
        fs_source: FileSystem,
        path_destination: str,
        fs_destination: FileSystem
    ) -> Tuple[str, str]:
        """Modify sync paths according to how a trailing slash on the source path should be treated"""
        # TODO I'm not exactly sure if this covers source and destination being symlinks (lstat vs stat etc)
        # we only need to consider when the destination is a directory
        try:
            lstat_destination = fs_destination.lstat(path_destination)
        except FileNotFoundError:
            return path_source, path_destination
        except (NotADirectoryError, PermissionError) as e:
            perror(path_source, e, FATAL)

        if stat.S_ISLNK(lstat_destination.st_mode):
            logging_fatal("Destination is a symlink. Not sure what to do. See GitHub issue #8")

        if not stat.S_ISDIR(lstat_destination.st_mode):
            return path_source, path_destination

        # we know the destination is a directory at this point
        try:
            lstat_source = fs_source.lstat(path_source)
        except FileNotFoundError:
            return path_source, path_destination
        except (NotADirectoryError, PermissionError) as e:
            perror(path_source, e, FATAL)

        if stat.S_ISREG(lstat_source.st_mode) or (stat.S_ISDIR(lstat_source.st_mode) and path_source[-1] not in ["/", "\\"]):
            path_destination = fs_destination.join(
                path_destination,
                fs_destination.split(path_source)[1]
            )
        return path_source, path_destination

def main():
    args = get_cli_args(__doc__, __version__)

    setup_root_logger(
        no_color = args.logging_no_color,
        verbosity_level = args.logging_verbosity_verbose,
        quietness_level = args.logging_verbosity_quiet,
        messagefmt = "[%(levelname)s] %(message)s" if os.name == "nt" else "%(message)s"
    )

    for exclude_from_pathname in args.exclude_from:
        with exclude_from_pathname.open("r") as f:
            args.exclude.extend(line for line in f.read().splitlines() if line)

    adb_arguments = [args.adb_bin] + [f"-{arg}" for arg in args.adb_flags]
    for option, value in args.adb_options:
        adb_arguments.append(f"-{option}")
        adb_arguments.append(value)

    fs_android = AndroidFileSystem(adb_arguments, args.adb_encoding)
    fs_local = LocalFileSystem(adb_arguments)

    try:
        fs_android.test_connection()
    except BrokenPipeError:
        logging_fatal("Connection test failed")

    if args.direction == "push":
        path_source = args.direction_push_local
        fs_source = fs_local
        path_destination = args.direction_push_android
        fs_destination = fs_android
    else:
        path_source = args.direction_pull_android
        fs_source = fs_android
        path_destination = args.direction_pull_local
        fs_destination = fs_local

    path_source, path_destination = FileSyncer.paths_to_fixed_destination_paths(path_source, fs_source, path_destination, fs_destination)

    path_source = fs_source.normpath(path_source)
    path_destination = fs_destination.normpath(path_destination)

    try:
        files_tree_source = fs_source.get_files_tree(path_source, follow_links = args.copy_links)
    except (FileNotFoundError, NotADirectoryError, PermissionError) as e:
        perror(path_source, e, FATAL)

    try:
        files_tree_destination = fs_destination.get_files_tree(path_destination, follow_links = args.copy_links)
    except FileNotFoundError:
        files_tree_destination = None
    except (NotADirectoryError, PermissionError) as e:
        perror(path_destination, e, FATAL)

    logging.info("Source tree:")
    if files_tree_source is not None:
        log_tree(path_source, files_tree_source)
    logging.info("")

    logging.info("Destination tree:")
    if files_tree_destination is not None:
        log_tree(path_destination, files_tree_destination)
    logging.info("")

    if isinstance(files_tree_source, dict):
        excludePatterns = [fs_destination.normpath(
            fs_destination.join(path_destination, exclude)
        ) for exclude in args.exclude]
    else:
        excludePatterns = [fs_destination.normpath(
            path_destination + exclude
        ) for exclude in args.exclude]
    logging.debug("Exclude patterns:")
    logging.debug(excludePatterns)
    logging.debug("")

    tree_delete, tree_copy, tree_excluded_source, tree_unaccounted_destination, tree_excluded_destination = FileSyncer.diff_trees(
        files_tree_source,
        files_tree_destination,
        path_source,
        path_destination,
        excludePatterns,
        fs_source.join,
        fs_destination.join,
        folder_file_overwrite_error = not args.dry_run and not args.force
    )

    tree_delete                  = FileSyncer.prune_tree(tree_delete)
    tree_copy                    = FileSyncer.prune_tree(tree_copy)
    tree_excluded_source         = FileSyncer.prune_tree(tree_excluded_source)
    tree_unaccounted_destination = FileSyncer.prune_tree(tree_unaccounted_destination)
    tree_excluded_destination    = FileSyncer.prune_tree(tree_excluded_destination)

    tree_delete                  = FileSyncer.sort_tree(tree_delete)
    tree_copy                    = FileSyncer.sort_tree(tree_copy)
    tree_excluded_source         = FileSyncer.sort_tree(tree_excluded_source)
    tree_unaccounted_destination = FileSyncer.sort_tree(tree_unaccounted_destination)
    tree_excluded_destination    = FileSyncer.sort_tree(tree_excluded_destination)

    logging.info("Delete tree:")
    if tree_delete is not None:
        log_tree(path_destination, tree_delete, log_leaves_types = False)
    logging.info("")

    logging.info("Copy tree:")
    if tree_copy is not None:
        log_tree(f"{path_source} --> {path_destination}", tree_copy, log_leaves_types = False)
    logging.info("")

    logging.info("Source excluded tree:")
    if tree_excluded_source is not None:
        log_tree(path_source, tree_excluded_source, log_leaves_types = False)
    logging.info("")

    logging.info("Destination unaccounted tree:")
    if tree_unaccounted_destination is not None:
        log_tree(path_destination, tree_unaccounted_destination, log_leaves_types = False)
    logging.info("")

    logging.info("Destination excluded tree:")
    if tree_excluded_destination is not None:
        log_tree(path_destination, tree_excluded_destination, log_leaves_types = False)
    logging.info("")


    tree_unaccounted_destination_non_excluded = None
    if tree_unaccounted_destination is not None:
        tree_unaccounted_destination_non_excluded = FileSyncer.prune_tree(
            FileSyncer.remove_excluded_folders_from_unaccounted_tree(
                tree_unaccounted_destination,
                tree_excluded_destination
            )
        )

    logging.info("Non-excluded-supporting destination unaccounted tree:")
    if tree_unaccounted_destination_non_excluded is not None:
        log_tree(path_destination, tree_unaccounted_destination_non_excluded, log_leaves_types = False)
    logging.info("")

    logging.info("SYNCING")
    logging.info("")

    if tree_delete is not None:
        logging.info("Deleting delete tree")
        fs_destination.remove_tree(path_destination, tree_delete, dry_run = args.dry_run)
    else:
        logging.info("Empty delete tree")
    logging.info("")

    if args.delete_excluded and args.delete:
        if tree_excluded_destination is not None:
            logging.info("Deleting destination excluded tree")
            fs_destination.remove_tree(path_destination, tree_excluded_destination, dry_run = args.dry_run)
        else:
            logging.info("Empty destination excluded tree")
        logging.info("")
        if tree_unaccounted_destination is not None:
            logging.info("Deleting destination unaccounted tree")
            fs_destination.remove_tree(path_destination, tree_unaccounted_destination, dry_run = args.dry_run)
        else:
            logging.info("Empty destination unaccounted tree")
        logging.info("")
    elif args.delete_excluded:
        if tree_excluded_destination is not None:
            logging.info("Deleting destination excluded tree")
            fs_destination.remove_tree(path_destination, tree_excluded_destination, dry_run = args.dry_run)
        else:
            logging.info("Empty destination excluded tree")
        logging.info("")
    elif args.delete:
        if tree_unaccounted_destination_non_excluded is not None:
            logging.info("Deleting non-excluded-supporting destination unaccounted tree")
            fs_destination.remove_tree(path_destination, tree_unaccounted_destination_non_excluded, dry_run = args.dry_run)
        else:
            logging.info("Empty non-excluded-supporting destination unaccounted tree")
        logging.info("")

    if tree_copy is not None:
        logging.info("Copying copy tree")
        fs_destination.push_tree_here(
            path_source,
            fs_destination.split(path_source)[1] if isinstance(tree_copy, tuple) else ".",
            tree_copy,
            path_destination,
            fs_source,
            dry_run = args.dry_run,
            show_progress = args.show_progress
        )
    else:
        logging.info("Empty copy tree")
    logging.info("")


if __name__ == "__main__": main()
