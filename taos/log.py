import time
import os
import sys


# info
show_info  = True
show_warn  = True
show_debug = True

# set show
def setting(info, warn, debug):
    show_info = info
    show_warn = warn
    show_debug = debug


# info
def info(msg):
    if show_info:
        print(msg)

# warn
def warn(msg):
    if show_warn:
        print(msg)

# debug
def debug(msg):
    if show_debug:
        print(msg)
