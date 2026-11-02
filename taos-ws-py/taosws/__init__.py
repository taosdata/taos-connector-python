# Copyright (c) 2026 TAOS Data, Inc.
#
# SPDX-License-Identifier: MIT
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import importlib
import importlib.util
import pkgutil
import sys


__path__ = pkgutil.extend_path(__path__, __name__)


def _load_native_module():
    full_name = f"{__name__}._taosws"
    if importlib.util.find_spec(full_name) is not None:
        return importlib.import_module(full_name)

    raise ImportError(
        "Failed to import native extension 'taosws._taosws'. " "Ensure taos-ws-py is built and installed correctly."
    )


_native = _load_native_module()

sys.modules.setdefault(f"{__name__}.taosws", _native)

if hasattr(_native, "__all__"):
    for name in _native.__all__:
        globals()[name] = getattr(_native, name)
else:
    for name in dir(_native):
        if not name.startswith("_"):
            globals()[name] = getattr(_native, name)

__doc__ = _native.__doc__
if hasattr(_native, "__all__"):
    __all__ = _native.__all__
