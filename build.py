#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@Project ：taos-connector-python 
@File ：build.py
@Author ：hadrianl
@Date ：2023/8/10 14:05 
"""

from Cython.Build import cythonize
from setuptools import Extension
from setuptools.command.build_ext import build_ext
import platform

compiler_directives = {"language_level": 3, "embedsignature": True}


def build(setup_kwargs):
    if platform.system() == "Linux":
        extensions = [
            Extension("taos._cinterface", ["taos/_cinterface.pyx"],
                      libraries=["taos"],
                      )
        ]
    elif platform.system() == "Windows":
        extensions = [
            Extension("taos._cinterface", ["taos/_cinterface.pyx"],
                      libraries=["taos"],
                      include_dirs=[r"C:\TDengine\include"],
                      library_dirs=[r"C:\TDengine\driver"],
                      )
        ]
    else:
        raise Exception("unsupported platform")

    setup_kwargs.update({
        "ext_modules": cythonize(extensions, compiler_directives=compiler_directives, nthreads=5, force=True),
        "cmdclass": {"build_ext": build_ext},
    })

