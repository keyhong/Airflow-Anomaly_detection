#!/bin/usr/env python3

import re

__all__ = ['RegexMixin']

class RegexMixin(object):
    
    def __init__(self):
        self.__uptimeTime_pattern = re.compile('[0-9].:[0-9].:[0-9].')
        self.__mpstatTime_pattern = re.compile('[0-9].:[0-9].:[0-9]. [A-Z].')
        self.__airflowTime_pattern = re.compile('[0-9]*-[0-9]*-[0-9]* [0-9]*:[0-9]*:[0-9]*')
        self.__loadAverage_pattern = re.compile('load average: [^a-zA-Z]*')
        self.__idle_pattern = re.compile('[0-9]*.[0-9]*\n')
        self.__fileDate_pattern = re.compile('[0-9]*-[0-9]*-[0-9]*')
    
    @property
    def uptimeTime(self):
        return self.__uptimeTime_pattern

    @property
    def mpstatTime(self):
        return self.__mpstatTime_pattern
    
    @property
    def airflowTime(self):
        return self.__airflowTime_pattern

    @property
    def loadAverage(self):
        return self.__loadAverage_pattern
    
    @property
    def idle(self):
        return self.__idle_pattern

    @property
    def fileDate(self):
        return self.__fileDate_pattern