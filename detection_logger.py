#!/bin/usr/env python3

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

stream_handler = logging.StreamHandler()
logger.addHandler(stream_handler)

file_handler = logging.FileHandler(__name__)
logger.addHandler(file_handler)