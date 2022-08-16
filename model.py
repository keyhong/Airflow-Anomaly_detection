from typing import TypeVar
pandas_DataFrame = TypeVar('pandas.core.frame.DataFrame')
numpy_ndarray = TypeVar('pandas.ndarray')

import pandas as pd
import numpy as np

from sklearn.cluster import MeanShift
from collections import Counter

class MeanShiftModel:

    def __init__(self):

        self.options = {}
        self.training_data = None
        self.is_traing_data = False
        self.is_hyper_params = False
        self.model = None


    def initialize_model(self):

        # initialize model, hyper parameter
        self.options = {}
        self.training_data = None
        self.is_traing_data = False
        self.is_hyper_params = False
        self.model = None

        return self


    def set_training_data(self, input_data: np.ndarray):
        
        if isinstance(input_data, (np.ndarray, pd.DataFrame, pd.Series)):

            # initialize training_dataz
            if len(input_data) == 1 or 1 < input_data.ndim:
                self.training_data = input_data
            else:
                self.training_data = self._change_2d_arr(input_data)

            self.is_traing_data = True
        else:
            return ValueError("input_data should be numpy.ndarray or pandas.DataFrame.")
        
        return 1


    def clustering(self):

        # if hyper_params are not exist, set hyper_params
        if not self.is_traing_data:
            raise Exception("first set training_data before using this function")
        
        if not self.is_hyper_params:
            self.options['bandwidth'] = self.training_data.std()
            self.is_hyper_params = True
        
        self.model = MeanShift(**self.options)
        self.model.fit(self.training_data)

        return self.model.labels__
        

    def get_fitting_lables(self):
        return self.model.labels_

        
    @staticmethod
    def _change_2d_arr(input_data):

        X = input_data.values

        # 알고리즘 사용을 위해 1차원 추가
        _ = np.zeros(len(X))

        # 행렬 전치
        changed_2d_arr = np.matrix([X, _]).transpose()

        return changed_2d_arr