import pandas as pd
import numpy as np
import pandas_profiling as pdf


class DataCleaner(object):

    def __init__(self, file_path, test_data_path="NULL"):
        self.data = pd.read_csv(file_path)
        self.original_data = self.data
        if test_data_path == "NULL":
            self.test_data = []
            self.original_test_data = []
        else:
            self.test_data = pd.read_csv(test_data_path)
            self.original_test_data = self.test_data

    def get_data(self):
        return self.data

    def get_original_data(self):
        return self.original_data

    def get_test_data(self):
        return self.test_data

    def get_original_test_data(self):
        return self.original_test_data

    def get_profile(self, profile_location, input_data="NULL"):
        if input_data == "NULL":
            profile = pdf.ProfileReport(self.data)
        else:
            profile = pdf.ProfileReport(input_data)
        profile.to_file(outputfile=profile_location)

    # use "NA" to replace all the missing data
    def replace_missing_data(self, variable_name, current_value, data_indicator=0, target_value="NA"):
        if data_indicator == 0:
            self.data[variable_name] = np.where(
                self.data[variable_name] == current_value, target_value, self.data[variable_name])
        else:
            self.test_data[variable_name] = np.where(
                self.data[variable_name] == current_value, target_value, self.data[variable_name])

    # Missing data imputation
    def impute_missing_data(self, variable, method, data_indicator=0, new_variable="NULL", value=0):
        # numerical: zero
        if new_variable == "NULL":
            new_variable = variable

        # numerical: median (Mean/median imputation has the assumption that the
        # data are missing completely at random (MCAR))
        if method == "median":
            temp_variable = self[variable].fillna(
                self.data[variable].median())

        # numerical: mean
        if method == "mean":
            temp_variable = self[
                variable].fillna(self.data[variable].mean())

        if method == "mode":
            temp_variable = self[
                variable].fillna(self.data[variable].mode())

        # numerical: end of distribution value (data are not missing completely at
        # random)
        if method == "eod":
            extreme = self.data[variable].mean() + 3 * \
                self.data[variable].std()
            temp_variable = self[variable].fillna(extreme)

        # numerical: arbitrary value(data are not missing completely at random)
        if method == "arbitrary":
            temp_variable = self[variable].fillna(value)

        # numerical/categorical: create variable indicating missingness (data are
        # not missing completely at random)
        if method == "binary":
            temp_variable = np.where(
                self.data[variable].isnull(), 1, 0)

        # numerical/categorical: frequency category (the data are missing
        # completely at random (MCAR))
        if method == "frequency":
            most_frequent_category = self.data.groupby(
                [variable])[variable].count().sort_values(ascending=False).index[0]
            temp_variable = self.data[variable].fillna(
                most_frequent_category, inplace=False)

        # categorical: create a category indicating missingness (no assumption)
        if method == "na category":
            temp_variable = np.where(
                self.data[variable].isnull(), "Missing", self.data[variable])

        if data_indicator == 0:
            self.data[new_variable] = temp_variable
        else:
            self.test_data[new_variable] = temp_variable

            # numerical/categorical: random sampling (Random sample imputation assumes
        # that the data are missing completely at random (MCAR))
        if method == "random":
            # extract the random sample to fill the na
            # pandas needs to have the same index in order to merge datasets
            if data_indicator == 0:
                data_sample = self.data[variable].dropna().sample(self.data[variable].isnull().sum(), random_state=0)
                data_sample.index = self.data[self.data[variable].isnull()].index
                self.data.loc[self.data[variable].isnull(), new_variable] = data_sample
            else:
                data_sample = self.data[variable].dropna().sample(self.test_data[variable].isnull().sum(), random_state=0)
                data_sample.index = self.test_data[self.test_data[variable].isnull()].index
                self.test_data.loc[self.test_data[variable].isnull(), new_variable] = data_sample

        # delete whole row
        if method == "drop":
            if data_indicator == 0:
                self.data[pd.notnull(self.data[variable])]
            else:
                self.test_data[pd.notnull(self.data[variable])]
