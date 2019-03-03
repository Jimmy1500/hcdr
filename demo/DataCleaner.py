import pandas as pd
import numpy as np
import pandas_profiling as pdf


class DataCleaner(object):

    def __init__(self, file_path):
        self.data = pd.read_csv(file_path)
        self.orginal_data = self.data

    def get_data(self):
        return self.data

    def get_profile(self, profile_location, input_data="NULL"):
        if input_data == "NULL":
            profile = pdf.ProfileReport(self.data)
        else:
            profile = pdf.ProfileReport(input_data)
        profile.to_file(outputfile=profile_location)

    # use "NA" to replace all the missing data
    def replace_missing_data(self, variable_name, current_value, target_value="NA"):
        self.data[variable_name] = np.where(
            self.data[variable_name] == current_value, target_value, self.data[variable_name])

    # Missing data imputation
    def impute_missing_data(self, variable, method, new_variable="NULL", value=0):
        # numerical: zero
        if new_variable == "NULL":
            new_variable = variable

        # numerical: median (Mean/median imputation has the assumption that the
        # data are missing completely at random (MCAR))
        if method == "median":
            self.data[new_variable] = self[variable].fillna(self.data[variable].median())

        # numerical: mean
        if method == "mean":
            self.data[new_variable] = self[variable].fillna(self.data[variable].mean())

        if method == "mode":
            self.data[new_variable] = self[variable].fillna(self.data[variable].mode())

        #delete whole row
        if method == "drop":
            self.data[pd.notnull(self.data[variable])]

        # numerical: end of distribution value (data are not missing completely at
        # random)
        if method == "eod":
            extreme = self.data[variable].mean() + 3 * \
                self.data[variable].std()
            self.data[new_variable] = self[variable].fillna(extreme)

        # numerical: arbitrary value(data are not missing completely at random)
        if method == "arbitrary":
            self.data[new_variable] = self[variable].fillna(value)

        # numerical/categorical: random sampling (Random sample imputation assumes
        # that the data are missing completely at random (MCAR))
        if method == "random":
            # extract the random sample to fill the na
            random_sample_train = self.data[variable].dropna().sample(
                self.data[variable].isnull().sum(), random_state=0)

            # pandas needs to have the same index in order to merge datasets
            random_sample_train.index = self.data[
                self.data[variable].isnull()].index

            self.data.loc[self.data[variable].isnull(
            ), new_variable] = random_sample_train

        # numerical/categorical: create variable indicating missingness (data are
        # not missing completely at random)
        if method == "binary":
            self.data[new_variable] = np.where(
                self.data[variable].isnull(), 1, 0)

        # numerical/categorical: frequency category (the data are missing
        # completely at random (MCAR))
        if method == "frequency":
            most_frequent_category = self.data.groupby(
                [variable])[variable].count().sort_values(ascending=False).index[0]
            self.data[new_variable] = self.data[variable].fillna(
                most_frequent_category, inplace=False)

        # categorical: create a category indicating missingness (no assumption)
        if method == "na category":
            self.data[new_variable] = np.where(
                self.data[variable].isnull(), "Missing", self.data[variable])
