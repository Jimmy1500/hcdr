import DataCleaner as dc
import pandas as pd
import os

os.chdir("./data")

data_train_class = dc.DataCleaner("application_test.csv")
data_train = data_train_class.get_data()
profile_train = data_train_class.get_profile("/Users/FangzhouYu/Desktop/home-credit-default-risk/test_profile.html")
data_train_class.replace_missing_data("ORGANIZATION_TYPE", "XNA", "NA")
