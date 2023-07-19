from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd
import numpy as np

class CustomEncoder(BaseEstimator, TransformerMixin):
    """Perform one-hot enconding using pandas.get_dummies method"""
    def __init__(self):
        pass

    def fit(self, data, y=None):
        return self

    def transform(self, data, y=None):
        categorical = data.select_dtypes(exclude=['int64', 'float64', 'datetime64']).columns.tolist()
        encoded_df = pd.get_dummies(data, columns=categorical, drop_first=True)
        #print("encoded df columns: ", encoded_df.columns)
        return encoded_df

    def get_params(self, data):
        categorical = data.select_dtypes(exclude=['int64', 'float64', 'datetime64']).columns.tolist()
        encoded_df = pd.get_dummies(cat_df, columns=categorical, drop_first=True)
        return encoded_df.columns

class DropNa(BaseEstimator, TransformerMixin):
    """Drop NaNs from dataframes. Optionally provide columns to drop.
    Default axis=0 to drop only rows with NaNs rather than columns."""
    def __init__(self, columns = None, axis=0):
        self.columns = columns
        self.axis = axis

    def fit(self, data, y=None):
        return self

    def transform(self, data):
        df = pd.DataFrame(data)
        df.dropna(axis=self.axis, inplace=True)
        return df

class DropCols(BaseEstimator, TransformerMixin):
    def __init__(self, columns):
        self.columns = columns

    def fit(self, data, y=None):
        return self

    def transform(self, data, y=None):
        try:
            data.drop(columns = self.columns, axis=1, inplace=True)
            return data
        except KeyError:
            cols_error = list(set(self.columns) - set(data.columns))
            raise KeyError("The DataFrame does not include the columns: %s" % cols_error)

class ColumnSelector(BaseEstimator, TransformerMixin):
    """Selects only passed in columns within the dataframe.
    Expects a list of columns as input."""
    def __init__(self, columns):
        self.columns = columns

    def fit(self, data, y=None):
        return self

    def transform(self, data, y=None):
        try:
            selected = data[self.columns]
            return selected
        except KeyError:
            cols_error = list(set(self.columns) - set(data.columns))
            raise KeyError("The DataFrame does not include the columns: %s" % cols_error)

class AssignJobs(BaseEstimator, TransformerMixin):
    """Assign names of jobs to the critical jobs in the data set.
    Predefines job name column as "Name"""
    def __init__(self, critical_list, col='Name'):
        self.critical_jobs = critical_list
        self.col = col

    def fit(self, data, y=None):
        return self

    def transform(self, data):
        data['job_name'] = 'NaN'
        for idx, job in enumerate(self.critical_jobs):
            filt = data[data[self.col].str.contains(self.critical_jobs[idx])]
            #print("job: ", job + "filt name: ", filt['Name'])
            indx = filt.index.values
            data.job_name.iloc[indx] = job
        return data

class DateTimeConverter(BaseEstimator, TransformerMixin):
    """Convert columns to DateTime64 data type"""
    def __init__(self, columns):
        self.columns = columns

    def fit(self, data, y=None):
        return self

    def transform(self, data, y=None):
        for i,j in zip(self.columns, self.columns):
            data[j] = data[i].astype('datetime64', inplace=True)
        return data

class Lookup(BaseEstimator, TransformerMixin):
    """Append average duration column based on lookup file.
    Join variable specifies the column to join on. Column variable
    specifies which column to include the data dataframe."""
    def __init__(self, lookup, join, column):
        self.lookup = lookup
        self.join = join
        self.column = column

    def fit(self, data, y=None):
        return self

    def transform(self, data, y=None):
        joined = data.join(self.lookup.set_index(self.join)[self.column], on=self.join, rsuffix='_right')
        return joined


class CalculateFeatures(BaseEstimator, TransformerMixin):
    """Calculate features of jobs based on list of input columns
    e.g duration = j_starttime, j_endtime."""
    def __init__(self, columns):
        self.columns = columns

    def fit(self, data, y=None):
        return self

    def transform(self, data, y=None):
        # Calculate core utilisation
        data['core_utilization'] = (data['CoresInUse'] / (data['CoresInUse'] + data['AvailableCores']))*100
        # Calculate duration based on columns list
        data['duration_min'] = (data[self.columns[0]] - data[self.columns[1]])/np.timedelta64(1, 'm')
        # Calculate task progress percentage based on number of total and finished tasks
        data['task_progress'] = (data['Finished']/data['Total'])*100
        # Calculate average time to run a task based on total number of tasks and average duration of job
        data['avg_task_time'] = data['avg_duration_min'] / data['Total']
        # Calculate base prediction based on average task time and total number of tasks
        data['base_pred_min'] = data['avg_task_time'] * data['Total']
        # Calculate current runtime of all tasks finished so far
        data['current_task_runtime'] = data['avg_task_time'] * data['Finished']
        # Calculate the residual time for the job based on average duration and current task runtime
        data['residual_task_time'] = data['avg_duration_min'] - data['current_task_runtime']
        # Calculate time elapsed based on start time of job and latest finished task time
        data['time_elapsed_min'] = (data['latest_update'] - data['j_starttime'])/np.timedelta64(1, 'm')
        # Calculate core utilisation based on AvailableCores and CoresInUse

        return data
