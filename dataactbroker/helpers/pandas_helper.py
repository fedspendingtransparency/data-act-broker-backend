import pandas as pd

from dataactcore.interfaces.db import GlobalDB


def check_dataframe_diff(new_data, model, id_col, sort_cols, lambda_funcs=None):
    """ Checks if 2 dataframes (the new data and the existing data for a model) are different.

        Args:
            new_data: dataframe containing the new data to compare
            model: The model to get the existing data from
            id_col: A string containing the name of the ID column to delete from the existing data
            sort_cols: An array containing the columns to sort on
            lambda_funcs: A dict with the column to update as the key and the lambda function to be executed as
                the value. As of now, it must take exactly 1 argument

        Returns:
            True if there are differences between the two dataframes, false otherwise
    """
    if not lambda_funcs:
        lambda_funcs = {}

    new_data_copy = new_data.copy(deep=True)

    # Drop the created_at and updated_at columns from the new data so they don't cause differences
    new_data_copy.drop(['created_at', 'updated_at'], axis=1, inplace=True)

    sess = GlobalDB.db().session
    current_data = pd.read_sql_table(model.__table__.name, sess.connection(), coerce_float=False)
    # Drop the created_at and updated_at for the same reason as above, also drop the pk ID column for this table
    current_data.drop([id_col, 'created_at', 'updated_at'], axis=1, inplace=True)

    # pandas comparison requires everything to be in the same order
    new_data_copy.sort_values(by=sort_cols, inplace=True)
    current_data.sort_values(by=sort_cols, inplace=True)

    # Columns have to be in order too
    cols = new_data_copy.columns.tolist()
    cols.sort()
    new_data_copy = new_data_copy[cols]

    cols = current_data.columns.tolist()
    cols.sort()
    current_data = current_data[cols]

    # Reset indexes after sorting, so that they match
    new_data_copy.reset_index(drop=True, inplace=True)
    current_data.reset_index(drop=True, inplace=True)

    # Apply any lambda functions provided to update values if needed
    for col_name, lambda_func in lambda_funcs.items():
        current_data[col_name] = current_data[col_name].apply(lambda x: lambda_func(x))

    return not new_data_copy.equals(current_data)
