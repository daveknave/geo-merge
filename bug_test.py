import pandas as pd

datadf = pd.DataFrame(data={'A': [10,10,92], 'B': [5,3,3], 'C': [324,230,562] })

def get_row_sum(d):
    rowsum = sum(d)
    return rowsum

datadf.apply(lambda x: get_row_sum(x))
