from __init__ import *

batch = 1
print('holi'+'0'*(batch<10)+str(batch))

complete_data = {
    "year": 2022,
    "total_ocid": 90561,
    "total_posible_batches": 10,
    "batches": 10,
    "batch": 1
    }
del complete_data['total_posible_batches'], complete_data['batches'], complete_data['batch']
print(complete_data)