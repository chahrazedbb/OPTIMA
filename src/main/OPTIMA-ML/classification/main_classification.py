import torch

from data_model_classification import train, predict, Representation
from database_loader import load_dataset
from internal_parameters import Parameters
from meta_info import load_dictionary, prepare_dataset, obtain_upper_bound_query_size

dataset = load_dataset('data')
column2pos, tables_id, columns_id, physic_ops_id, compare_ops_id, bool_ops_id, table_names = prepare_dataset(
    dataset)
print('preparing data')
word_vectors = load_dictionary('wordvectors_updated.kv')
print('loading word_vectors')

table_total_num = len(tables_id)
column_total_num = len(columns_id)
physic_op_total_num = len(physic_ops_id)
compare_ops_total_num = len(compare_ops_id)
bool_ops_total_num = len(bool_ops_id)

condition_op_dim = bool_ops_total_num + compare_ops_total_num + column_total_num + 1000
plan_node_max_num, condition_max_num = obtain_upper_bound_query_size('plans.json')


print('query upper size prepared')

parameters = Parameters(condition_max_num, tables_id, columns_id, physic_ops_id, column_total_num,
                        table_total_num, physic_op_total_num, condition_op_dim, compare_ops_id,
                        bool_ops_id, bool_ops_total_num, compare_ops_total_num, dataset, word_vectors)

model = train(0, 40, 40, 50, 30, parameters, directory='job')
