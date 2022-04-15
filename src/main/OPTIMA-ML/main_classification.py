from plan_model.pytorch_model.base_line_classification import train, predict, Representation
from plan_model.pytorch_model.database_loader import load_dataset
from plan_model.pytorch_model.internal_parameters import Parameters
from plan_model.pytorch_model.meta_info import load_dictionary, prepare_dataset, obtain_upper_bound_query_size

dataset = load_dataset('/home/chahrazed/IdeaProjects/squerallGraph/src/main/resources/input_files/data')
column2pos, tables_id, columns_id, physic_ops_id, compare_ops_id, bool_ops_id, table_names = prepare_dataset(
    dataset)
print('data prepared')
word_vectors = load_dictionary('/home/chahrazed/PycharmProjects/SOMproject/plan_model/pytorch_model/wordvectors_updated.kv')
print('word_vectors loaded')

table_total_num = len(tables_id)
column_total_num = len(columns_id)
physic_op_total_num = len(physic_ops_id)
compare_ops_total_num = len(compare_ops_id)
bool_ops_total_num = len(bool_ops_id)

condition_op_dim = bool_ops_total_num + compare_ops_total_num + column_total_num + 1000
plan_node_max_num, condition_max_num = obtain_upper_bound_query_size('plans.json')

parameters = Parameters(condition_max_num, tables_id, columns_id, physic_ops_id, column_total_num,
                        table_total_num, physic_op_total_num, condition_op_dim, compare_ops_id,
                        bool_ops_id, bool_ops_total_num, compare_ops_total_num, dataset, word_vectors)

model = train(0, 40, 40, 50, 30, parameters, directory='/home/chahrazed/PycharmProjects/SOMproject/plan_model/job_baseline')