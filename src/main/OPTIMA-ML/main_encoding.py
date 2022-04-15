import json
import psutil
from plan_model.pytorch_model.base_line_encoding import save_data_job
from plan_model.pytorch_model.database_loader import load_dataset
from plan_model.pytorch_model.internal_parameters import Parameters
from plan_model.pytorch_model.meta_info import load_dictionary, obtain_upper_bound_query_size, prepare_dataset

test_plans = []
with open('pprediction_plans.json', 'r') as f:
    for idx, seq in enumerate(f.readlines()):
        plan = json.loads(seq)
        test_plans.append(plan)

#loading data set
dataset = load_dataset('/home/chahrazed/IdeaProjects/squerallGraph/src/main/resources/input_files/data')
#integer encoding of operations and tables and columns
column2pos, tables_id, columns_id, physic_ops_id, compare_ops_id, bool_ops_id, table_names = prepare_dataset(dataset)
print('data prepared')
word_vectors = load_dictionary('/home/chahrazed/PycharmProjects/SOMproject/plan_model/pytorch_model/wordvectors_updated.kv')
print('word_vectors loaded')

table_total_num = len(tables_id)
column_total_num = len(columns_id)
physic_op_total_num = len(physic_ops_id)
compare_ops_total_num = len(compare_ops_id)
bool_ops_total_num = len(bool_ops_id)
condition_op_dim = bool_ops_total_num + compare_ops_total_num + column_total_num + 1000

#getting the maximum length of a single sequence + max condition length
plan_node_max_num, condition_max_num = obtain_upper_bound_query_size('pprediction_plans.json')

parameters = Parameters(condition_max_num, tables_id, columns_id, physic_ops_id, column_total_num,
                        table_total_num, physic_op_total_num, condition_op_dim, compare_ops_id,
                        bool_ops_id, bool_ops_total_num, compare_ops_total_num, dataset, word_vectors)

save_data_job(plans=test_plans, parameters=parameters, istest=True, batch_size=1, directory='/home/chahrazed/PycharmProjects/SOMproject/plan_model/job_baseline_prediction')
