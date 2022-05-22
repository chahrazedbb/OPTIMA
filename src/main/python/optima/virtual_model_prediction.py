import json
from data_model_encoding import save_data_job
import torch
from database_loader import load_dataset
from internal_parameters import Parameters
from meta_info import load_dictionary, prepare_dataset, obtain_upper_bound_query_size
from data_model_classification import predict, Representation

test_plans = []
with open('/home/chahrazed/IdeaProjects/OPTIMA/src/main/python/optima/dataset/one_plan.json', 'r') as f:
    for idx, seq in enumerate(f.readlines()):
        plan = json.loads(seq)
        test_plans.append(plan)

#loading data set
dataset = load_dataset('/home/chahrazed/IdeaProjects/OPTIMA/src/main/evaluation/small_data')
#integer encoding of operations and tables and columns
column2pos, tables_id, columns_id, physic_ops_id, compare_ops_id, bool_ops_id, table_names = prepare_dataset(dataset)

#word_vectors = load_dictionary('/home/chahrazed/PycharmProjects/SOMproject/plan_model/pytorch_model/wordvectors_updated.kv')
#print('loading word_vectors')

table_total_num = len(tables_id)
column_total_num = len(columns_id)
physic_op_total_num = len(physic_ops_id)
compare_ops_total_num = len(compare_ops_id)
bool_ops_total_num = len(bool_ops_id)
condition_op_dim = bool_ops_total_num + compare_ops_total_num + column_total_num + 1000

#getting the maximum length of a single sequence + max condition length
plan_node_max_num, condition_max_num = obtain_upper_bound_query_size('/home/chahrazed/IdeaProjects/OPTIMA/src/main/python/optima/dataset/one_plan.json')

parameters = Parameters(condition_max_num, tables_id, columns_id, physic_ops_id, column_total_num,
                        table_total_num, physic_op_total_num, condition_op_dim, compare_ops_id,
                        bool_ops_id, bool_ops_total_num, compare_ops_total_num, dataset)
#encoding
save_data_job(plans=test_plans, parameters=parameters, istest=True, batch_size=1, directory='/home/chahrazed/IdeaProjects/OPTIMA/src/main/python/optima/job')

#predicting
input_dim = parameters.condition_op_dim
hidden_dim = 128
hid_dim = 256
middle_result_dim = 128
task_num = 1
#model = Representation(input_dim, hidden_dim, hid_dim)
model = Representation(input_dim, hidden_dim, hid_dim, middle_result_dim, task_num)
model.load_state_dict(torch.load("/home/chahrazed/PycharmProjects/SOMproject/plan_model/model/cost_prediction_optima.pth"))
y = predict(model, directory='/home/chahrazed/IdeaProjects/OPTIMA/src/main/python/optima/job')
print(y)