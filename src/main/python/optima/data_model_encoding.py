import numpy as np
import re
from sklearn.ensemble._hist_gradient_boosting.grower import TreeNode

from meta_info import determine_prefix


def get_representation(value):
    #if value in word_vectors:
    #    embedded_result = np.array(list(word_vectors[value]))
    #else:
    embedded_result = np.array([0.0 for _ in range(500)])
    hash_result = np.array([0.0 for _ in range(500)])
    for t in value:
        hash_result[hash(t) % 500] = 1.0
    return np.concatenate((embedded_result, hash_result), 0)


def get_str_representation(value, column):
    vec = np.array([])
    count = 0
    for v in value.split('%'):
        if len(v) > 0:
            if len(vec) == 0:
                vec = get_representation(column + v)
                count = 1
            else:
                new_vec = get_representation(column + v)
                vec = vec + new_vec
                count += 1
    if count > 0:
        vec /= float(count)
    return vec


def encode_condition_op(condition_op, relation_name, parameters):
    # bool_operator + left_value + compare_operator + right_value
    if condition_op is None:
        vec = [0 for _ in range(parameters.condition_op_dim)]
    #elif condition_op['op_type'] == 'Bool':
    #    idx = parameters.bool_ops_id[condition_op['operator']]
    #    vec = [0 for _ in range(parameters.bool_ops_total_num)]
    #    vec[idx - 1] = 1
    elif condition_op['op_type'] == 'Compare':
        operator = condition_op['operator']
        left_value = condition_op['left_value']
        if re.match(r'.+\..+', left_value) is None:
            left_value = relation_name + '.' + left_value
        else:
            relation_name = left_value.split('.')[0]
        relation_name = left_value.split('.')[0]
        left_value = relation_name + '.' + determine_prefix(left_value)
        left_value_idx = parameters.columns_id[left_value]
        left_value_vec = [0 for _ in range(parameters.column_total_num)]
        left_value_vec[left_value_idx - 1] = 1
        right_value = condition_op['right_value']
        column_name = left_value.split('.')[1]
        if re.match(r'^[a-z][a-zA-Z0-9_]*\.[a-z][a-zA-Z0-9_]*$', right_value) is not None and right_value.split('.')[0] in parameters.data:
            operator_idx = parameters.compare_ops_id[operator]
            operator_vec = [0 for _ in range(parameters.compare_ops_total_num)]
            operator_vec[operator_idx - 1] = 1
            right_value_idx = parameters.columns_id[right_value]
            right_value_vec = [0]
            left_value_vec[right_value_idx - 1] = 1
        elif parameters.data[relation_name].dtypes[column_name] == 'int64' or parameters.data[relation_name].dtypes[column_name] == 'float64':
            right_value = float(right_value)
            value_max = parameters.min_max_column[relation_name][column_name]['max']
            value_min = parameters.min_max_column[relation_name][column_name]['min']
            right_value_vec = [(right_value - value_min) / (value_max - value_min)]
            operator_idx = parameters.compare_ops_id[operator]
            operator_vec = [0 for _ in range(parameters.compare_ops_total_num)]
            operator_vec[operator_idx - 1] = 1
        elif re.match(r'^__LIKE__', right_value) is not None:
            operator_idx = parameters.compare_ops_id['~~']
            operator_vec = [0 for _ in range(parameters.compare_ops_total_num)]
            operator_vec[operator_idx - 1] = 1
            right_value = right_value.strip('\'')[8:]
            right_value_vec = get_str_representation(right_value, left_value).tolist()
        elif re.match(r'^__NOTLIKE__', right_value) is not None:
            operator_idx = parameters.compare_ops_id['!~~']
            operator_vec = [0 for _ in range(parameters.compare_ops_total_num)]
            operator_vec[operator_idx - 1] = 1
            right_value = right_value.strip('\'')[11:]
            right_value_vec = get_str_representation(right_value, left_value).tolist()
        elif re.match(r'^__NOTEQUAL__', right_value) is not None:
            operator_idx = parameters.compare_ops_id['!=']
            operator_vec = [0 for _ in range(parameters.compare_ops_total_num)]
            operator_vec[operator_idx - 1] = 1
            right_value = right_value.strip('\'')[12:]
            right_value_vec = get_str_representation(right_value, left_value).tolist()
        elif re.match(r'^__ANY__', right_value) is not None:
            operator_idx = parameters.compare_ops_id['=']
            operator_vec = [0 for _ in range(parameters.compare_ops_total_num)]
            operator_vec[operator_idx - 1] = 1
            right_value = right_value.strip('\'')[7:].strip('{}')
            right_value_vec = []
            count = 0
            for v in right_value.split(','):
                v = v.strip('"').strip('\'')
                if len(v) > 0:
                    count += 1
                    vec = get_str_representation(v, left_value).tolist()
                    if len(right_value_vec) == 0:
                        right_value_vec = [0 for _ in vec]
                    for idx, vv in enumerate(vec):
                        right_value_vec[idx] += vv
            for idx in range(len(right_value_vec)):
                right_value_vec[idx] /= len(right_value.split(','))
        elif right_value == 'None':
            operator_idx = parameters.compare_ops_id['!Null']
            operator_vec = [0 for _ in range(parameters.compare_ops_total_num)]
            operator_vec[operator_idx - 1] = 1
            if operator == 'IS':
                right_value_vec = [1]
            elif operator == '!=':
                right_value_vec = [0]
            else:
                #print(operator)
                raise
        else:
            #             print (left_value, operator, right_value)
            operator_idx = parameters.compare_ops_id[operator]
            operator_vec = [0 for _ in range(parameters.compare_ops_total_num)]
            operator_vec[operator_idx - 1] = 1
            right_value_vec = get_str_representation(right_value, left_value).tolist()
        vec = [0 for _ in range(parameters.bool_ops_total_num)]
        vec = vec + left_value_vec + operator_vec + right_value_vec
    num_pad = parameters.condition_op_dim - len(vec)
    result = np.pad(vec, (0, num_pad), 'constant')
    #     print 'condition op: ', result
    return result

def encode_condition(condition, relation_name, parameters):
    if len(condition) == 0:
        vecs = [[0 for _ in range(parameters.condition_op_dim)]]
    else:
        vecs = [encode_condition_op(condition_op, relation_name, parameters) for condition_op in condition]
    num_pad = parameters.condition_max_num - len(vecs)
    result = np.pad(vecs, ((0, num_pad), (0, 0)), 'constant')
    return result

def encode_node_job(node, parameters):
    # operator + first_condition + second_condition + relation
    extra_info_num = max(parameters.column_total_num, parameters.table_total_num)
    operator_vec = np.array([0 for _ in range(parameters.physic_op_total_num)])
    extra_info_vec = np.array([0 for _ in range(extra_info_num)])
    condition_vec = np.array([[0 for _ in range(parameters.condition_op_dim)] for _ in range(parameters.condition_max_num)])

    #has_condition = 0
    if node != None:
        operator = node['node_type']
        operator_idx = parameters.physic_ops_id[operator]
        operator_vec[operator_idx - 1] = 1
        if operator == 'join':
            condition_vec = encode_condition(node['condition'], None, parameters)
        elif operator == 'bgp':
            relation_name = node['relation_name']
            triple = node['triple']
            #relartion encoding
            extra_info_inx = parameters.tables_id[relation_name]
            extra_info_vec[extra_info_inx - 1] = 1
            #projection columns encoding (triple)
            columns_names = triple.split(',')
            for column_name in columns_names:
                column_prefix = determine_prefix(relation_name + "." + column_name)
                column_idx = parameters.columns_id[relation_name + "." + column_prefix]
                triple_vec = [0 for _ in range(extra_info_num)]
                triple_vec[column_idx - 1] = 1
                extra_info_vec = extra_info_vec + triple_vec
            #condition encoding
            condition_vec = encode_condition(node['filter'], relation_name, parameters)
        elif operator == 'orderby':
            entity = node['entity']
            # entity encoding
            extra_info_inx = parameters.columns_id[entity]
            extra_info_vec[extra_info_inx - 1] = 1
        elif operator == 'limit':
            # float encoding
            value_max = 5000
            limit = node['value']
            limit_vec = [ limit / 5000]
            operator_idx = parameters.compare_ops_id['=']
            op_vec = [0 for _ in range(parameters.compare_ops_total_num)]
            op_vec[operator_idx - 1] = 1
            vec = [0 for _ in range(parameters.bool_ops_total_num)]
            vec = vec + op_vec + limit_vec
            num_pad = parameters.condition_op_dim - len(vec)
            vecs = [np.pad(vec, (0, num_pad), 'constant')]
            num_pad = parameters.condition_max_num - len(vecs)
            condition_vec = np.pad(vecs, ((0, num_pad), (0, 0)), 'constant')
    return operator_vec, extra_info_vec, condition_vec

class TreeNode(object):
    def __init__(self, current_vec, parent, idx, level_id):
        self.item = current_vec
        self.idx = idx
        self.level_id = level_id
        self.parent = parent
        self.children = []

    def get_parent(self):
        return self.parent

    def get_item(self):
        return self.item

    def get_children(self):
        return self.children

    def add_child(self, child):
        self.children.append(child)

    def get_idx(self):
        return self.idx

    def __str__(self):
        return 'level_id: ' + self.level_id + '; idx: ' + self.idx


def recover_tree(vecs, parent, start_idx):
    if len(vecs) == 0:
        return vecs, start_idx
    if vecs[0] == None:
        return vecs[1:], start_idx + 1
    node = TreeNode(vecs[0], parent, start_idx, -1)
    while True:
        vecs, start_idx = recover_tree(vecs[1:], node, start_idx + 1)
        parent.add_child(node)
        if len(vecs) == 0:
            return vecs, start_idx
        if vecs[0] == None:
            return vecs[1:], start_idx + 1
        node = TreeNode(vecs[0], parent, start_idx, -1)


def dfs_tree_to_level(root, level_id, nodes_by_level):
    root.level_id = level_id
    if len(nodes_by_level) <= level_id:
        nodes_by_level.append([])
    nodes_by_level[level_id].append(root)
    root.idx = len(nodes_by_level[level_id])
    for c in root.get_children():
        dfs_tree_to_level(c, level_id + 1, nodes_by_level)


def encode_plan_job(plan, parameters):
    #operators, extra_infos, conditions, condition_masks = [], [], [], []
    operators, extra_infos, conditions = [], [], []
    mapping = []
    nodes_by_level = []
    node = TreeNode(plan[0], None, 0, -1)
    recover_tree(plan[1:], node, 1)
    dfs_tree_to_level(node, 0, nodes_by_level)

    for level in nodes_by_level:
        operators.append([])
        extra_infos.append([])
        conditions.append([])
        #condition_masks.append([])
        mapping.append([])
        for node in level:
            #operator, extra_info, condition, condition_mask = encode_node_job(node.item, parameters)
            operator, extra_info, condition = encode_node_job(node.item, parameters)
            operators[-1].append(operator)
            extra_infos[-1].append(extra_info)
            conditions[-1].append(condition)
            #condition_masks[-1].append(condition_mask)
            if len(node.children) == 2:
                mapping[-1].append([n.idx for n in node.children])
            elif len(node.children) == 1:
                mapping[-1].append([node.children[0].idx, 0])
            else:
                mapping[-1].append([0, 0])
    #return operators, extra_infos, conditions, condition_masks, mapping
    return operators, extra_infos, conditions, mapping


def normalize_label(labels, mini, maxi):
    labels_norm = (np.log(labels) - mini) / (maxi - mini)
    labels_norm = np.minimum(labels_norm, np.ones_like(labels_norm))
    labels_norm = np.maximum(labels_norm, np.zeros_like(labels_norm))
    return labels_norm


def merge_plans_level(level1, level2, isMapping=False):
    for idx, level in enumerate(level2):
      #  print("idx")
      #  print(idx)
        if idx >= len(level1):
            level1.append([])
        if isMapping:
            if idx < len(level1) - 1:
                base = len(level1[idx + 1])
                for i in range(len(level)):
                    if level[i][0] > 0:
                        level[i][0] += base
                    if level[i][1] > 0:
                        level[i][1] += base
        level1[idx] += level
    return level1


def make_data_job(plans, parameters):
    target_model_batch = []
    operators_batch = []
    extra_infos_batch = []
    conditions_batch = []
    #condition_masks_batch = []
    mapping_batch = []

    for plan in plans:
        target_model = plan['model']
        target_model_batch.append(target_model)

        plan = plan['seq']

#       operators, extra_infos, conditions, condition_masks, mapping = encode_plan_job(plan, parameters)
        operators, extra_infos, conditions, mapping = encode_plan_job(plan, parameters)

        operators_batch = merge_plans_level(operators_batch, operators)
        extra_infos_batch = merge_plans_level(extra_infos_batch, extra_infos)

        conditions_batch = merge_plans_level(conditions_batch, conditions)
        #condition_masks_batch = merge_plans_level(condition_masks_batch, condition_masks)
        mapping_batch = merge_plans_level(mapping_batch, mapping, True)
    max_nodes = 0
    for o in operators_batch:
        if len(o) > max_nodes:
            max_nodes = len(o)

    operators_batch = np.array([np.pad(v, ((0, max_nodes - len(v)), (0, 0)), 'constant') for v in operators_batch])
    extra_infos_batch = np.array([np.pad(v, ((0, max_nodes - len(v)), (0, 0)), 'constant') for v in extra_infos_batch])
    conditions_batch = np.array(
        [np.pad(v, ((0, max_nodes - len(v)), (0, 0), (0, 0)), 'constant') for v in conditions_batch])
    #condition_masks_batch = np.array([np.pad(v, (0, max_nodes - len(v)), 'constant') for v in condition_masks_batch])
    mapping_batch = np.array([np.pad(v, ((0, max_nodes - len(v)), (0, 0)), 'constant') for v in mapping_batch])

    operators_batch = np.array([operators_batch])
    extra_infos_batch = np.array([extra_infos_batch])
    conditions_batch = np.array([conditions_batch])
    #condition_masks_batch = np.array([condition_masks_batch])
    mapping_batch = np.array([mapping_batch])

    return (target_model_batch, operators_batch, extra_infos_batch, conditions_batch, mapping_batch)


def chunks(arr, batch_size):
    return [arr[i:i+batch_size] for i in range(0, len(arr), batch_size)]

def save_data_job(plans, parameters, istest=False, batch_size=64, directory='jobtest'):
    if istest:
        suffix = 'test_'
    else:
        suffix = ''
    timelist = []
    batch_id = 0
    for batch_id, plans_batch in enumerate(chunks(plans, batch_size)):
      #  print('batch_id', batch_id, len(plans_batch))
        import time
        start_time = round(time.time() * 1000)
        target_model_batch, operators_batch, extra_infos_batch, conditions_batch, mapping_batch = make_data_job(plans_batch, parameters)
        timelist.append(round(time.time() * 1000) - start_time)
        np.save(directory + '/target_model_' + suffix + str(batch_id) + '.np', target_model_batch)
        np.save(directory + '/operators_' + suffix + str(batch_id) + '.np', operators_batch)
        np.save(directory + '/extra_infos_' + suffix + str(batch_id) + '.np', extra_infos_batch)
        np.save(directory + '/conditions_' + suffix + str(batch_id) + '.np', conditions_batch)
        #np.save(directory + '/condition_masks_' + suffix + str(batch_id) + '.np', condition_masks_batch)
        np.save(directory + '/mapping_' + suffix + str(batch_id) + '.np', mapping_batch)
        #print("--- %s seconds ---" % (time.time() - start_time))
       # print('saved: ', str(batch_id))
   # print(timelist)