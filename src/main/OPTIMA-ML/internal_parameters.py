
class Parameters():
    #meta information about the dataset liek size of data, maximum condition, nb of columns , max cost
    def __init__(self, condition_max_num, tables_id, columns_id, physic_ops_id, column_total_num,
                 table_total_num, physic_op_total_num, condition_op_dim, compare_ops_id, bool_ops_id,
                 bool_ops_total_num, compare_ops_total_num, data, word_vectors):
        self.condition_max_num = condition_max_num
        self.tables_id = tables_id
        self.columns_id = columns_id
        self.physic_ops_id = physic_ops_id
        self.column_total_num = column_total_num
        self.table_total_num = table_total_num
        self.physic_op_total_num = physic_op_total_num
        self.condition_op_dim = condition_op_dim
        self.compare_ops_id = compare_ops_id
        self.bool_ops_id = bool_ops_id
        self.bool_ops_total_num = bool_ops_total_num
        self.compare_ops_total_num = compare_ops_total_num
        self.data = data
#        self.min_max_column = min_max_column
        self.word_vectors = word_vectors