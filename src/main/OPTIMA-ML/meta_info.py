from gensim.models import Word2Vec
from gensim.models import KeyedVectors

import json
import math

def load_dictionary(path):
    word_vectors = KeyedVectors.load(path, mmap='r')
    return word_vectors

def determine_prefix(column):
    relation_name = column.split('.')[0]
    column_name = column.split('.')[1]
    if relation_name == 'all_relations':
        if column_name == 'all_columns':
            return 'all'
    if relation_name == 'person':
        if column_name == 'nr' or column_name == 'id':
            return 'nr'
        elif column_name == 'name':
            return 'name'
        elif column_name == 'mbox_sha1sum':
            return 'mbox_sha1sum'
        elif column_name == 'personCountry' or column_name == 'country':
            return 'personCountry'
        elif column_name == 'personPublisher' or column_name == 'publisher':
            return 'personPublisher'
        elif column_name == 'personPublishDate' or column_name == 'publishDate':
            return 'personPublishDate'
        else:
            print (column)
            raise
    elif relation_name == 'offer':
        if column_name == 'id':
            return 'id'
        elif column_name == 'product':
            return 'product'
        elif column_name == 'vendor':
            return 'vendor'
        elif column_name == 'price':
            return 'price'
        elif column_name == 'validFrom':
            return 'validFrom'
        elif column_name == 'validTo':
            return 'validTo'
        elif column_name == 'deliveryDays':
            return 'deliveryDays'
        elif column_name == 'offerWebpage':
            return 'offerWebpage'
        elif column_name == 'publisher':
            return 'publisher'
        elif column_name == 'publishDate':
            return 'publishDate'
        else:
            print (column)
            raise
    elif relation_name == 'producer':
        if column_name == 'id':
            return 'id'
        elif column_name == 'producerLabel' or column_name == 'label':
            return 'producerLabel'
        elif column_name == 'comment' or column_name == 'producerComment':
            return 'producerComment'
        elif column_name == 'homepage':
            return 'homepage'
        elif column_name == 'country' or column_name == 'producerCountry':
            return 'producerCountry'
        elif column_name == 'producerPublisher' or column_name == 'publisher':
            return 'producerPublisher'
        elif column_name == 'producerPublishDate' or column_name == 'publishDate':
            return 'producerPublishDate'
        else:
            print (column)
            raise
    elif relation_name == 'product':
        if column_name == 'id':
            return 'id'
        elif column_name == 'label':
            return 'label'
        elif column_name == 'comment':
            return 'comment'
        elif column_name == 'producer':
            return 'producer'
        elif column_name == 'propertyNum1':
            return 'propertyNum1'
        elif column_name == 'propertyNum2':
            return 'propertyNum2'
        elif column_name == 'propertyNum3':
            return 'propertyNum3'
        elif column_name == 'propertyNum4':
            return 'propertyNum4'
        elif column_name == 'propertyNum5':
            return 'propertyNum5'
        elif column_name == 'propertyNum6':
            return 'propertyNum6'
        elif column_name == 'propertyTex1':
            return 'propertyTex1'
        elif column_name == 'propertyTex2':
            return 'propertyTex2'
        elif column_name == 'propertyTex3':
            return 'propertyTex3'
        elif column_name == 'propertyTex4':
            return 'propertyTex4'
        elif column_name == 'propertyTex5':
            return 'propertyTex5'
        elif column_name == 'propertyTex6':
            return 'propertyTex6'
        elif column_name == 'productPublisher' or column_name == 'publisher':
            return 'productPublisher'
        elif column_name == 'productPublishDate' or column_name == 'publishDate':
            return 'productPublishDate'
        else:
            print (column)
            raise
    elif relation_name == 'review':
        if column_name == 'id':
            return 'id'
        elif column_name == 'product':
            return 'product'
        elif column_name == 'producer':
            return 'producer'
        elif column_name == 'person':
            return 'person'
        elif column_name == 'reviewDate':
            return 'reviewDate'
        elif column_name == 'title':
            return 'title'
        elif column_name == 'text':
            return 'text'
        elif column_name == 'language':
            return 'language'
        elif column_name == 'rating1':
            return 'rating1'
        elif column_name == 'rating2':
            return 'rating2'
        elif column_name == 'rating3':
            return 'rating3'
        elif column_name == 'rating4':
            return 'rating4'
        elif column_name == 'reviewPublisher' or column_name == 'publisher':
            return 'reviewPublisher'
        elif column_name == 'reviewPublishDate' or column_name == 'publishDate':
            return 'reviewPublishDate'
        else:
            print (column)
            raise
    else:
        print (column)
        raise

def obtain_upper_bound_query_size(path):
    plan_node_max_num = 0
    condition_max_num = 0
    plans = []
    with open(path, 'r') as f:
        for plan in f.readlines():
            plan = json.loads(plan)
            plans.append(plan)
            sequence = plan['seq']
            plan_node_num = len(sequence)
            if plan_node_num > plan_node_max_num:
                plan_node_max_num = plan_node_num
            for node in sequence:
                if node == None:
                    continue
                if 'filter' in node:
                    condition_num = len(node['filter'])
                    if condition_num > condition_max_num:
                        condition_max_num = condition_num
    return plan_node_max_num, condition_max_num

def prepare_dataset(database):

    column2pos = dict()

    tables = ['person','offer','producer','product','review']

    for table_name in tables:
        column2pos[table_name] = database[table_name].columns

    physic_ops_id = {'join': 1, 'bgp': 2, 'orderby': 3, 'limit': 4}
    compare_ops_id = {'=': 1, '>': 2, '<': 3, '!=': 4, '>=': 5, '<=': 6}
    bool_ops_id = {'AND': 1, 'OR': 2}
    tables_id = {}
    columns_id = {}
    table_id = 1
    column_id = 1
    for table_name in tables:
        tables_id[table_name] = table_id
        table_id += 1
        for column in column2pos[table_name]:
            columns_id[table_name+'.'+column] = column_id
            column_id += 1
    return column2pos, tables_id, columns_id, physic_ops_id, compare_ops_id, bool_ops_id, tables