import json
import os

import numpy as np
import tensorflow
import torch
import torch.nn as nn
import torch.nn.functional as F
import time

from numpy import mean
from torch.autograd import Variable

def BinaryAccuracy(y_pred,y):
    num_correct = 0
    num_examples = 0
    print("this is accuracy output y")
    print(torch.round(y_pred).type(y.type()))
    print(y)
    correct = torch.eq(torch.round(y_pred).type(y.type()), y).view(-1)
    num_correct += torch.sum(correct).item()
    num_examples += correct.shape[0]
    if num_examples == 0:
        raise ('BinaryAccuracy must have at least one example before it can be computed')
    return num_correct / num_examples

def accuracy(output, target):
    """Computes the accuracy for multiple binary predictions"""
    pred = output >= 0.5
    truth = target >= 0.5
    acc = pred.eq(truth).sum() / target.numel()
    return acc

def binary_acc(y_pred, y_test):
    y_pred_tag = torch.round(torch.sigmoid(y_pred))

    correct_results_sum = (y_pred_tag == y_test).sum().float()
    acc = correct_results_sum / y_test.shape[0]
    acc = torch.round(acc * 100)

    return acc

def get_batch_job(batch_id, istest=False, directory='job_old_2'):
    if istest:
        suffix = 'test_'
    else:
        suffix = ''
    target_model_batch = np.load(directory+'/target_model_test_'+suffix+str(batch_id)+'.np.npy')
    operators_batch = np.load(directory+'/operators_test_'+suffix+str(batch_id)+'.np.npy')
    extra_infos_batch = np.load(directory+'/extra_infos_test_'+suffix+str(batch_id)+'.np.npy')
    conditions_batch = np.load(directory+'/conditions_test_'+suffix+str(batch_id)+'.np.npy')
    mapping_batch = np.load(directory+'/mapping_test_'+suffix+str(batch_id)+'.np.npy')
    return target_model_batch, operators_batch, extra_infos_batch, conditions_batch, mapping_batch

# model definition
class Representation(nn.Module):
    def __init__(self, input_dim, hidden_dim, hid_dim, middle_result_dim, task_num):
        #operatorss, extra_infoss, conditionss, mapping
        super(Representation, self).__init__()
        self.hidden_dim = hidden_dim
        self.lstm1 = nn.LSTM(input_dim, hidden_dim, batch_first=True)
        self.batch_norm1 = nn.BatchNorm1d(hid_dim)
        self.condition_mlp = nn.Linear(hidden_dim, hid_dim)

        self.lstm2 = nn.LSTM(4 + 56 + 1 * hid_dim, hidden_dim, batch_first=True)
        self.batch_norm2 = nn.BatchNorm1d(hidden_dim)

        self.hid_mlp2_task1 = nn.Linear(hidden_dim, hid_dim)
        self.batch_norm3 = nn.BatchNorm1d(hid_dim)

        self.hid_mlp3_task1 = nn.Linear(hid_dim, hid_dim)

        self.out_mlp2_task1 = nn.Linear(hid_dim, 1)

    def init_hidden(self, hidden_dim, batch_size=1):
        # Before we've done anything, we dont have any hidden state.
        # Refer to the Pytorch documentation to see exactly
        # why they have this dimensionality.
        # The axes semantics are (num_layers, minibatch_size, hidden_dim)
        return (torch.zeros(1, batch_size, hidden_dim),
                torch.zeros(1, batch_size, hidden_dim))

    def forward(self, operators, extra_infos, conditions, mapping):
        batch_size = 0
        for i in range(operators.size()[1]):
            if operators[0][i].sum(0) != 0:
               batch_size += 1
            else:
               break
        print("this is the batch size")
        print(batch_size)
        num_level = conditions.size()[0]
        num_node_per_level = conditions.size()[1]
        num_condition_per_node = conditions.size()[2]
        condition_op_length = conditions.size()[3]

        inputs = conditions.view(num_level * num_node_per_level, num_condition_per_node, condition_op_length)
        hidden = self.init_hidden(self.hidden_dim, num_level * num_node_per_level)

        out, hid = self.lstm1(inputs, hidden)
        last_output = hid[0].view(num_level * num_node_per_level, -1)
        last_output = F.relu(self.condition_mlp(last_output))
        last_output = self.batch_norm1(last_output).view(num_level, num_node_per_level, -1)

        out = torch.cat((operators, extra_infos, last_output), 2)

        start = time.time()
        hidden = self.init_hidden(self.hidden_dim, num_node_per_level)
        last_level = out[num_level - 1].view(num_node_per_level, 1, -1)
        _, (hid, cid) = self.lstm2(last_level, hidden)
        mapping = mapping.long()

        for idx in reversed(range(0, num_level - 1)):
            mapp_left = mapping[idx][:, 0]
            mapp_right = mapping[idx][:, 1]
            pad = torch.zeros_like(hid)[:, 0].unsqueeze(1)
            next_hid = torch.cat((pad, hid), 1)
            pad = torch.zeros_like(cid)[:, 0].unsqueeze(1)
            next_cid = torch.cat((pad, cid), 1)
            hid_left = torch.index_select(next_hid, 1, mapp_left)
            cid_left = torch.index_select(next_cid, 1, mapp_left)
            hid_right = torch.index_select(next_hid, 1, mapp_right)
            cid_right = torch.index_select(next_cid, 1, mapp_right)
            hid = (hid_left + hid_right) / 2
            cid = (cid_left + cid_right) / 2
            last_level = out[idx].view(num_node_per_level, 1, -1)
            _, (hid, cid) = self.lstm2(last_level, (hid, cid))
        output = hid[0]

        end = time.time()
        print('Forest Evaluate Running Time: ', end - start)
        last_output = output[0:batch_size]
        out = self.batch_norm2(last_output)

        out_task1 = F.relu(self.hid_mlp2_task1(out))
        out_task1 = self.batch_norm3(out_task1)
        out_task1 = F.relu(self.hid_mlp3_task1(out_task1))
        out_task1 = F.relu(self.hid_mlp3_task1(out_task1))
        out_task1 = self.out_mlp2_task1(out_task1)
        out_task1 = F.sigmoid(out_task1)

        #         print 'out: ', out.size()
        # batch_size * task_num
        return out_task1

def unnormalize(vecs):
    return torch.exp(vecs)

def CrossEntropy(preds, targets):
    #preds = unnormalize(preds)
    #targets = unnormalize(targets)
    return torch.mean(-torch.sum(targets * torch.log(preds), 1))


def qerror_loss(preds, targets):
    qerror = []
    preds = unnormalize(preds)
    targets = unnormalize(targets)
    for i in range(len(targets)):
        if (preds[0] > targets[i]).cpu().data.numpy()[0]:
            qerror.append(preds[0] / targets[i])
        else:
            qerror.append(targets[i] / preds[0])
    return torch.mean(torch.cat(qerror)), torch.median(torch.cat(qerror)), torch.max(torch.cat(qerror)), torch.argmax(
        torch.cat(qerror))


def train(train_start, train_end, validate_start, validate_end, num_epochs, parameters, directory):
    acc = list()


    lsss = list()
    mymaccuracy = []
    allaccuracy = []
    input_dim = parameters.condition_op_dim
    hidden_dim = 128
    hid_dim = 256
    middle_result_dim = 128
    task_num = 1
    model = Representation(input_dim, hidden_dim, hid_dim, middle_result_dim, task_num)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
    model.train()
    start2 = time.time()
    for epoch in range(num_epochs):
        cost_loss_total = 0.
        model.train()
        for batch_idx in range(train_start, train_end):
            print('batch_idx: ', batch_idx)
            target_model, operatorss, extra_infoss, conditionss, mapping = get_batch_job(batch_idx, directory=directory)
            target_model, operatorss, extra_infoss, conditionss, mapping = torch.IntTensor(target_model), \
                                                                                      torch.FloatTensor(operatorss), \
                                                                                      torch.FloatTensor(extra_infoss), \
                                                                                      torch.FloatTensor(conditionss), \
                                                                                      torch.FloatTensor(mapping)
            operatorss, extra_infoss, conditionss = operatorss.squeeze(0), extra_infoss.squeeze(0), conditionss.squeeze(0)
            mapping = mapping.squeeze(0)
            target_model, operatorss, extra_infoss, conditionss = Variable(target_model),\
                                                                               Variable(operatorss), \
                                                                               Variable(extra_infoss), \
                                                                               Variable(conditionss)
            optimizer.zero_grad()
            estimate_cost = model(operatorss, extra_infoss, conditionss, mapping)
            target_model = target_model
            #cost_loss, cost_loss_median, cost_loss_max, cost_max_idx = qerror_loss(estimate_cost, target_model)
            cost_loss = CrossEntropy(estimate_cost,target_model)
            loss = cost_loss
            cost_loss_total += cost_loss.item()
            start = time.time()
            loss.backward()
            optimizer.step()
            end = time.time()
            #print("accuracy: {}".format(BinaryAccuracy(estimate_cost, target_model)))
            m = tensorflow.keras.metrics.BinaryAccuracy()
            m.update_state(target_model.detach().numpy(), estimate_cost.detach().numpy())
            #print("this is default accuracy ", m.result())
            #print('batchward time: ', end - start)
            mymaccuracy.append(m.result())
            allaccuracy.append(m.result())
        batch_num = train_end - train_start
        print("Epoch {}, training cost loss: {}".format(epoch, cost_loss_total / batch_num))
        print("trainig accuracy mean {}".format(mean(mymaccuracy)))

        cost_loss_total = 0.
        # accuracy
        predictions, actuals = list(), list()
        mymaccuracy = []

        for batch_idx in range(validate_start, validate_end):
            print('batch_idx: ', batch_idx)
            target_model, operatorss, extra_infoss, conditionss, mapping = get_batch_job(batch_idx, directory=directory)
            target_model, operatorss, extra_infoss, conditionss, mapping = torch.IntTensor(target_model),\
                                                                                        torch.FloatTensor(operatorss), \
                                                                                        torch.FloatTensor(extra_infoss), \
                                                                                        torch.FloatTensor(conditionss), \
                                                                                        torch.FloatTensor(mapping)
            operatorss, extra_infoss, conditionss = operatorss.squeeze(0), extra_infoss.squeeze(0), conditionss.squeeze(0)
            mapping = mapping.squeeze(0)
            target_model, operatorss, extra_infoss, conditionss = Variable(target_model), \
                                                                               Variable(operatorss), \
                                                                               Variable(extra_infoss), \
                                                                               Variable(conditionss)
            estimate_cost = model(operatorss, extra_infoss, conditionss,mapping)
            target_model = target_model
            #cost_loss, cost_loss_median, cost_loss_max, cost_max_idx = qerror_loss(estimate_cost, target_model)
            cost_loss = CrossEntropy(estimate_cost,target_model)
            loss = cost_loss
            cost_loss_total += cost_loss.item()
            m = tensorflow.keras.metrics.BinaryAccuracy()
            m.update_state(target_model.detach().numpy(), estimate_cost.detach().numpy())
            #print("this is default validation accuracy ",m.result())
            mymaccuracy.append(m.result())
            allaccuracy.append(m.result())
            # retrieve numpy array
            #yhat = estimate_cost
            actual = target_model.reshape((len(target_model), 1))
            # store
            predictions.append(estimate_cost)
            actuals.append(actual)

        batch_num = validate_end - validate_start
        #print("Epoch {}, validating cost loss:----------------------------- {}".format(epoch, cost_loss_total / batch_num))
        lsss.append(cost_loss_total / batch_num)
        # calculate accuracy
        predictions, actuals = torch.cat(predictions), torch.cat(actuals)
        #bce_loss = torch.binary_cross_entropy_with_logits(predictions, actuals)
        #print("accuracy: {}".format(BinaryAccuracy(predictions,actuals)))
        #print("binary loss: {}".format(bce_loss))
        acc.append(BinaryAccuracy(predictions,actuals))
        print("validation accuracy mean {}".format(mean(mymaccuracy)))

        #bcelsss.append(bce_loss)
    print(*acc,sep=', ')
    print("Best accuracy: {}".format(max(acc)))
    print("Average accuracy: {}".format(mean(acc)))
    print("Best validating cost loss: {}".format(min(lsss)))
    end2 = time.time()
    print("this is training time " + str(round( 1000 * ( end2 - start2))))
    print("All accuracy mean {}".format(mean(allaccuracy)))
    print("Best of accuracy {}".format(max(allaccuracy)))
    torch.save(model.state_dict(), os.path.join("/home/chahrazed/PycharmProjects/SOMproject/plan_model/model","cost_prediction_optima.pth"))
    return model

def predict(validate_start, validate_end,model, directory):
    estimate_costs=[]
    prediction_time=[]
    for batch_idx in range(validate_start, validate_end):
        target_model, operatorss, extra_infoss, conditionss, mapping = get_batch_job(batch_idx, directory=directory)
        target_model, operatorss, extra_infoss, conditionss, mapping = torch.IntTensor(target_model), \
                                                                       torch.FloatTensor(operatorss), \
                                                                       torch.FloatTensor(extra_infoss), \
                                                                       torch.FloatTensor(conditionss), \
                                                                       torch.FloatTensor(mapping)
        operatorss, extra_infoss, conditionss = operatorss.squeeze(0), extra_infoss.squeeze(0), conditionss.squeeze(0)
        mapping = mapping.squeeze(0)
        target_model, operatorss, extra_infoss, conditionss = Variable(target_model), \
                                                              Variable(operatorss), \
                                                              Variable(extra_infoss), \
                                                              Variable(conditionss)
        model.eval()
        import time
        start_time = round(time.time() * 1000)
        estimate_cost = model(operatorss, extra_infoss, conditionss, mapping)
        prediction_time.append(round(time.time() * 1000) - start_time)
        estimate_costs.append(estimate_cost)
    return estimate_costs,prediction_time