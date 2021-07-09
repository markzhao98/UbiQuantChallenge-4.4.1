"""
厦一代表队
"""

import grpc
import contest_pb2
import contest_pb2_grpc
import question_pb2
import question_pb2_grpc

import pickle
import numpy as np
import pandas as pd
import time
import random

class Client:
    
    # --- class attribute ---
    ID = 121 # your ID
    PIN = 's5eouCB3X1' # your PIN
    CHANNEL_LOGIN_SUBMIT = grpc.insecure_channel('47.100.97.93:40723')
    CHANNEL_GETDATA = grpc.insecure_channel('47.100.97.93:40722')
    
    stub_contest = contest_pb2_grpc.ContestStub(CHANNEL_LOGIN_SUBMIT)
    stub_question = question_pb2_grpc.QuestionStub(CHANNEL_GETDATA)
    
    def __init__(self):
        # login
        self.session_key = None # 用于提交position
        self.login_success = None # 是否成功login
        # get data
        self.sequence = None # 数据index
        self.has_next_question = None # 后续是否有数据
        self.capital = None # 总资产
        self.dailystk = None # 数据！共500支股票
        self.positons = None # 当前持仓 
        # output
        self.is_initialized = False
        self.loaded_model = pickle.load(open('Strategy/MLP_model_1.sav', 'rb')) # 使用的模型
        self.pos_frame = pd.DataFrame(np.zeros([10, 500]))
        self.leverage = 1.5  # 杠杆率
        # submit
        self.accepted = None

    def XOX(self, s, p):
        '''
        将绝对量数据转化为增长率 ( e.g. [1,2,3] -> [2,1.5,NA] )
        s : array
        p : look-back period
        '''
        return np.append((s[p:] - s[:-p])/s[:-p], np.repeat(np.nan, p))

    def lagging(self, s, l):
        '''
        向前平移时间序列 ( e.g. [1,2,3] -> [2,3,NA] )
        s : array
        l : lagging period
        '''
        return np.append(s[l:], np.repeat(np.nan, l))

    def login(self):
        response_login = self.stub_contest.login(contest_pb2.LoginRequest(
            user_id=self.ID,
            user_pin=self.PIN
            ))
        self.session_key = response_login.session_key # 用于提交position
        self.login_success = response_login.success # 是否成功login
        if not self.login_success:
            time.sleep(0.1)

    def getdata(self):
        response_question = self.stub_question.get_question(question_pb2.QuestionRequest(
            user_id=self.ID,
            user_pin=self.PIN,
            sequence=0 # 首次询问数据 0 # 注意用0有收不到数据风险
        ))
        self.sequence = response_question.sequence # 之后的寻求数据sequence为这个sequence num + 1 # 如果-1 出错
        if self.sequence == -1:
            time.sleep(0.1)
        self.has_next_question = response_question.has_next_question # True - 后续仍有数据
        self.capital = response_question.capital # 总资产
        self.dailystk = response_question.dailystk # 数据！共500支股票
        self.positons = response_question.positions # 当前持仓 
        
    def output(self):

        # if self.is_initialized == False:
        #     self.submit_pos = np.random.randint(low=-20, high=30,size=(500)) # 随机仓位
        #     return
        # else: # 在这里编写你的策略 ...
        #     pass

        X_pred = self.dailynew.iloc[:,8:108].values
        pred = self.loaded_model.predict(X_pred)
        longstock = pred.argsort()[-25:]
        shortstock = pred.argsort()[:25]
        newpostoday = np.zeros(500)
        newpostoday[longstock] = self.capital*self.leverage/50/10/self.dailynew \
            .iloc[:,5].values[longstock]
        newpostoday[shortstock] = -self.capital*self.leverage/50/10/self.dailynew \
            .iloc[:,5].values[shortstock]

        self.pos_frame = self.pos_frame.append(pd.DataFrame([newpostoday]))
        self.pos_frame = self.pos_frame.iloc[1:]
        self.submit_pos = self.pos_frame.sum().values
        return

    def submit(self):
        response_ansr = self.stub_contest.submit_answer(contest_pb2.AnswerRequest(
            user_id=self.ID,
            user_pin=self.PIN,
            session_key=self.session_key, # 使用login时系统分配的key来提交
            sequence=self.sequence, # 使用getdata时获得的sequence
            positions=self.submit_pos # 使用output中计算的pos作为答案仓位
        ))
        self.accepted = response_ansr.accepted # 是否打通提交通道
        if not self.accepted:
            print(response_ansr.reason) # 未成功原因
        
    def run(self):

        last_get = time.time()

        self.login()
        print(f'Log in result: {self.login_success} ...')   
        self.getdata()
        print(f'Sequence now: {self.sequence} ...')
        self.dailynew = pd.DataFrame(np.asarray([array.values for array in self.dailystk]))
        self.output()
        self.submit()
        print(f'Submit result: {self.accepted} ...')
        
        try:        
            while True:
                while time.time() - last_get < 5 + 0.1 * random.randint(0,9):
                    continue
                last_get += 5
                
                self.login()
                print(f'Log in result: {self.login_success} ...')
                self.getdata()
                print(f'Sequence now: {self.sequence} ...')
                self.dailynew = pd.DataFrame(np.asarray([array.values for array in self.dailystk]))
                self.output()
                self.submit()
                print(f'Submit result: {self.accepted} ...')
                
        except KeyboardInterrupt:
            return

if __name__ == "__main__":
    c = Client()
    c.run()