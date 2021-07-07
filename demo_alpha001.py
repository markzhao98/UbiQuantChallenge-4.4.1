from typing import Sequence
import grpc
import contest_pb2
import contest_pb2_grpc
import question_pb2
import question_pb2_grpc

import numpy as np
import pandas as pd
import time

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
        
        # alpha001
        self.is_initialized = False
        
        # submit
        self.accepted = None
        
    def login(self):
        response_login = self.stub_contest.login(contest_pb2.LoginRequest(
            user_id=self.ID,
            user_pin=self.PIN
            ))
        self.session_key = response_login.session_key # 用于提交position
        self.login_success = response_login.success # 是否成功login
        
    def getdata(self):
        response_question = self.stub_question.get_question(question_pb2.QuestionRequest(
            user_id=self.ID,
            user_pin=self.PIN,
            sequence=0 # 首次询问数据 0 # 注意用0有收不到数据风险
        ))
        self.sequence = response_question.sequence # 之后的寻求数据sequence为这个sequence num + 1 # 如果-1 出错
        self.has_next_question = response_question.has_next_question # True - 后续仍有数据
        self.capital = response_question.capital # 总资产
        self.dailystk = response_question.dailystk # 数据！共500支股票
        self.positons = response_question.positions # 当前持仓 
        
    def alpha001_ret1(self):
        if self.is_initialized == False:
            self.submit_pos = np.random.randint(low=-2, high=3,size=(500)) # 随机仓位
            return
        else: # 在这里编写你的策略 ...
            pass
        
    def submit(self):
        response_ansr = self.stub_contest.submit_answer(contest_pb2.AnswerRequest(
            user_id=self.ID,
            user_pin=self.PIN,
            session_key=self.session_key, # 使用login时系统分配的key来提交
            sequence=self.sequence, # 使用getdata时获得的sequence
            positions=self.submit_pos # 使用alpha001_ret1中计算的pos作为答案仓位
        )) 
        self.accepted = response_ansr.accepted # 是否打通提交通道
        if not self.accepted:
            print(response_ansr.reason) # 未成功原因
        
    # def run(self):
    #     try:
    #         while True:

    #             time.sleep(0.5) # 隔4.5秒 搞一次

    #             print(time.time)

    #             self.login()
    #             print(f'Log in result: {self.login_success} ...')
    #             self.getdata()
    #             print(f'Sequence now: {self.sequence} ...')
    #             pd.DataFrame(np.asarray([array.values for array in self.dailystk])) \
    #                 .to_csv('data1000.csv', index=False, mode='a', header=False)
    #             self.alpha001_ret1()
    #             self.submit()
    #             print(f'Submit result: {self.accepted} ...')


    #     except KeyboardInterrupt:
    #         return      

    def run(self):

        pd_data = pd.DataFrame()
        last_get = time.time()

        try:            
            while True:
                while time.time() - last_get > 5:

                    last_get += 5
                    
                    t_login = time.time()

                    self.login()
                    print(f'Log in result: {self.login_success} ...')   

                    t_getdata = time.time()
                    self.getdata()
                    print(f'Sequence now: {self.sequence} ...')
                    # print(f'len of dailystk: {len(pd_data)}')
                    
                    pd_data.append(pd.DataFrame(np.asarray([array.values for array in self.dailystk])))
                    # pd.DataFrame(np.asarray([array.values for array in self.dailystk])).to_csv('data.csv', index=False, mode='a', header=False)

                    t_ret = time.time()
                    self.alpha001_ret1()

                    t_submit = time.time()
                    self.submit()
                    print(f'Submit result: {self.accepted} ...')
                
                    print(f'time for login: {t_getdata - t_login}')
                    print(f'time for getdt: {t_ret - t_getdata}')
                    print(f'time for retur: {t_submit - t_ret}')
                    print(f'time for submi: {time.time() - t_submit}')
        except KeyboardInterrupt:
            return

if __name__ == "__main__":
    c = Client()
    c.run() 
