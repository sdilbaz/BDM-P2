# -*- coding: utf-8 -*-
"""
Created on Mon Feb 17 20:08:33 2020

@author: User
"""

import numpy as np
import os

MAX_X=10000
MAX_Y=10000

def DataGen(K=1,scale=250,number_of_points=float('inf')):
    validator=lambda point:[min(MAX_X-1,max(0,point[0])),min(MAX_Y-1,max(0,point[1]))]
    cluster_centers = [[np.random.randint(scale/2,MAX_X-scale/2),np.random.randint(scale/2,MAX_Y-scale/2)] for _ in range(K)]
    n=0
    while n<number_of_points:
        n+=1
        selected_cluster=np.random.randint(K)
        cluster_center=cluster_centers[selected_cluster]
        point=np.round(np.random.normal(loc=cluster_center,scale=scale)).astype('int')
        point=validator(point)
        yield [str(val) for val in point]
    
if __name__=='__main__':
    run_dir=os.path.dirname(os.path.abspath(__file__))
    
    with open(os.path.join(run_dir,"points_k_means.txt"),"w") as file:
        dgen = DataGen(K=20,number_of_points=10000000)
        for point in dgen:
            file.write(','.join(point)+'\n')
    
    