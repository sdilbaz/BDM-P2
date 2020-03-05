'''
Problem 1 dataset

@author Geng
'''

import random
import pandas as pd
random.seed(123)

def dataset_P():
    X_coord = []
    Y_coord = []
    for i in range(11000000):

        X_coord.append(random.randint(1, 10000))
        Y_coord.append(random.randint(1, 10000))

    file = pd.DataFrame({
        'X': X_coord,
        'Y': Y_coord
    })
    file.to_csv('./spatial_join_P.csv', header=False, index=None)


def dataset_R():
    num = []
    x_bottom_left = []
    y_bottom_left = []
    x_top_right = []
    y_top_right = []

    for i in range(5500000):
        num.append('r'+str(i+1))
        center_x = random.randint(1, 10000)
        while center_x == 10000:
            center_x = random.randint(1, 10000)
        center_y = random.randint(1, 10000)
        while center_y == 10000:
            center_y = random.randint(1, 10000)
        x_bottom_left.append(center_x)
        y_bottom_left.append(center_y)
        width = random.randint(center_x + 1, center_x + 20)
        while width > 10000:
            width = random.randint(center_x + 1, center_x + 20)
        hight = random.randint(center_y + 1, center_y + 5)
        while hight > 10000:
            hight = random.randint(center_y + 1, center_y + 5)

        x_top_right.append(width)
        y_top_right.append(hight)

        print('now is :', i)

    file = pd.DataFrame({
        'num':num,
        'x1':x_bottom_left,
        'y1':y_bottom_left,
        'x2':x_top_right,
        'y2':y_top_right
    })

    file.to_csv('./spatial_join_R.csv', header=False, index=None)

if __name__=='__main__':
    dataset_P()
    dataset_R()





