import multiprocessing
import pandas as pd
import numpy as np

np_rgb = np.load('rgb_220208.npy')

def to_df(count):
    tmp = pd.DataFrame()
    print(count)
        
    i = int(count/4)
    for j in range((count%4*64),((count%4+1)*64)):
        for k in range(256):   
            tmp = tmp.append({'R':i+1,
                              'G':j+1,
                              'B':k+1,
                              'Anger':np_rgb[i][j][k][0],
                              'Fear':np_rgb[i][j][k][1],
                              'Happy':np_rgb[i][j][k][2],
                              'Sad':np_rgb[i][j][k][3],
                              'Energy':np_rgb[i][j][k][4],
                              'Depress':np_rgb[i][j][k][5]}, ignore_index=True)

    return tmp

def parallelize_dataframe():
    pool = multiprocessing.Pool(processes=12)
    rgb = pd.concat(pool.map(to_df, range(1024)))
    pool.close()
    pool.join()
    
    return rgb

if __name__ == '__main__':  
    rgb = parallelize_dataframe()
    rgb = rgb.astype({'R':'int', 'G':'int', 'B':'int'})
    rgb.to_csv("rgb.csv")