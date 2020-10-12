import matplotlib.pyplot as plt
import csv
import string
import numpy as np
import pandas as pd
import sys
import os, os.path
import glob

x = []
y = []
dates = []

file_name = sys.argv[1];

path = (sys.argv[1])

num_files = (len([f for f in os.listdir(path)if os.path.isfile(os.path.join(path, f))]))-4

print (num_files)

file_name_complete =[]

file_name_complete = (file_name+"/part-r-00000"+" ")
        
for i in range(1,num_files,+1):
	file_name_complete += (file_name+"/part-r-0000"+ str(i)+" ")

list= file_name_complete.split()

specificity.txt

print(list)

for fname in list:
	print(fname)
	with open(fname,'r') as csvfile:
		plots = csv.reader(csvfile, delimiter='	')
		for row in plots:
			x.append((row[0]))
			y.append(float(row[1]))

plt.plot(x,y, label='Revenues % time')
plt.xlabel('Time in years')
plt.ylabel('Revenues in euros')
plt.title('Taxi revenues from airport')
plt.legend()
plt.savefig('graph.png')	
plt.show()

    