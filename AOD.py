import os
import math
import functools
from multiprocessing import Pool

# Create list 
file_names = os.listdir("wiki")
with open(os.path.join("wiki", file_names[0])) as f:
    print(f.read())
