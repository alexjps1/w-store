import numpy as np
import matplotlib.pyplot as plt
import time
from math import ceil



def create_time_graph() ->tuple[list[int],list[float]]:
    """Create a graph that records a value over time.
        -x      list[]         seconds since event
        -y      list[]         value at seconds since event"""
    y = [0.0]
    x = [time.time()]
    return x,y

def time_graph_record(value:float, x:list[int], y:list[float]) -> None:
    """For a plot graph that records a value (float) over time 
    record that value along graph."""
    x.append(time.time() - x[0])        #Setting recorded values as offset in
    y.append(value)

def time_graph_finish(x, y, step):
    """Stops recording of data for a time-value graph. Finalizes
    axes as to make them presentable in matplotlib. Step should
    be _______"""
    x[0] = 0.0                                                     #Always starts w/no value on time 0.0
    #Create time steps
    x_axis = [ i*step for i in range(0, ceil(x[-1] * step**-1)+1) ]
    y_axis = [ 1.0 for i in range(0, ceil(x[-1] * step**-1)+1) ]
    #Average results along time step
    x_idx = 0
    total = 0.0
    for axis_idx in range(len(x_axis)):
        count = 0
        while (x_idx<len(x) and x[x_idx]<=x_axis[axis_idx]):         #While the x we're on is before the step
            count += 1
            total += y[x_idx]
            x_idx += 1
        y_axis[axis_idx] = total
    return x_axis,y_axis

def display_time_graph(x,y):
    fig, ax = plt.subplots()
    ax.plot(x, y)
    plt.show()





if __name__=="__main__":
    x,y = create_time_graph()
    for i in range(10):
        time.sleep(.5)
        time_graph_record(i,x,y)
    x,y = time_graph_finish(x,y,.5)
    display_time_graph(x,y)


#x = [923420.0, 0.1, 0.1, 0.2, 0.3, 0.5, 2.2]
#y = [0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]      #5, 5, 5, 5, 6.0
#for i in range(10):
#    time.sleep(.5)
#   time_graph_record(i, x, y)

#print(time.gmtime(0))
