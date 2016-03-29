import matplotlib
matplotlib.use('Agg')

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np

def show():
    plt.show()


class Diagram(object):
    def __init__(self, intervals):
        self.intervals = intervals

    def draw(self, filename):
        x, y = self.intervals.x_y_lasts()
        fig = plt.figure()
        plt.plot(x, y, 'r.')
        plt.xlabel('start time (s)')
        plt.ylabel('query last (s)')
        plt.title(filename)
        plt.grid(True)
        plt.savefig(filename + ".png")

    def print_points(self):
        x, y = self.intervals.x_y()
        tups = zip(x, y)
        for tup in tups:
            print("(%s, %s)" % tup)

class DiagramStack(object):
    def __init__(self, intervals_list):
        self.intervals_list = intervals_list

    def draw(self, filename):
        x_y_list = [intervals.x_y_incremental() for intervals in
            self.intervals_list]

        x_list = set()
        for x_y in x_y_list:
            for x in x_y[0]:
                x_list.add(x)
        x_list = list(x_list)
        x_list.sort()

        ys_list = []
        for x_y in x_y_list:
            y_list = []
            index = 0
            last_y = 0
            for x_i in range(0, len(x_list)):
                if index < len(x_y[0]) \
                        and x_list[x_i] == x_y[0][index]:
                    last_y = x_y[1][index]
                    index += 1
                y_list.append(last_y)
            ys_list.append(y_list)

        fig = plt.figure(figsize=(20,5))
        c_inapi = "#ffa043"
        c_msg = "#e83dc1"
        c_cond = "#3a65ff"
        c_sche = "#3de889"
        c_comp = "#fff53a"
        c_fail = "#ff0000"
        c_retry = "#666666"
        width = 0
        plt.stackplot(x_list, *ys_list,
                      colors=[c_inapi, c_msg, c_cond,
                              c_msg, c_sche, c_msg,
                              c_cond, c_msg, c_comp,
                              c_fail, c_retry],
                      linewidth=width)
        plt.legend([mpatches.Patch(color=c_inapi),
                    mpatches.Patch(color=c_msg),
                    mpatches.Patch(color=c_cond),
                    mpatches.Patch(color=c_sche),
                    mpatches.Patch(color=c_comp),
                    mpatches.Patch(color=c_fail),
                    mpatches.Patch(color=c_retry),
                   ],
                   ["api", "msg", "conductor",
                    "scheduler", "compute",
                    "fail", "retry"])
        plt.grid(True)
        plt.savefig(filename + ".png")
