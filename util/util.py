import json

def read_auth_file(auth_file):
    with open(auth_file, 'r') as f:
        return json.loads(f.read())

END = '\x1b[m'
RED = '\x1b[31m'
GREEN = '\x1b[32m'
CYAN = '\x1b[36m'
BLUE = '\x1b[34m'
YELLOW = '\x1b[93m'
BR_BLACK_BG = '\x1b[1m'

# Test performance of memoize vs non-memoize
def memoize(memory, index, f, *args):
    if index not in memory:
        memory[index] = f(*args)

    return memory[index]
    #return f(*args)

# Memoize
def knapsack(item, weights, values, W, pack, memory):
    if item == -1 or W == 0:
        return 0, pack

    # Can't include this item, too heavy
    if weights[item] > W:
        '''
        if (item-1,W) in memory:
            return memory[(item-1,W)]
        else:
            memory[(item-1, W)] = knapsack(item-1, weights, values, W, pack, memory)
            return memory[(item-1, W)]
        '''
        return memoize(memory, (item-1, W), knapsack, item-1, weights, values, W, pack, memory)

        #return memory[(item-1,W)] if (item-1,W) in memory else knapsack(item-1, weights, values, W, pack)

    # See which is better (starting from the last item):
    # - Adding the current item to the knapsack or
    # - Excluding the current item from the knapsack

    keep, exclude = None, None
    '''
    if (item-1, W-weights[item]) not in memory:
        memory[(item-1, W-weights[item])] = knapsack(item-1, weights, values, W-weights[item], [*pack, item], memory)

    keep = memory[(item-1, W-weights[item])]
    '''
    keep = memoize(memory, (item-1,W-weights[item]), knapsack, item-1, weights, values, W-weights[item], [*pack, item], memory)

    '''
    if (item-1, W) not in memory:
        memory[(item-1, W)] = knapsack(item-1, weights, values, W, pack, memory)

    exclude = memory[(item-1, W)]
    '''

    exclude = memoize(memory, (item-1,W), knapsack, item-1, weights, values, W, pack, memory)

    '''
    keep, exclude = knapsack(item-1, weights, values, W-weights[item], [*pack, item]),\
        knapsack(item-1, weights, values, W, pack)
    '''

    if values[item] + keep[0] > exclude[0]:
        return (values[item] + keep[0], keep[1])
    else:
        return exclude

    #return max(values[item] + knapsack(item-1, weights, values, W-weights[item], [*pack, item])[0],
    #        knapsack(item-1, weights, values, W, pack), key=lambda k: k[0])


