#What is Spark Broadcast?
# Spark Broadcast variables are secured, read-only variables that get distributed and cached to worker nodes.
# This is helpful to Spark because when the driver sends packets of information to worker nodes, it sends the data and tasks attached together
# which could be a little heavier on the network side.
# Broadcast variables seek to reduce network overhead and to reduce communications.
# Spark Broadcast variables are used only with Spark Context.


from pyspark import SparkContext

sc = SparkContext('local[*]', 'pyspark')

my_dict = {"item1": 1, "item2": 2, "item3": 3, "item4": 4}
my_list = ["item1", "item2", "item3", "item4"]

my_dict_bc = sc.broadcast(my_dict)

def my_func(letter):
    return my_dict_bc.value[letter]

my_list_rdd = sc.parallelize(my_list)

result = my_list_rdd.map(lambda x: my_func(x)).collect()

print(result)