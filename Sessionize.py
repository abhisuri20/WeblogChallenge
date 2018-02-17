
# coding: utf-8

# In[38]:


import pyspark
import numpy as np
from pyspark.sql.functions import udf, collect_list, struct, mean, stddev, sum, max, floor
from pyspark.sql import functions as F, SQLContext
from pyspark.sql.types import *
from datetime import datetime
import dateutil.parser
from pyspark.sql.functions import lag, col
from pyspark.sql.window import Window


# In[39]:


#Read the file, make a spark dataframe, keep useful columns and rename them
def filter_columns(p):
    values = p.split(" ")
    required_values = [values[0], values[2], values[12]]
    return required_values
log_txt=sc.textFile("IP Session Data.log")
temp_var = log_txt.map(lambda x: filter_columns(x))
log_df=temp_var.toDF()
log_df = log_df.selectExpr("_1 as Timestamp", "_2 as IP_address", "_3 as Link_Visited")


# In[40]:


#Remove socket info from ip_address
def remove_socket(ip_address):
    ip_address = ip_address.split(":")[0]
    return ip_address
ip_udf = udf(remove_socket, StringType())
log_df_ip = log_df.withColumn('IP', ip_udf(log_df.IP_address)).drop(log_df.IP_address)


# In[41]:


#Adding previous timestamp by IP
my_window = Window.partitionBy("IP").orderBy("Timestamp")
log_df_ip_prev_time = log_df_ip.withColumn("Prev_Timestamp", F.lag(log_df_ip.Timestamp).over(my_window))


# In[42]:


#Calculate Inacitvity duration as time(next_request)-time(prev_request) groupBy IP and orderBy Timestamp
def calculate_interval(time,prev_time): 
    import dateutil.parser
    if(prev_time==None):
        return 0.0
    time = dateutil.parser.parse(time)
    prev_time = dateutil.parser.parse(prev_time)
    delta = (time - prev_time).total_seconds()
    return delta
calculate_interval_udf = udf(calculate_interval, DoubleType())
log_ip_inactivity_time = log_df_ip_prev_time.withColumn('Inactivity_Duration', calculate_interval_udf(log_df_ip_prev_time.Timestamp, log_df_ip_prev_time.Prev_Timestamp))


# In[43]:


#Check whether the row is in a new session by comparing inacitivity duration with threshold
def new_session(inactivity_duration):
    inactivity_threshold = 15*60 #15 minutes
    if(inactivity_duration > inactivity_threshold):
        return 1
    else:
        return 0
new_session_udf = udf(new_session, IntegerType())
log_ip_new_session = log_ip_inactivity_time.withColumn('New_Session', new_session_udf(log_ip_inactivity_time.Inactivity_Duration))


# In[44]:


#Create Session Number
my_window = Window.partitionBy("IP").orderBy("Timestamp")
log_ip_session_number = log_ip_new_session.withColumn("Session_Number", F.sum(log_ip_new_session.New_Session).over(my_window))


# In[26]:


#Create Session ID Ip_Session_number
def session_id(ip, session_number):
    session_id = ip + "_" + str(session_number)
    return session_id
session_number_udf = udf(session_id, StringType())
log_ip_session_id = log_ip_session_number.withColumn('Session_Id', session_number_udf(log_ip_session_number.IP, log_ip_session_number.Session_Number))


# In[45]:


#Merge session details Q1.1
#Collect a list of all the links and timestamps per session_id
log_session = log_ip_session_id.groupBy(["Session_Id", "IP"]).agg(collect_list("Link_Visited").alias("Links"),collect_list("Timestamp").alias("Times"))


# In[46]:


#Create session_interval as difference in first and last timestamp in collected list of timestamps
def session_length(timestamps):
    import dateutil.parser
    timestamps.sort()
    first_hit = dateutil.parser.parse(timestamps[0])
    last_hit = dateutil.parser.parse(timestamps[len(timestamps)-1])
    delta = (last_hit - first_hit).total_seconds() #assumes that if there's only one visit the session interval is zero
    return delta
session_length_udf = udf(session_length, DoubleType())
log_session_details = log_session.withColumn("Session_Length", session_length_udf(log_session.Times))
log_session_details_describe = log_session_details.describe("Session_Length")


# In[47]:


#Determining average session time Q1.2
# Average = sum_all_session_time/total_number_sessions
mean_session_time = float(log_session_details_describe.filter("summary = 'mean'").select("Session_Length").collect()[0].asDict()['Session_Length'])
print "Average sesion time is %f seconds" %(mean_session_time)


# In[30]:


#Determine number of unique URL visits per session Q1.3 counting only unqiue links in a session
log_session_info = log_ip_session_id.groupBy("Session_Id").agg(collect_list("Link_Visited").alias("Links"), collect_list("Timestamp").alias("Times"))

def unique_link_visited(link_visited):
    links= list()
    return len(set(link_visited))
unique_visits_udf = udf(unique_link_visited, IntegerType())
log_session_visits = log_session_details.withColumn("Unique_Visits",unique_visits_udf(log_session_details.Links)).select("Session_ID","Unique_Visits")


# In[31]:


#find users with maximum session length we can also have a range of interval for maximum session length Q1.4
max_session_length = float(log_session_details_describe.filter("summary = 'max'").select("Session_Length").collect()[0].asDict()['Session_Length'])
engaged_users = log_session_details.select("IP").where(log_session_details["Session_Length"] == max_session_length)
print "Users with maximum session length"
engaged_users.show()


# In[32]:


#Expected Session length for a given IP Q2.2

#Groupby IP
log_mean_session_length = log_session_details.groupBy("IP").agg(mean('Session_Length'))
session_length_describe = log_mean_session_length.describe("avg(Session_Length)")

#Session length distribution for each IP

mean_session_length = float(session_length_describe.filter("summary = 'mean'").select("avg(Session_Length)").collect()[0].asDict()['avg(Session_Length)'])
sigma_session_length = float(session_length_describe.filter("summary = 'stddev'").select("avg(Session_Length)").collect()[0].asDict()['avg(Session_Length)'])
sample_size = float(session_length_describe.filter("summary = 'count'").select("avg(Session_Length)").collect()[0].asDict()['avg(Session_Length)'])

#Finding confidence interval for session length. SInce sample size > 30 the assumption about the population shouldn't matter
expected_session_lower_bound = mean_session_length - 1.96*sigma_session_length/np.sqrt(sample_size)
if(expected_session_lower_bound<0):#session length cannot be negative
    expected_session_lower_bound = 0
expected_session_upper_bound = mean_session_length + 1.96*sigma_session_length/np.sqrt(sample_size)

print "The session length for a given IP is expected to lie in the interval[%f, %f] seconds 95 percent of the time " %(expected_session_lower_bound, expected_session_upper_bound)


# In[33]:


#Expected load in next minute Q 2.1
#Adding previous timestamp 
my_window = Window.partitionBy().orderBy("Timestamp")
log_prev_time = log_df_ip.withColumn("Prev_Timestamp", lag(log_df_ip.Timestamp).over(my_window))
#Calculate Request_Interval
def request_interval(time,prev_time): 
    import dateutil.parser
    if(prev_time==None):
        return 0.0
    time = dateutil.parser.parse(time)
    prev_time = dateutil.parser.parse(prev_time)
    delta = (time - prev_time).total_seconds()
    return delta
request_interval_udf = udf(request_interval, DoubleType())
log_ip_request_interval = log_prev_time.withColumn('Request_Interval', request_interval_udf(log_prev_time.Timestamp, log_prev_time.Prev_Timestamp)).drop("Link_Visited")


# In[34]:


#Checking if the request is in new second 
# Bin all request in nth second together
#Example 
# Input
#     Req_1 2.5
#     Req_2 2.4
#     Req_3 3.2
#     Req_4 3.99
# Output
#     Req_1 2
#     Req_2 2
#     Req_3 3
#     Req_3 3
my_window = Window.partitionBy().orderBy("Timestamp")
log_ip_request_second = log_ip_request_interval.withColumn("Request_Second", sum(log_ip_request_interval.Request_Interval).over(my_window))
log_ip_request_each_second = log_ip_request_second.select("Request_Second", floor(log_ip_request_second['Request_Second']))
# Assumption - there's atleast one request in each second if not then we would also need to insert the missing second and with
# zero requests in that second that would require to join two dataframe and thats a costly operation
log_ip_request_in_each_second = log_ip_request_each_second.groupBy("Floor(Request_Second)").count()


# In[35]:


#Calculate the expected number of requests/second

## assume that there's a request in each second from initial T i.e there's not a single second interval starting from T when the number of requests were 0
ip_request_in_each_second_describe = log_ip_request_in_each_second.describe("count")
mean_load= float(ip_request_in_each_second_describe.filter("summary = 'mean'").select("count").collect()[0].asDict()['count'])
sigma_load= float(ip_request_in_each_second_describe.filter("summary = 'stddev'").select("count").collect()[0].asDict()['count'])
sample_size= float(ip_request_in_each_second_describe.filter("summary = 'count'").select("count").collect()[0].asDict()['count'])

#Using sampling distribution, since sample size >30 assumption about the population distribution doesn;t really matter
expected_load_lower_bound = mean_load - 1.96*sigma_load/np.sqrt(sample_size)
if(expected_load_lower_bound<0): #Number of request cannot be negative
    expected_load_lower_bound = 0
expected_load_upper_bound = mean_load + 1.96*sigma_load/np.sqrt(sample_size)
print "The number of requests/second is expected to lie in the interval[%f, %f] 95 percent of the time " %(expected_load_lower_bound, expected_load_upper_bound)


# In[36]:


#Predict unique URL visits per IP 2.3
log_visits_ip = log_session_details.groupBy("IP").agg(collect_list('Links').alias('Links'))
def count_unique(links):
    links_flat_list = list()
    for subitem in links:
        for item in subitem:
            links_flat_list.append(item)
    return len(set(links_flat_list))        
unique_visits_udf = udf(count_unique, IntegerType())
log_visits_ip_unique_links = log_visits_ip.withColumn("Count_Unique_Links", unique_visits_udf(log_visits_ip.Links)).select("IP","Count_Unique_Links")
visits_ip_unique_links_describe = log_visits_ip_unique_links.describe("Count_Unique_Links")


# In[37]:


#Expected number of unique URL visited
#Using sampling distribution, since sample size >30 assumption about the population distribution doesn't really matter

mean_number_url= float(visits_ip_unique_links_describe.filter("summary = 'mean'").select("Count_Unique_Links").collect()[0].asDict()['Count_Unique_Links'])
sigma_number_url= float(visits_ip_unique_links_describe.filter("summary = 'stddev'").select("Count_Unique_Links").collect()[0].asDict()['Count_Unique_Links'])
sample_size = float(visits_ip_unique_links_describe.filter("summary = 'count'").select("Count_Unique_Links").collect()[0].asDict()['Count_Unique_Links'])

expected_number_url_lower_bound = mean_number_url - 1.96*sigma_number_url/np.sqrt(sample_size)
if(expected_number_url_lower_bound<0):#number of unique URL cannot be negative
    expected_number_url_lower_bound = 0
expected_number_url_upper_bound = mean_number_url + 1.96*sigma_number_url/np.sqrt(sample_size)
print "The number of unique URL visited for a given IP is expected to lie in the interval[%f, %f] 95 percent of the time " %(expected_number_url_lower_bound, expected_number_url_upper_bound)

