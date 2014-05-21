import sys
import time
import re

from NaServer import NaServer
from NaElement import NaElement

class NetCrAPIOut(Exception):
    '''
    Custom exception for the NetApp API.
    '''
    def __init__(self, errmsg):
        self.errmsg = errmsg

    def __str__(self):
        return repr(self.errmsg)

def check_zapi_error(output, errormsg='An error occured: %s'):
    if output and output.results_errno() != 0:
        reason = out.results_reason()
        raise NetCrAPIOut(errormsg % reason)


def filer_connection(filer, user, password, transport_type='HTTPS'):
    '''
    Creates the connection to the filer. Transport_type defaults to 'HTTPS'.
    Todo:
        Allow different connection styles?
    '''
    conn = NaServer(filer, 1, 3)

    out = conn.set_transport_type(transport_type)
    if out and out.results_errno() != 0:
        r = out.results_reason()
        raise NetCrAPIOut("connection to filer failed: %s" % r)
        #print("connection to filer failed: %s\n" % r)
        #sys.exit(2)

    out = conn.set_style("LOGIN")
    if out and out.results_errno() != 0:
        r = out.results_reason()
        raise NetCrAPIOut("connection to filer failed: %s" % r)
        #print("connection to filer failed: %s\n" % r)
        #sys.exit(2)

    out = conn.set_admin_user(user, password)
    if out and out.results_errno() != 0:
        r = out.results_reason()
        raise NetCrAPIOut("connection to filer failed: %s" % r)
        #print("connection to filer failed: %s\n" % r)
        #sys.exit(2)

    return conn


def get_filer_api_list(conn):
    '''
    returns list of ONTAP APIs
    '''
    list_in = NaElement("system-api-list")


def get_perf_objects(conn):
    '''
    Lists all performancei objects along with privilege levels.
    '''
    list_in = NaElement("perf-object-list-info")
    out = conn.invoke_elem(list_in)
    if out.results_status() == "failed":
        raise NetCrAPIOut(out.results_reason())
        #print(out.results_reason() + "\n")
        #sys.exit(2)
    obj_info = out.child_get("objects")
    result = obj_info.children_get()
    obj_list = []
    for obj in result:
        obj_name = obj.child_get_string("name")
        priv = obj.child_get_string("privilege-level")
        obj_list.append([obj_name, priv])
    return obj_list

def get_instance_list(conn, perf_obj):
    '''
    Lists instances associated with the supplied performance object.
    Some objects actually have instances, like 'volume' and 'aggregate', while
    others only have themselves (so sad...) like 'system' or 'cifs'
    '''
    list_in = NaElement("perf-object-instance-list-info")
    list_in.child_add_string("objectname", perf_obj)

    out = conn.invoke_elem(list_in)
    if out.results_status() == "failed":
        raise NetCrAPIOut(out.results_reason())
        #print(out.results_reason() + "\n")
        #sys.exit(2)

    inst_info = out.child_get("instances")
    result = inst_info.children_get()

    instance_names = []
    for inst in result:
        inst_name = inst.child_get_string("name")
        instance_names.append(inst_name)
    
    return instance_names

def get_counter_list(conn, perf_obj):
    '''
    Returns the counters associated with a performance object.
    This function also outputs the 'base counter', 'privilege level' and 
    'unit' of the counter.
    '''
    list_in = NaElement("perf-object-counter-list-info")
    list_in.child_add_string("objectname", perf_obj)

    out = conn.invoke_elem(list_in)
    if out.results_status() == "failed":
        raise NetCrAPIOut(out.results_reason())
        #print(out.results_reason() + "\n")
        #sys.exit(2)
    
    counter_info = out.child_get("counters")
    result = counter_info.children_get()

    counter_list = []
    for counter in result:
        counter_name = counter.child_get_string("name")
        
        if counter.child_get_string("base-counter"):
            base_counter = counter.child_get_string("base-counter")
        else:
            base_counter = "None"

        privilege_level = counter.child_get_string("privilege-level")

        if counter.child_get_string("unit"):
            unit = counter.child_get_string("unit")
        else:
            unit = "None"

        if counter.child_get_string("properties"):
            counter_type = counter.child_get_string("properties")
        else:
            counter_type = "None"
        counter_list.append([counter_name, base_counter, counter_type, privilege_level, unit])
    return counter_list

def get_perfdata_by_counter(conn, perf_obj, counter_list, max_records=10, domain='test'):
    '''
    Takes a list of counters for a given performances and returns the
    counters for all instances of the performance object.
    The ouput is a list of crazy-ass tuples of the following format:
        (path, (value, time))

        where:
            - path is a period (.) delimited string containing the
            domain, filername instance name, performance object and
            counter name
            - value is the counter value
            - time is the epoch time (number of seconds since the great
            computer Epoch (1/1/1970?) of the measurement
    This output is ready for pushing into graphite.
    '''

    path_base = "%s.storage.%s.%s." % (domain,
                                       conn.server_type.lower(),
                                       conn.server.lower())

    perf_in = NaElement("perf-object-get-instances-iter-start")
    perf_in.child_add_string(("objectname"), perf_obj)
    counters = NaElement("counters")

    for item in counter_list:
        counters.child_add_string("counter", item)

    perf_in.child_add(counters)

    now = int(time.time())
    out = conn.invoke_elem(perf_in)
    if out.results_status() == "failed":
        raise NetCrAPIOut(out.results_reason())

    iter_tag = out.child_get_string("tag")
    num_records = 1
    perfdata = []

    while num_records != 0:
        perf_in = NaElement("perf-object-get-instances-iter-next")
        perf_in.child_add_string("tag", iter_tag)
        perf_in.child_add_string("maximum", max_records)
        out = conn.invoke_elem(perf_in)

        if out.results_status() == "failed":
            raise NetCrAPIOut(out.results_reason())

        num_records = out.child_get_int("records")

        if num_records > 0:
            instances_list = out.child_get("instances")
            instances = instances_list.children_get()

            for inst in instances:
                inst_name_raw = inst.child_get_string("name")
                if perf_obj == 'lun':
                    remove_vol = re.sub('/vol/', '', inst_name_raw)
                    sub_slashes = re.sub('/', '.', remove_vol)
                    inst_name = sub_slashes
                else:
                    inst_name = inst_name_raw
                counters_list = inst.child_get("counters")
                counters = counters_list.children_get()

                for counter in counters:
                    counter_name = counter.child_get_string("name")
                    #counter_value = counter.child_get_string("value")
                    counter_val_int = counter.child_get_int("value")
                    counter_path = path_base + "%s.%s.%s" % (perf_obj,
                                                             inst_name,
                                                             counter_name)

                    perfdata.append((counter_path,
                                     (now, counter_val_int)))
                    #perfdata.append([counter_path,
                    #                 counter_val_int,
                    #                 counter_value,
                    #                 now])
    
    perf_in = NaElement("perf-object-get-instances-iter-end")
    perf_in.child_add_string("tag", iter_tag)
    out = conn.invoke_elem(perf_in)
    if out.results_status() == "failed":
        raise NetCrAPIOut(out.results_reason())

    return perfdata


#def get_snapshots(conn, volume_name):
#    output_snaps = conn.invoke("snapshot-list-info",
#                               "target-name", volume_name,
#                               "target-type", "volume",
#                               "terse", "True"
#                              )

