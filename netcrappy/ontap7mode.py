import sys
import time
import re
import copy

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
    """@todo: Docstring for check_zapi_error.

    :output: @todo
    :errormsg: @todo
    :returns: @todo

    """
    if output and output.results_errno() != 0:
        reason = output.results_reason()
        raise NetCrAPIOut(errormsg % reason)


class Filer:
    '''
    Class for interacting with Filers or Clusters
    '''
    def __init__(self, filer_name, user, password, transport_type='HTTPS'):
        '''
        Creates the connection to the filer. Transport_type defaults to 'HTTPS'.
        Todo:
            Allow different connection styles?
        '''
        conn = NaServer(filer_name, 1, 15)
        out = conn.set_transport_type(transport_type)
        check_zapi_error(out, "connection to filer failed: %s")
        out = conn.set_style("LOGIN")
        check_zapi_error(out, "connection to filer failed: %s")
        out = conn.set_admin_user(user, password)
        check_zapi_error(out, "connection to filer failed: %s")
        self.conn = conn
        
    def invoke(self, *args):
        """@todo: Docstring for invoke.

        :args: Arguments passed to NaServer.invoke() 
        :returns: API output object

        """
        out = self.conn.invoke(*args)
        check_zapi_error(out)
        return out

    def invoke_elem(self, naelem):
        """@todo: Docstring for invoke_elem.

        :naelem: NaElement object
        :returns: output object

        """
        out = self.conn.invoke_elem(naelem)
        check_zapi_error(out)
        return out

    def invoke_cli(self, command):
        """Undocumented API that runs supplied arguments as a command. API
        documentation and python code found here:
            https://communities.netapp.com/message/74370

        :command: @todo
        :returns: Object containing cli-output (string) and 
                  cli-result-value (integer)

        """
        args = NaElement('args')
        for arg in command.split():
            args.child_add(NaElement('arg', arg))
        cli = NaElement('system-cli')
        cli.child_add(args)
        out = self.invoke_elem(cli)
        check_zapi_error(out)
        return out

    def get_filer_api_list(self):
        '''
         @todo: return list of ONTAP APIs
        '''
        #list_in = NaElement("system-api-list")
        pass

    def get_perf_objects(self):
        '''
        Lists all performance objects along with privilege levels.
        '''
        list_in = NaElement("perf-object-list-info")
        out = self.invoke_elem(list_in)
        check_zapi_error(out)
        obj_info = out.child_get("objects")
        result = obj_info.children_get()
        obj_list = []
        for obj in result:
            obj_name = obj.child_get_string("name")
            priv = obj.child_get_string("privilege-level")
            obj_list.append([obj_name, priv])
        return obj_list

    def get_instance_list(self, perf_obj):
        '''
        Lists instances associated with the supplied performance object.
        Some objects actually have instances, like 'volume' and 'aggregate', while
        others only have themselves (so sad...) like 'system' or 'cifs'
        '''
        list_in = NaElement("perf-object-instance-list-info")
        list_in.child_add_string("objectname", perf_obj)

        out = self.invoke_elem(list_in)
        check_zapi_error(out)
        inst_info = out.child_get("instances")
        result = inst_info.children_get()

        instance_names = []
        for inst in result:
            inst_name = inst.child_get_string("name")
            instance_names.append(inst_name)
        return instance_names

    def get_counter_list(self, perf_obj):
        '''
        Returns the counters associated with a performance object.
        This function also outputs the 'base counter', 'privilege level' and 
        'unit' of the counter.
        '''
        list_in = NaElement("perf-object-counter-list-info")
        list_in.child_add_string("objectname", perf_obj)
        out = self.invoke_elem(list_in)
        check_zapi_error(out)
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
            counter_list.append([counter_name,
                                 base_counter,
                                 counter_type,
                                 privilege_level,
                                 unit])
        return counter_list

    def get_volumes(self):
        """@todo: Docstring for get_volumes.
        :returns: @todo

        """
        pass

    def get_aggrs(self):
        """@todo: Docstring for get_aggrs.
        :returns: @todo

        """
        out = self.invoke('aggr-list-info')
        check_zapi_error(out)
        aggr_info_items = {"name": "string",
                           "state": "string",
                           "size-total": "integer",
                           "size-used": "integer",
                           "size-available": "integer",
                           "volume-count": "integer",
                           "has-local-root": "string"
                          }
        aggr_list = out.child_get('aggregates').children_get()
        aggr_info = {}
        for aggr_obj in aggr_list:
            aggr_name = aggr_obj.child_get_string('name')
            aggr_info[aggr_name] = {}
            #print aggr_name
            for item_name, item_type in aggr_info_items.iteritems():
                #print("name: %s, type: %s" % (item_name, item_type))
                if item_type == 'string':
                    aggr_info[aggr_name][item_name] = aggr_obj.child_get_string(item_name)
                elif item_type == 'integer':
                    aggr_info[aggr_name][item_name] = aggr_obj.child_get_int(item_name)
        return aggr_info            

    def create_vol(self, name, aggr, size):
        """@todo: Docstring for create_vol.

        :name: Name of volume
        :aggr: Containing aggregate
        :size: Size in the form of <number>[kmgt]
        :returns: 'volume' object

        """
        vol = Volume(self, name)
        vol.create(aggr, size)
        return vol

    def system_info(self):
        """@todo: Docstring for system_info.
        :returns: @todo
        This method does not work in cmode

        """
        out = self.invoke('system-get-info')
        check_zapi_error(out)
        system_info_objs = {"backplane-part-number": "string", 
                            "backplane-revision": "string",
                            "backplane-serial-number": "string",
                            "board-speed": "integer",
                            "board-type": "string",
                            "controller-address": "string",
                            "cpu-ciob-revision-id": "string",
                            "cpu-firmware-release": "string",
                            "cpu-microcode-version": "string",
                            "cpu-part-number": "string",
                            "cpu-processor-id": "string",
                            "cpu-processor-type": "string",
                            "cpu-revision": "string",
                            "cpu-serial-number": "string",
                            "memory-size": "integer",
                            "number-of-processors": "integer",
                            "partner-system-id": "string",
                            "partner-system-name": "string",
                            "partner-system-serial-number": "string",
                            "prod-type": "string",
                            "supports-raid-array": "string",
                            "system-id": "string",
                            "system-machine-type": "string",
                            "system-model": "string",
                            "system-name": "string",
                            "system-revision": "string",
                            "system-serial-number": "string",
                            "vendor-id": "string"
                           }
        sysinfo_filers = out.children_get()
        sysinfo = {}
        for filer_obj in sysinfo_filers:
            filer_name = filer_obj.child_get_string('system-name')
            sysinfo[filer_name] = {}
            for item_name, item_type in system_info_objs.iteritems():
                if item_type == 'string':
                    sysinfo[filer_name][item_name] = filer_obj.child_get_string(item_name)
                elif item_type == 'integer':
                    sysinfo[filer_name][item_name] = filer_obj.child_get_int(item_name)
        return sysinfo


class Volume:
    def __init__(self, filer_inst, name):
        self.invoke = filer_inst.invoke
        self.invoke_elem = filer_inst.invoke_elem
        self.invoke_cli = filer_inst.invoke_cli
        self.name = name

    def create(self, aggr, size):
        """@todo: Docstring for create.

        :aggr: @todo
        :size: @todo
        :returns: @todo

        """
        list_in = NaElement('volume-create')
        #name
        list_in.child_add_string("volume", self.name)
        #aggr
        list_in.child_add_string("containing-aggr-name", aggr)
        #size
        size_pattern = re.compile('^[1-9][0-9]*[kmgt]')
        if size_pattern.match(size):
            list_in.child_add_string("size", size)
        else:
            raise NetCrAPIOut("Size not valid. Please use <number>k|m|g|t.")
            pass
        #space reservation (none)
        list_in.child_add_string("space-reserve", "none")
        #language (en_US)
        list_in.child_add_string("language-code", "en_US")
        out = self.invoke_elem(list_in)
        check_zapi_error(out)

    def get_info(self):
        """@todo: Docstring for get_info.
        :returns: @todo

        """
        volume_info = {'autosize': 'autosize-info',
                       'block-type': 'string',
                       'clone-children': 'clone-child-info',
                       'clone-parent': 'clone-parent-info',
                       'containing-aggregate': 'string',
                       'files-total': 'integer',
                       'files-used': 'integer',
                       'owning-vfiler': 'string',
                       'percentage-used': 'integer',
                       'sis': 'sis-info',
                       'size-available': 'integer',
                       'size-total': 'integer',
                       'size-used': 'integer',
                       'space-reserve': 'string',
                       'state': 'string',
                      }
        out = self.invoke('volume-list-info',
                          'volume', self.name,
                          'verbose', 'true'
                         )
        volumes = out.child_get('volumes').children_get()
        vol_info_dict = {}
        if len(volumes) == 1:
            vol = volumes[0]
            for key, value in volume_info.iteritems():
                if value == 'string':
                    vol_info_dict[key] = vol.child_get_string(key)
                elif value == 'integer':
                    vol_info_dict[key] = vol.child_get_int(key)
                elif value == 'autosize-info':
                    vol_info_dict[key] = {}
                    autosize = vol.child_get('autosize').child_get('autosize-info')
                    vol_info_dict[key]['increment-size'] = autosize.child_get_int('increment-size')
                    vol_info_dict[key]['maximum-size'] = autosize.child_get_int('maximum-size')
                    vol_info_dict[key]['is-enabled'] = (autosize.child_get_string('is-enabled') == "true")
                elif value == 'clone-child-info':
                    child_clones = vol.child_get('clone-children')
                    if child_clones:
                        clones = child_clones.children_get()
                        vol_info_dict[key] = [child.child_get_string('clone-child-name') for child in clones]
                    else:
                        vol_info_dict[key] = None
                elif value == 'clone-parent-info':
                    clone_parent = vol.child_get('clone-parent')
                    if clone_parent:
                        clones = clone_parent.children_get()
                        vol_info_dict[key] = [{'parent-volume-name': parent.child_get_string('parent-volume-name'), 'parent-snapshot-name': parent.child_get_string('parent-snapshot-name')} for parent in clones]
                    else:
                        vol_info_dict[key] = None
            return vol_info_dict

    def online(self):
        """@todo: Docstring for online.
        :returns: @todo

        """
        out = self.invoke('volume-online',
                         'name', self.name
                         )
        check_zapi_error(out)

    def offline(self, cifs_delay=0):
        """@todo: Docstring for offline.
        :cifs_delay: number of minutes to delay offline for cifs users
                     defaults to 0 (immediate termination)
        :returns: @todo

        """
        out = self.invoke('volume-offline',
                         'name', self.name,
                         'cifs-delay', cifs_delay
                         )
        check_zapi_error(out)

    def destroy(self):
        """@todo: Docstring for destroy.
        :returns: @todo

        """
        out = self.invoke('volume-destroy',
                         'name', self.name)
        check_zapi_error(out)

    def get_option(self, option=None):
        """@todo: Docstring for get_option.

        :option: @todo
        :returns: @todo

        """
        out = self.invoke("volume-options-list-info",
                          "volume", self.name
                         )
        check_zapi_error(out)
        option_dict = {}
        for opt in out.child_get('options').children_get():
            option_name = opt.child_get_string('name')
            option_value = opt.child_get_string('value')
            option_dict[option_name] = option_value
        if option:
            return option_dict[option]
        else:
            return option_dict

    def set_option(self, option, value):
        """@todo: Docstring for set_option.

        :option: @todo
        :value: @todo
        :returns: @todo

        """
        out = self.invoke('volume-set-option',
                          'volume', self.name,
                          'option-name', option,
                          'option-value', value
                          )
        check_zapi_error(out)

    def get_snapshots(self):
        """@todo: Docstring for get_snapshots.
        :returns: @todo

        """
        out = self.invoke("snapshot-list-info",
                                   "target-name", self.name,
                                   "target-type", "volume",
                                   "terse", "True"
                                  )
        snapshots_child = out.child_get("snapshots")
        #cluster-mode returns a construct without a <snapshots> element if
        #there are no snapshots in the volume
        if snapshots_child == None:
            return []
        snapshots = snapshots_child.children_get()
        snapshot_list = []
        for snap in snapshots:
            accesstime = float(snap.child_get_int("access-time"))
            busy = (snap.child_get_string("busy") == "true")
            dependency = snap.child_get_string("dependency")
            if dependency == "":
                dependency = None
            snap_name = snap.child_get_string("name")
            date = time.localtime(accesstime)
            snapshot_list.append([snap_name,
                                  time.strftime("%Y-%m-%d %H:%M:%S", date),
                                  busy,
                                  dependency
                                 ])
        return snapshot_list

    def create_snapshot(self, snap_name):
        """@todo: Docstring for create_snapshot.

        :snap_name: @todo
        :returns: @todo

        """
        out = self.invoke("snapshot-create",
                          "volume", self.name,
                          "snapshot", snap_name
                         )
        check_zapi_error(out)

    def delete_snapshot(self, snap_name):
        """@todo: Docstring for delete_snapshot.

        :snap_name: @todo
        :returns: @todo

        """
        out = self.invoke("snapshot-delete",
                          "volume", self.name,
                          "snapshot", snap_name
                         )
        check_zapi_error(out)

    def get_snapshot_schedule(self):
        """@todo: Docstring for get_snapshot_schedule.
        :returns: @todo

        """
        out = self.invoke("snapshot-get-schedule",
                          "volume", self.name
                         )
        check_zapi_error(out)
        days = out.child_get_int('days')
        hours = out.child_get_int('hours')
        minutes = out.child_get_int('minutes')
        weeks = out.child_get_int('weeks')
        which_hours = out.child_get_string('which-hours')
        which_minutes = out.child_get_string('which-minutes')
        snap_sched = {'days': days,
                      'hours': hours,
                      'minutes': minutes,
                      'weeks': weeks,
                      'which-hours': which_hours,
                      'which-minutes': which_minutes
                     }
        return snap_sched

    def set_snapshot_schedule(self, snap_sched):
        """@todo: Docstring for set_snapshot_schedule.

        :snap_sched: Dict that my contain one or more of the following keys:
            days,
            hours,
            minutes,
            weeks,
            which-hours,
            which-minutes
        :returns: @todo

        """
        list_in = NaElement('snapshot-set-schedule')
        list_in.child_add_string('volume', self.name)
        for snap_interval, snap_count in snap_sched.iteritems():
            list_in.child_add_string(snap_interval, snap_count)
        out = self.invoke_elem(list_in)
        check_zapi_error(out)

    def get_snapshot_reserve(self):
        """@todo: Docstring for get_snapshot_reserve.
        :returns: @todo

        """
        out = self.invoke("snapshot-get-reserve",
                          "volume", self.name
                         )
        check_zapi_error(out)
        reserve = out.child_get_int('percent-reserved')
        return reserve

    def set_snapshot_reserve(self, reserve):
        """@todo: Docstring for set_snapshot_reserve.

        :reserve: @todo
        :returns: @todo

        """
        out = self.invoke('snapshot-set-reserve',
                          'volume', self.name,
                          'percentage', reserve
                         )
        check_zapi_error(out)

    def set_snapshot_autodelete(self, option_name, option_value):
        """@todo: Docstring for set_snapshot_autodelete.

        :option: @todo
        :value: @todo
        :returns: @todo

        """
        out = self.invoke('snapshot-autodelete-set-option',
                          'volume', self.name,
                          'option-name', option_name,
                          'option-value', option_value
                         )
        check_zapi_error(out)

    def sis_status(self):
        """@todo: Docstring for sis_status.
        :returns: @todo

        """
        out = self.invoke('sis-status'
                          'path', "/vol/%s" % self.name
                         )
        check_zapi_error(out)
        sis_object = out.child_get('sis-object')
        status = sis_object.child_get_string('status')
        return status

    def sis_enable(self):
        """@todo: Docstring for sis_enable.
        :returns: @todo

        """
        out = self.invoke("sis-enable",
                          "path", "/vol/%s" % self.name
                         )
        check_zapi_error(out)

    def sis_disable(self):
        """@todo: Docstring for sis_disable.
        :returns: @todo

        """
        out = self.invoke("sis-disable",
                          "path", "/vol/%s" % self.name
                         )
        check_zapi_error(out)

    def get_qtree_security(self):
        """@todo: Docstring for get_qtree_security.
        :returns: @todo

        """
        out = self.invoke_cli("qtree security /vol/%s" % self.name)
        check_zapi_error(out)
        output_message = out.child_get_string("cli-output")
        #output_code = out.child_get_int("cli-result-value")
        qtree_pattern = re.compile("^.*has (mixed|ntfs|unix) security style and oplocks are (enabled|disabled).\n$")
        match = qtree_pattern.match(output_message)
        if match:
            security_style = match.groups()[0]
            return security_style
        else:
            raise NetCrAPIOut(output_message.strip())

    def set_qtree_security(self, security):
        """@todo: Docstring for set_qtree_security.

        :security: qtree security value, can be unix, ntfs, or mixed
        :returns: @todo

        """
        if security not in ['unix', 'ntfs', 'mixed']:
            raise NetCrAPIOut(
                'The security style can only be unix, ntfs, or mixed.')
        else:
            out = self.invoke_cli('qtree security /vol/%s %s' %
                                  (self.name, security)
                                 )
            output_message = out.child_get_string('cli-output')
            if not output_message == '':
                raise NetCrAPIOut(output_message.strip())
    
    def get_autosize(self):
        """@todo: Docstring for get_autosize.
        :returns: @todo

        """
        autosize_params = self.get_info()['autosize']
        return autosize_params

    def set_autosize(self, enabled, maximum, increment):
        """@todo: Docstring for set_autosize.

        :enabled: @todo
        :maximum: @todo
        :increment: @todo
        :returns: @todo

        """
        size_regex = re.compile('[1-9][0-9]*[kmgt]')
        for item in [maximum, increment]:
            if not size_regex.match(item):
                raise NetCrAPIOut("Size not valid. Please use <number>k|m|g|t.")
        out = self.invoke('volume-autosize-set',
                          'volume', self.name,
                          'is-enabled', enabled,
                          'maximum-size', maximum,
                          'increment-size', increment
                         )
        check_zapi_error(out)
