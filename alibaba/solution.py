from argparse import ArgumentParser
import collections
import csv
import logging
import os
from statistics import mean
from uriutils import URIFileType
# import decimal

logger = logging.getLogger(__name__)


TIME_FRAME = 98

E = 16


def main():
    parser = ArgumentParser(description='Alibaba scheduling problem')
    parser.add_argument('--app', type=str, metavar='<input>', required=True, help='Path to app resources file.')
    parser.add_argument('--machine', type=str, metavar='<input>', required=True, help='Path to machine resources file.')
    parser.add_argument('--instances', type=str, metavar='<input>', required=True, help='Path to instances resources file.')
    parser.add_argument('--interference', type=str, metavar='<input>', required=True, help='Path to app interference file.')
    parser.add_argument('-o', '--output', type=URIFileType('w'), metavar='<output>', required=True, help='Path to output file.')
    A = parser.parse_args()

    logging.basicConfig(format='%(asctime)-15s [%(name)s] %(levelname)s: %(message)s', level=logging.INFO)
    logger.setLevel(logging.INFO)

    root = os.getcwd()

    # load app.csv
    with open(os.path.join(root, A.app)) as csvfile:
        csv_rows = csv.reader(csvfile, delimiter=',')
        app_dict = collections.OrderedDict()
        for row in csv_rows:
            cpu_list = [float(cpu) for cpu in row[1].split('|')]
            mem_list = [float(mem) for mem in row[2].split('|')]
            # resource_dict = dict(mean_cpu=mean(cpu_list), mean_mem=mean(mem_list), disk=float(row[3]) / 10, p=float(row[4]), m=float(row[5]), pm=float(row[6]))
            resource_dict = dict(mean_cpu=mean(cpu_list), mean_mem=mean(mem_list), disk=float(row[3]) / 10)
            sorted_resource_dict = collections.OrderedDict(sorted(resource_dict.items(), key=lambda t: t[1], reverse=True))

            # sum_resources = mean(cpu_list) + mean(mem_list) + float(row[3]) / 10 + float(row[4]) + float(row[5]) + float(row[6])
            sum_resources = mean(cpu_list) + mean(mem_list) + float(row[3]) / 10
            app_dict.update({row[0]: [cpu_list, mem_list, sorted_resource_dict, [], [], sum_resources]})
        #end for
    #end with

    app_dict = collections.OrderedDict(sorted(app_dict.items(), key=lambda t: t[1][-1], reverse=True))

    # load machine.csv
    with open(os.path.join(root, A.machine)) as csvfile:
        csv_rows = csv.reader(csvfile, delimiter=',')
        machine_dict = collections.OrderedDict()
        for row in csv_rows:
            cpu_list = [float(row[1]) for i in range(TIME_FRAME)]
            mem_list = [float(row[2]) for i in range(TIME_FRAME)]
            # resource_capacity_dict = dict(mean_cpu=float(row[1]), mean_mem=float(row[2]), disk=float(row[3]) / 10, p=float(row[4]), m=float(row[5]), pm=float(row[6]))
            resource_capacity_dict = dict(mean_cpu=float(row[1]), mean_mem=float(row[2]), disk=float(row[3]) / 10)
            sorted_resource_capacity_dict = collections.OrderedDict(sorted(resource_capacity_dict.items(), key=lambda t: t[1], reverse=True))

            # sum_resources = float(row[1]) + float(row[2]) + float(row[3]) / 10 + float(row[4]) + float(row[5]) + float(row[6])
            sum_resources = float(row[1]) + float(row[2]) + float(row[3]) / 10
            machine_dict.update({row[0]: [cpu_list, mem_list, sorted_resource_capacity_dict, dict(), dict(), sum_resources]})
        #end for
    #end with

    # load interference csv
    with open(os.path.join(root, A.interference)) as csvfile:
        csv_rows = csv.reader(csvfile, delimiter=',')
        interf_dict = dict()
        for row in csv_rows:
            app_id_1 = row[0]
            app_id_2 = row[1]
            k = int(row[2])
            if app_id_1 == app_id_2:
                k += 1
            try:
                interf_dict[app_id_1].update({app_id_2: k})
            except KeyError:
                interf_dict[app_id_1] = {app_id_2: k}
        #end for
    #end with

    # load current plan
    with open(os.path.join(root, A.instances)) as csvfile:
        csv_rows = csv.reader(csvfile, delimiter=',')
        for row in csv_rows:
            instance_id = row[0]
            app_id = row[1]
            machine_id = row[2]

            if machine_id:
                app_dict[app_id][3].append(instance_id)  # update allocated-instance list (irregardless whether it's correctly allocated)
                if check_constraint(machine_dict[machine_id], app_id, app_dict[app_id], interf_dict):  # good allocated-instance
                    try:
                        machine_dict[machine_id][3][app_id].append(instance_id)
                    except KeyError:
                        machine_dict[machine_id][3].update({app_id: [instance_id]})

                    machine_dict[machine_id] = deduct_resources(machine_dict[machine_id], app_dict[app_id])
                else:  #  bad allocated-instance
                    try:
                        machine_dict[machine_id][4][app_id].append(instance_id)
                    except KeyError:
                        machine_dict[machine_id][4].update({app_id: [instance_id]})

                    machine_dict[machine_id] = deduct_resources(machine_dict[machine_id], app_dict[app_id])
            else:
                app_dict[app_id][4].append(instance_id)  # update unallocated-instance list
            #end if
        #end for
    #end with

    output_csv_rows = []
    # fix badly allocated-instances
    count = 0
    while True:
        not_fixed_flag = False
        machine_dict_list = list(machine_dict.items())
        for machine_id, machine_details in machine_dict_list:
            if len(machine_details[4]) > 0:  # have bad instances
                for app_id, instance_ids in machine_details[4].items():
                    for instance_id in instance_ids:
                        # temporarily add the instance id into unallocated instance list
                        app_dict[app_id][4].append(instance_id)  # update unallocated-instance list
                        app_dict[app_id][3].remove(instance_id)  # update allocated-instance list
                        if count < 3:
                            machine_dict, app_dict[app_id], output_csv_rows, success = fix_schedule_instance(
                                machine_dict, machine_id, app_id, app_dict[app_id], interf_dict, output_csv_rows)  # immediately reschedule
                        else:
                            machine_dict, app_dict[app_id], output_csv_rows, success = fix_schedule_instance_relax(
                                machine_dict, machine_id, app_id, app_dict[app_id], interf_dict, output_csv_rows)  # immediately reschedule
                        if not success:
                            not_fixed_flag = True
                            break
                    #end for
                #end for
            #end if
        #end for

        if not not_fixed_flag:
            break
        count += 1

        print('-')
    #end while

    print('-----------')

    machine_dict = collections.OrderedDict(sorted(machine_dict.items(), key=lambda t: t[1][-1]))

    # schedule unscheduled
    # for each machine, find the top 3 most under-utilised resources in order,
    # and then from the unallocated instances, find the instance that has top 3 needs that's exactly the same as
    # the top 3 most under-utilised resources in the same order.
    # For each machine, if at least one of the resources is fully used, the algo will move on to the next machine.
    count = 0
    while True:
        not_scheduled_flag = False
        full_machine_list = list()
        for machine_id, machine_details in machine_dict.items():
            if (list(machine_details[2].items())[-1][-1] - 0) < E:  # at least one of the resources is fully used
                full_machine_list.append(machine_id)
                continue
            #end if

            good_app_list = list()
            underutilised_capacities = [item[0] for item in list(machine_details[2].items())[0:3]]  # examine the first 3 most underutilised capacities
            next_machine = False
            for app_id, app_details in app_dict.items():
                most_needed_resources = [item[0] for item in list(app_details[2].items())[0:3]]
                if count < 3:
                    if (most_needed_resources == underutilised_capacities) & (len(app_details[4]) > 0):
                        for instance_id in app_details[4]:
                            if check_constraint(machine_details, app_id, app_details, interf_dict):
                                app_details[3].append(instance_id)  # update allocated-instance list
                                app_details[4].remove(instance_id)
                                try: # update the machine's goodly allocated instance list
                                    machine_details[3][app_id].append(instance_id)
                                except KeyError:
                                    machine_details[3].update({app_id: [instance_id]})
                                machine_details = deduct_resources(machine_details, app_details)
                                output_csv_rows.append((instance_id, machine_id))
                                underutilised_capacities = [item[0] for item in list(machine_details[2].items())[0:3]]  # update under-utilised capacities
                                if (list(machine_details[2].items())[-1][-1] - 0) < E:  # at least one of the resources is fully used
                                    full_machine_list.append(machine_id)
                                    next_machine = True
                                    break
                            else:
                                not_scheduled_flag = True
                                break
                            #end if
                        #end for
                    #end if
                else:
                    if (check_element(underutilised_capacities, most_needed_resources)) & (len(app_details[4]) > 0):
                        for instance_id in app_details[4]:
                            if check_constraint(machine_details, app_id, app_details, interf_dict):
                                app_details[3].append(instance_id)  # update allocated-instance list
                                app_details[4].remove(instance_id)
                                try: # update the machine's goodly allocated instance list
                                    machine_details[3][app_id].append(instance_id)
                                except KeyError:
                                    machine_details[3].update({app_id: [instance_id]})
                                machine_details = deduct_resources(machine_details, app_details)
                                output_csv_rows.append((instance_id, machine_id))
                                underutilised_capacities = [item[0] for item in list(machine_details[2].items())[0:3]]  # update under-utilised capacities
                                if (list(machine_details[2].items())[-1][-1] - 0) < E:  # at least one of the resources is fully used
                                    full_machine_list.append(machine_id)
                                    next_machine = True
                                    break
                            else:
                                not_scheduled_flag = True
                                break
                            #end if
                        #end for
                    #end if
                #end if

                app_dict[app_id] = app_details
                if len(app_details[4]) == 0:
                    good_app_list.append(app_id)

                if next_machine:
                    break
                #end if
            #end for

            for app_id in good_app_list:
                del app_dict[app_id]
            #end for

            machine_dict[machine_id] = machine_details
        #end for

        for machine_id in full_machine_list:
            del machine_dict[machine_id]
     
        machine_dict = collections.OrderedDict(sorted(machine_dict.items(), key=lambda t: t[1][-1]))

        if not not_scheduled_flag:
            break
        count += 1
        print('--')
    #end while

    count = len(output_csv_rows)
    writer = csv.writer(A.output, delimiter=',')
    writer.writerows(output_csv_rows)

    logger.info('Done! {} lines written to <{}>.'.format(count, A.output.name))
#end def


def fix_schedule_instance(machine_dict, cur_machine_id, app_id, app_details, interf_dict, output_csv_rows):
    most_needed_resources = [item[0] for item in list(app_details[2].items())[0:3]]
    success = False
    for machine_id, machine_details in machine_dict.items():
        underutilised_capacities = [item[0] for item in list(machine_details[2].items())[0:3]]  # examine the first 3 most underutilised capacities
        if (most_needed_resources == underutilised_capacities) & (len(app_details[4]) > 0) & (len(machine_details[4]) == 0):
            if check_constraint(machine_details, app_id, app_details, interf_dict):
                success = True
                instance_id = app_details[4][-1]
                app_details[3].append(instance_id)  # update allocated-instance list
                app_details[4].pop(-1)
                try:
                    machine_details[3][app_id].append(instance_id)
                except KeyError:
                    machine_details[3].update({app_id: [instance_id]})
                machine_details = deduct_resources(machine_details, app_details)
                machine_dict[machine_id] = machine_details
                output_csv_rows.append((instance_id, machine_id))
                break
            #end if
        #end if
    #end for

    # remove bad instance from current machine
    if success:
        machine_dict[cur_machine_id][4][app_id].remove(instance_id)
        # if len(machine_dict[cur_machine_id][4][app_id]) == 0:
        #     del machine_dict[cur_machine_id][4][app_id]
        machine_dict[cur_machine_id] = free_resources(machine_dict[cur_machine_id], app_details)
    else:
        app_details[3].append(app_details[4][-1])
        app_details[4].pop(-1)  # update allocated-instance list

    return machine_dict, app_details, output_csv_rows, success
#end def


def fix_schedule_instance_relax(machine_dict, cur_machine_id, app_id, app_details, interf_dict, output_csv_rows):
    most_needed_resources = [item[0] for item in list(app_details[2].items())[0:3]]
    success = False
    for machine_id, machine_details in machine_dict.items():
        underutilised_capacities = [item[0] for item in list(machine_details[2].items())[0:3]]  # examine the first 3 most underutilised capacities
        if (check_element(underutilised_capacities, most_needed_resources)) & (len(app_details[4]) > 0) & (len(machine_details[4]) == 0):
            if check_constraint(machine_details, app_id, app_details, interf_dict):
                success = True
                instance_id = app_details[4][-1]
                app_details[3].append(instance_id)  # update allocated-instance list
                app_details[4].pop(-1)
                try:
                    machine_details[3][app_id].append(instance_id)
                except KeyError:
                    machine_details[3].update({app_id: [instance_id]})
                machine_details = deduct_resources(machine_details, app_details)
                machine_dict[machine_id] = machine_details
                output_csv_rows.append((instance_id, machine_id))
                break
            #end if
        #end if
    #end for

    # remove bad instance from current machine
    if success:
        machine_dict[cur_machine_id][4][app_id].remove(instance_id)
        # if len(machine_dict[cur_machine_id][4][app_id]) == 0:
        #     del machine_dict[cur_machine_id][4][app_id]
        machine_dict[cur_machine_id] = free_resources(machine_dict[cur_machine_id], app_details)
    else:
        app_details[3].append(app_details[4][-1])
        app_details[4].pop(-1)  # update allocated-instance list

    return machine_dict, app_details, output_csv_rows, success
#end def


def check_constraint(machine_details, app_id, app_details, interf_dict):
    for i in range(TIME_FRAME):
        if (machine_details[0][i] - app_details[0][i]) < E:  # check cpu list
            return False
        if (machine_details[1][i] - app_details[1][i]) < E:  # check mem list
            return False
    if (machine_details[2]['disk'] - app_details[2]['disk']) < E:
        return False
    # if app_details[2]['p'] > machine_details[2]['p']:
    #     return False
    # if app_details[2]['m'] > machine_details[2]['m']:
    #     return False
    # if app_details[2]['pm'] > machine_details[2]['pm']:
    #     return False

    cur_apps = machine_details[3].keys()
    for cur_app_id in cur_apps:
        # if any instance of cur_app_id exist, then max_k_1 of app_id can exist
        try:
            max_k_1 = interf_dict[cur_app_id][app_id]  # if key_error raised, means no interference
            k = len(machine_details[3].get(app_id, ''))
            if k >= max_k_1:  # if this condition is true, it means already has k number of app_id
                return False
        except KeyError:  # no interference
            pass

        # if any instance of app_id exist, then max_k_2 of cur_app_id can exist
        try:
            max_k_2 = interf_dict[app_id][cur_app_id]  # if key_error raised, means no interference
            # if max_k_2 == 0:
            #     return False
            cur_k = len(machine_details[3].get(cur_app_id, ''))
            num_of_app_id = len(machine_details[3].get(app_id, ''))  # check number of app_id in machine
            if (cur_k >= max_k_2) & (num_of_app_id >= 0):  # if this condition is true, it means we cannot add app_id to this machine, otherwise <app_id, cur_app_id, k> will not hold
                return False
        except KeyError:  # no interference
            pass
    #end for

    return True
#end def


def free_resources(machine_details, app_details):
    for i in range(TIME_FRAME):
        machine_details[0][i] += app_details[0][i]
        machine_details[1][i] += app_details[1][i]
    machine_details[2]['mean_cpu'] = mean(machine_details[0])
    machine_details[2]['mean_mem'] = mean(machine_details[1])
    machine_details[2]['disk'] += app_details[2]['disk']
    # machine_details[2]['p'] -= app_details[2]['p']
    # machine_details[2]['m'] -= app_details[2]['m']
    # machine_details[2]['pm'] -= app_details[2]['pm']
    # machine_details[4] -= (mean(machine_details[0]) + mean(machine_details[1]) + app_details[2]['disk'] + app_details[2]['p'] + app_details[2]['m'] + app_details[2]['pm'])
    machine_details[5] = machine_details[2]['mean_cpu'] + machine_details[2]['mean_mem'] + machine_details[2]['disk']
    machine_details[2] = collections.OrderedDict(sorted(machine_details[2].items(), key=lambda t: t[1], reverse=True))
    return machine_details
#end def


def deduct_resources(machine_details, app_details):
    for i in range(TIME_FRAME):
        machine_details[0][i] -= app_details[0][i]
        machine_details[1][i] -= app_details[1][i]
    machine_details[2]['mean_cpu'] = mean(machine_details[0])
    machine_details[2]['mean_mem'] = mean(machine_details[1])
    machine_details[2]['disk'] -= app_details[2]['disk']
    # machine_details[2]['p'] -= app_details[2]['p']
    # machine_details[2]['m'] -= app_details[2]['m']
    # machine_details[2]['pm'] -= app_details[2]['pm']
    # machine_details[4] -= (mean(machine_details[0]) + mean(machine_details[1]) + app_details[2]['disk'] + app_details[2]['p'] + app_details[2]['m'] + app_details[2]['pm'])
    machine_details[5] = machine_details[2]['mean_cpu'] + machine_details[2]['mean_mem'] + machine_details[2]['disk']
    machine_details[2] = collections.OrderedDict(sorted(machine_details[2].items(), key=lambda t: t[1], reverse=True))
    return machine_details
#end def


def check_element(underutilised_capacities, most_needed_resources):
    for item in underutilised_capacities:
        if item not in most_needed_resources:
            return False
    return True
#end def


if __name__ == '__main__': main()
