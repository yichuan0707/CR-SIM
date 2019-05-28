import argparse
import sys

from simulator.Configuration import Configuration
from simulator.utils.utils import splitMethod


class C(object):
    pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="CR-SIM", description="Process system parameters")
    parser.add_argument("data_placement", help="data placement", type=str)
    parser.add_argument("-r", help="data redundancy scheme: drs_n_k", type=str, dest="drs")
    parser.add_argument("-R", "--heterogeneous_redundancy", help="drs1 drs2 Transfer_Policy others", default=False, dest="hdrs")
    parser.add_argument("-m", "--heterogeneous_medium", help="Format: medium1,medium2,medium3 (drs for medium1):(drs for medium2):(drs for medium3) Transfer_Policy; Sample: SSD,HDD (RS_n_k):(LRC_n_k_l) hotness", default=False, dest="hm")
    parser.add_argument("-s", "--system_scaling", help="Format: scale_start style (disk_capcity) inc_capcity inc_slices slice_rate load_balancing (disk_generators) (machine_generators) (rack_generators)", action='append', nargs="*", dest="ss")
    parser.add_argument("-u", "--system_upgrade", help="Format: upgrade_start upgrade_rate(machines/hour) downtime", action='append', nargs="*", dest="su")
    parser.add_argument("-c", "--correlate_failures", help="Format: timestamp unavailable_machines downtime loss_prob", action='append', nargs="*", dest="cf")
    parser.add_argument("-b", "--block_failure", help="block failure probability", nargs="?", const=0.0001, default=False, dest="bf")
    parser.add_argument("-l", "--lazy_recovery", help="Format: max_degraded_slices availability_to_durability_threshold recovery_probability", default=False, dest="lazy")
    parser.add_argument("-f", "--rafi_recovery", help="Format: detect_interval1 detect_interval2", default=False, dest="rafi")
    parser.add_argument("-o", "--output", help="Output Conents, sample: DL UNA RB", nargs="*", dest="output")

    c = C()
    res = parser.parse_args(namespace=c)

    print c.hdrs
    print c.hm
    print c.ss
