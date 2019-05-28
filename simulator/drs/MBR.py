from simulator.drs.MSR import MSR


class MBR(MSR):

    # MBR stores more data on each node with sub-blocks.
    @property
    def DSC(self):
        return float(2 * self.d)/float(2 * self.d - self.k + 1)

    @property
    def ORC(self):
        return float(2 * self.d)/float(2 * self.d - self.k + 1)


if __name__ == "__main__":
    mbr = MBR(['9', '6', '6'])
    print "device stor:", mbr.DSC
    print "system stor:", mbr.SSC
    print "optimal repair:", mbr.ORC
    print "normal repair:", mbr.RC
